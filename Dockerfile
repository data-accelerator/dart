# Build DART as a static binary and ship it on a minimal base.
#
#   docker build -t dart:dev .
#
# Two properties matter here and are easy to lose:
#
#   * CGO_ENABLED=0 gives a genuinely static binary, so the runtime stage needs no
#     libc. It also forces Go's pure-Go DNS resolver, which is what we want: the
#     cgo resolver would need /etc/nsswitch.conf and libnss on the runtime image.
#   * The runtime stage still needs CA certificates, because an origin is usually
#     an HTTPS registry or object store. `scratch` has none and every fetch would
#     fail x509 verification, so we use distroless/static, which ships them.

FROM golang:1.22-alpine AS build

WORKDIR /src

# Copy the module files first so dependency resolution is cached separately from
# source edits. DART has no external dependencies today, but that can change.
COPY go.mod ./
RUN go mod download

COPY . .

# -trimpath keeps build paths out of the binary; -s -w drop the symbol table and
# DWARF, which is a large fraction of the image.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/dart ./cmd/dart

# Run the unit tests inside the build so a broken image cannot be produced
# silently. Skip with --build-arg SKIP_TESTS=1 for a fast local iteration.
ARG SKIP_TESTS=0
RUN if [ "$SKIP_TESTS" != "1" ]; then go vet ./... && go test ./... ; fi

# ---------------------------------------------------------------------------
# dart-k8s variant: the same node plus the EndpointSlice discovery scheme from
# the providers/k8s module (client-go). Build with:
#
#   docker build --target dart-k8s -t dart-k8s:dev .
#
# The variant is NOT on the default build's dependency chain, so building the
# plain image never downloads client-go; the k8s module's go.mod makes the
# sibling checkout via its replace directive, which the COPY above already
# brought in. Deploy the variant with deploy/k8s/rbac.yaml.
FROM build AS build-k8s

# ARGs do not cross a FROM boundary; re-declare the ones this stage uses.
ARG VERSION=dev
ARG SKIP_TESTS=0

RUN cd providers/k8s && go mod download
RUN cd providers/k8s && CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/dart-k8s ./cmd/dart-k8s
RUN if [ "$SKIP_TESTS" != "1" ]; then cd providers/k8s && go vet ./... && go test ./... ; fi

FROM gcr.io/distroless/static-debian12:nonroot AS dart-k8s
COPY --from=build-k8s /out/dart-k8s /dart
EXPOSE 19145 19146 19147
USER nonroot:nonroot
ENTRYPOINT ["/dart"]

# The default target (the plain dart image) stays LAST: `docker build .` with
# no --target builds the final stage, and this must keep being it.
FROM gcr.io/distroless/static-debian12:nonroot

# nonroot is uid/gid 65532. The cache directory must be writable by it; the
# manifests set fsGroup accordingly.
COPY --from=build /out/dart /dart

# Client, peer and admin planes. Documented here so `docker inspect` shows them;
# EXPOSE does not publish anything by itself.
EXPOSE 19145 19146 19147

USER nonroot:nonroot
ENTRYPOINT ["/dart"]
