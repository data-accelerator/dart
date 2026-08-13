{{/*
Resource name prefix. Deliberately the chart name (not release-qualified): a
cluster runs exactly one DART DaemonSet, and the headless Service name is
what peers dial, so it must be predictable rather than release-specific.
*/}}
{{- define "dart.fullname" -}}
{{- default .Chart.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dart.labels" -}}
app: {{ include "dart.fullname" . }}
app.kubernetes.io/name: {{ include "dart.fullname" . }}
app.kubernetes.io/part-of: dart
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* Selector labels are a subset and must stay immutable across upgrades. */}}
{{- define "dart.selectorLabels" -}}
app: {{ include "dart.fullname" . }}
{{- end -}}

{{/*
Whether the EndpointSlice RBAC objects are needed: always in k8s discovery
mode (the dart-k8s variant cannot run without them), otherwise opt-in.
Rendered as the string "true"/"false" for use in eq comparisons.
*/}}
{{- define "dart.rbacNeeded" -}}
{{- if or .Values.rbac.create (eq .Values.discovery.mode "k8s") -}}true{{- else -}}false{{- end -}}
{{- end -}}

{{- define "dart.serviceAccountName" -}}
{{- if eq (include "dart.rbacNeeded" .) "true" -}}
{{- include "dart.fullname" . -}}
{{- else -}}
{{- "default" -}}
{{- end -}}
{{- end -}}

{{/*
The image for the selected discovery mode: the plain repository for dns, the
repository with a "-k8s" suffix (the Dockerfile's dart-k8s target) for k8s.
*/}}
{{- define "dart.image" -}}
{{- $suffix := ternary "-k8s" "" (eq .Values.discovery.mode "k8s") -}}
{{- printf "%s%s:%s" .Values.image.repository $suffix .Values.image.tag -}}
{{- end -}}

{{/* The -discover argument for the selected mode. */}}
{{- define "dart.discoverArg" -}}
{{- if eq .Values.discovery.mode "k8s" -}}
{{- printf "-discover=k8s:$(POD_NAMESPACE)/%s" (include "dart.fullname" .) -}}
{{- else -}}
{{- printf "-discover=dns:%s.$(POD_NAMESPACE).svc.cluster.local:%v" (include "dart.fullname" .) .Values.ports.peer -}}
{{- end -}}
{{- end -}}
