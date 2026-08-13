package node

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
)

// byteSize is a flag.Value for sizes expressed either as a plain byte count or
// with a unit suffix, so a cache size can be written 8GiB instead of 8589934592.
//
// Suffixes follow the Kubernetes convention, since DART's manifests sit next to
// container resource limits and mixing two conventions in one file would be a
// trap: an "i" means a power of two (KiB/MiB/GiB/TiB, also accepted as Ki/Mi/Gi/Ti)
// and its absence means a power of ten (KB/MB/GB/TB, also K/M/G/T). A bare number,
// or one suffixed with B, is bytes. Matching is case-insensitive.
type byteSize int64

func (b byteSize) String() string { return strconv.FormatInt(int64(b), 10) }

// Set parses s. It rejects negative and unparseable values; zero is allowed
// because several sizes use it to mean "disabled".
func (b *byteSize) Set(s string) error {
	v, err := parseByteSize(s)
	if err != nil {
		return err
	}
	*b = byteSize(v)
	return nil
}

func parseByteSize(s string) (int64, error) {
	t := strings.TrimSpace(s)
	if t == "" {
		return 0, fmt.Errorf("empty size")
	}

	// Split the numeric head from the unit tail.
	i := 0
	for i < len(t) && (t[i] >= '0' && t[i] <= '9' || t[i] == '.' || t[i] == '-' || t[i] == '+') {
		i++
	}
	num, unit := t[:i], strings.ToLower(strings.TrimSpace(t[i:]))
	if num == "" {
		return 0, fmt.Errorf("no number in %q", s)
	}

	// "B" is decoration on any form: 512B, 8GiB, 100MB.
	unit = strings.TrimSuffix(unit, "b")

	mult := int64(1)
	if unit != "" {
		var ok bool
		if mult, ok = unitMultiplier(unit); !ok {
			return 0, fmt.Errorf("unknown size unit %q in %q (want KiB/MiB/GiB/TiB or KB/MB/GB/TB)", unit, s)
		}
	}

	// Accept a fraction so 1.5GiB works, but the result is an integral count of
	// bytes.
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("bad size %q", s)
	}
	if f < 0 {
		return 0, fmt.Errorf("negative size %q", s)
	}
	bytes := f * float64(mult)
	// Guard the conversion: a float that exceeds int64 range would wrap silently.
	if bytes > 1<<62 {
		return 0, fmt.Errorf("size %q is too large", s)
	}
	return int64(bytes), nil
}

func unitMultiplier(unit string) (int64, bool) {
	switch unit {
	case "ki":
		return 1 << 10, true
	case "mi":
		return 1 << 20, true
	case "gi":
		return 1 << 30, true
	case "ti":
		return 1 << 40, true
	case "k":
		return 1_000, true
	case "m":
		return 1_000_000, true
	case "g":
		return 1_000_000_000, true
	case "t":
		return 1_000_000_000_000, true
	}
	return 0, false
}

// sizeVar defines a size flag on fs, backed by an int64 the rest of the program
// can use directly.
func sizeVar(fs *flag.FlagSet, p *int64, name string, def int64, usage string) {
	*p = def
	fs.Var((*byteSize)(p), name, usage)
}
