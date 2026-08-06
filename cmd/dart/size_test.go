package main

import (
	"flag"
	"io"
	"testing"
)

func TestParseByteSize(t *testing.T) {
	cases := map[string]int64{
		// Bare byte counts, with and without the decorative B.
		"0":    0,
		"1":    1,
		"4096": 4096,
		"512B": 512,

		// Powers of two. The "i" is what selects them.
		"1KiB": 1024,
		"1Ki":  1024,
		"8MiB": 8 << 20,
		"8GiB": 8 << 30,
		"2TiB": 2 << 40,

		// Powers of ten. A cache sized 1MB is *not* 1MiB, and conflating the two
		// would silently mis-size the arena by ~5%.
		"1KB": 1_000,
		"1MB": 1_000_000,
		"1GB": 1_000_000_000,
		"1M":  1_000_000,

		// Fractions are useful for limits like 1.5GiB.
		"1.5GiB": 1<<30 + 1<<29,
		"0.5MiB": 512 << 10,

		// Case and surrounding space should not matter.
		"8gib":   8 << 30,
		"8GIB":   8 << 30,
		" 8MiB ": 8 << 20,
	}
	for in, want := range cases {
		got, err := parseByteSize(in)
		if err != nil {
			t.Errorf("parseByteSize(%q) error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("parseByteSize(%q) = %d, want %d", in, got, want)
		}
	}
}

// TestParseByteSizeBinaryVsDecimal states the distinction on its own, because it
// is the one a reader is most likely to assume away.
func TestParseByteSizeBinaryVsDecimal(t *testing.T) {
	bin, _ := parseByteSize("1GiB")
	dec, _ := parseByteSize("1GB")
	if bin == dec {
		t.Fatalf("1GiB and 1GB both parsed to %d; the i must select a power of two", bin)
	}
	if bin != 1<<30 || dec != 1_000_000_000 {
		t.Errorf("1GiB = %d (want %d), 1GB = %d (want %d)", bin, 1<<30, dec, 1_000_000_000)
	}
}

func TestParseByteSizeRejects(t *testing.T) {
	bad := []string{
		"",
		"   ",
		"abc",
		"MiB",        // no number
		"8XiB",       // unknown unit
		"8Mib/s",     // trailing junk
		"-1",         // negative
		"-4MiB",      // negative with a unit
		"1e400GiB",   // the number itself overflows a float64
		"5000000TiB", // ~5.5e18 bytes: parses, but exceeds the guard
	}
	for _, in := range bad {
		if got, err := parseByteSize(in); err == nil {
			t.Errorf("parseByteSize(%q) = %d, want an error", in, got)
		}
	}

	// A large but representable size must still be accepted, so the guard is not
	// just rejecting anything big.
	if got, err := parseByteSize("99999TiB"); err != nil {
		t.Errorf("parseByteSize(99999TiB) unexpectedly failed: %v", err)
	} else if got != 99999*(1<<40) {
		t.Errorf("parseByteSize(99999TiB) = %d, want %d", got, int64(99999)*(1<<40))
	}
}

// TestSizeFlagWiring checks the flag integration, including that the default is
// visible to the program when the flag is not passed.
func TestSizeFlagWiring(t *testing.T) {
	var v int64
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	sizeVar(fs, &v, "size", 7, "test size")

	if err := fs.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if v != 7 {
		t.Errorf("default not applied: v = %d, want 7", v)
	}

	fs2 := flag.NewFlagSet("t", flag.ContinueOnError)
	fs2.SetOutput(io.Discard)
	sizeVar(fs2, &v, "size", 7, "test size")
	if err := fs2.Parse([]string{"-size=64MiB"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if v != 64<<20 {
		t.Errorf("v = %d, want %d", v, 64<<20)
	}

	fs3 := flag.NewFlagSet("t", flag.ContinueOnError)
	fs3.SetOutput(io.Discard)
	sizeVar(fs3, &v, "size", 7, "test size")
	if err := fs3.Parse([]string{"-size=bogus"}); err == nil {
		t.Error("expected a parse error for -size=bogus")
	}
}

// TestSizeFlagsAcceptedByDart is the regression guard for the deployment
// manifests: they express sizes as 8GiB and similar, and before this parser
// existed those flags took a plain integer, so every such pod failed at startup
// with "parse error". Keep the shapes the manifests use working.
func TestSizeFlagsAcceptedByDart(t *testing.T) {
	args := []string{
		"-cache-size=8GiB", "-mem-size=512MiB",
		"-block-size=1MiB", "-chunk-size=8MiB",
	}
	cfg, err := parseFlags(args, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags(%v): %v", args, err)
	}
	if cfg.cacheSize != 8<<30 {
		t.Errorf("cacheSize = %d, want %d", cfg.cacheSize, int64(8)<<30)
	}
	if cfg.memSize != 512<<20 {
		t.Errorf("memSize = %d, want %d", cfg.memSize, int64(512)<<20)
	}
	if cfg.blockSize != 1<<20 {
		t.Errorf("blockSize = %d, want %d", cfg.blockSize, int64(1)<<20)
	}
	if cfg.chunkSize != 8<<20 {
		t.Errorf("chunkSize = %d, want %d", cfg.chunkSize, int64(8)<<20)
	}
}
