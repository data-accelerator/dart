package engine

import (
	"errors"
	"github.com/data-accelerator/dart/internal/fetch"
	"github.com/data-accelerator/dart/internal/peer"
)

// upstreamRefused preserves the credential refusal across a peer response.
type upstreamRefused struct {
	err  error
	code int
}

func (u *upstreamRefused) Error() string       { return u.err.Error() }
func (u *upstreamRefused) Unwrap() error       { return u.err }
func (u *upstreamRefused) UpstreamStatus() int { return u.code }

var _ peer.UpstreamRefusedError = (*upstreamRefused)(nil)

func asRelayError(err error) error {
	var se *fetch.StatusError
	if errors.As(err, &se) && se.Refused() {
		return &upstreamRefused{err: err, code: se.Code}
	}
	return err
}
