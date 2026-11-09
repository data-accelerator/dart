package store

import "io"

// disabledBudget occupies no slots, index, sketch or file. Tiered validates
// writes and declines disabled-class insertion before reaching this backend.
type disabledBudget struct{}

func (disabledBudget) Get(BlockKey) ([]byte, bool, error)                 { return nil, false, nil }
func (disabledBudget) GetReader(BlockKey) (io.Reader, int64, bool, error) { return nil, 0, false, nil }
func (disabledBudget) Put(BlockKey, []byte) error                         { return nil }
func (disabledBudget) Has(BlockKey) bool                                  { return false }
func (disabledBudget) Delete(BlockKey) bool                               { return false }
func (disabledBudget) Len() int                                           { return 0 }
func (disabledBudget) Slots() int                                         { return 0 }
func (disabledBudget) Close() error                                       { return nil }
func (disabledBudget) putIfAdmitted(BlockKey, []byte, func(BlockKey) bool) (bool, error) {
	return false, nil
}
