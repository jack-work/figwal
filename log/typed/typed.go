// Package typed wraps figwal/log with JSON marshalling for a value type T.
//
// All non-typed methods (LastIndex, Hash, Close, …) are inherited from
// the embedded *log.Log. Read and Write are overridden to marshal T as
// JSON. To access the byte-oriented log directly, use tl.Log.
package typed

import (
	"encoding/json"
	"figwal/log"
)

// Log[T] is a *log.Log with JSON marshal/unmarshal for entries of type T.
type Log[T any] struct {
	*log.Log
}

func Open[T any](dir string, opts log.Options) (*Log[T], error) {
	l, err := log.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &Log[T]{Log: l}, nil
}

func (t *Log[T]) Write(idx uint64, v T) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return t.Log.Write(idx, b)
}

func (t *Log[T]) Read(idx uint64) (T, error) {
	var v T
	b, err := t.Log.Read(idx)
	if err != nil {
		return v, err
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return v, err
	}
	return v, nil
}
