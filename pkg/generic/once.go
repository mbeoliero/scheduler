package generic

import "sync"

func Once[T any](f func() T) func() T {
	var (
		once sync.Once
		v    T
	)
	return func() T {
		once.Do(func() { v = f() })
		return v
	}
}
