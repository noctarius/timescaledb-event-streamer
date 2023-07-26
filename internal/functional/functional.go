package functional

import "sort"

func Zero[T any]() (t T) {
	return
}

func MappingTransformer[T, V any](
	transformer func(T) V,
) func(T, int) V {

	return func(value T, _ int) V {
		return transformer(value)
	}
}

func Sort[I any](
	collection []I, less func(this, other I) bool,
) []I {

	sort.Slice(collection, func(i, j int) bool {
		return less(collection[i], collection[j])
	})
	return collection
}

func ArrayEqual[T comparable](
	this, that []T,
) bool {

	if (this == nil && that != nil) || (this != nil && that == nil) {
		return false
	}
	if len(this) != len(that) {
		return false
	}
	for i := 0; i < len(this); i++ {
		if this[i] != that[i] {
			return false
		}
	}
	return true
}
