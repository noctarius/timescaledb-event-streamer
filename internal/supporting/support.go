package supporting

import (
	"math/rand"
	"strings"
)

var validCharacters = []string{
	"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
	"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
}

func RandomTextString(length int) string {
	builder := strings.Builder{}
	for i := 0; i < length; i++ {
		index := rand.Intn(len(validCharacters))
		builder.WriteString(validCharacters[index])
	}
	return builder.String()
}
