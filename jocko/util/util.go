package util

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
)

func Dump(i interface{}) string {
	return fmt.Sprintf("%s", i)
	// return strings.Replace(spew.Sdump(i), "\n", "", -1)
}

func Hash(s string) uint64 {
	h := xxhash.New()
	if _, err := h.WriteString(s); err != nil {
		panic(err)
	}
	return h.Sum64()
}
