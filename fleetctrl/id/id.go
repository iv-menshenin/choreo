package id

import (
	"crypto/rand"
	"fmt"
)

const Size = 16

type ID [Size]byte

var NullID ID

func New() ID {
	var x ID
	if _, err := rand.Read(x[:]); err != nil {
		panic(err)
	}
	return x
}

func (x ID) String() string {
	return fmt.Sprintf("%x", x[:])
}

func (x ID) Less(y ID) bool {
	for n := 0; n < len(x); n++ {
		switch {
		case x[n] < y[n]:
			return true
		case x[n] > y[n]:
			return false
		}
	}
	return false
}
