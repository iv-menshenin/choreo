package id

import (
	"crypto/rand"
	"fmt"
)

type ID [16]byte

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
