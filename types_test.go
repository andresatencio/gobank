package main

import (
	"fmt"
	"testing"
)

func TestNewAccount(t *testing.T) {
	acc, err := NewAccount("a", "b", "c")
	if err != nil {
		t.Fatalf("error")
	}

	if acc.FirstName != "a" {
		t.Fatalf("error")
	}

	fmt.Printf("%+v", acc)
}
