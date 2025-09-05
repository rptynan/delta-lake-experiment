package utils

import (
	"fmt"
	"os"
	"slices"
)

func Assert(b bool, msg string) {
	if !b {
		panic(msg)
	}
}

func AssertEq[C comparable](a C, b C, prefix string) {
	if a != b {
		panic(fmt.Sprintf("%s '%v' != '%v'", prefix, a, b))
	}
}

func AssertNil(a any) {
	if a != nil {
		panic(fmt.Sprintf("variable is nil, expected non-nil: %v", a))
	}
}

var DEBUG = slices.Contains(os.Args, "--debug")

func Debug(a ...any) {
	if !DEBUG {
		return
	}

	args := append([]any{"[DEBUG]"}, a...)
	fmt.Println(args...)
}

// Because of JSON, all our numbers come back as floats. We are just going to assume everything is an int for now.
func AsInt(x any) (int, error) {
	switch v := x.(type) {
	case int:
		return v, nil
	case float64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("Wasn't able to cast %v to int", x)
	}
}
