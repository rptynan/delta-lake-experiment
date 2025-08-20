package utils

func Assert(b bool, msg string) {
	if !b {
		panic(msg)
	}
}
