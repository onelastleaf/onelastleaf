package echo

import "testing"

func TestRun(t *testing.T) {
	result, err := Run(nil, []string{"one", "two"})
	if err != nil || result.String() != "one two" {
		t.Fatalf("unexpected result: %#v, %v", result, err)
	}
}
