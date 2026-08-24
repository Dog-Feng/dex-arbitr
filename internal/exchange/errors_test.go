package exchange

import (
	"errors"
	"testing"
)

func TestClassifyNil(t *testing.T) {
	if Classify(ClassRetryable, "place", nil) != nil {
		t.Fatal("Classify(nil) should return nil")
	}
}

func TestClassOf(t *testing.T) {
	if ClassOf(nil) != ClassUnknown {
		t.Fatal("ClassOf(nil) should be unknown")
	}
	if ClassOf(errors.New("plain")) != ClassUnknown {
		t.Fatal("unclassified error should be unknown")
	}

	err := Classify(ClassRetryable, "place_order", errors.New("timeout"))
	if ClassOf(err) != ClassRetryable {
		t.Fatalf("got %s, want retryable", ClassOf(err))
	}
	if !ClassOf(err).Retryable() {
		t.Fatal("retryable class should Retryable()")
	}
	if !ClassOf(err).CountsAsFailure() {
		t.Fatal("retryable should count as failure")
	}

	por := Classify(ClassPostOnlyRejected, "place_order", errors.New("would cross"))
	if ClassOf(por).CountsAsFailure() {
		t.Fatal("post-only reject must not count as failure")
	}
	if ClassOf(por).Retryable() {
		t.Fatal("post-only reject is not a retryable transport error")
	}
}

func TestErrorString(t *testing.T) {
	err := Classify(ClassFatal, "set_leverage", errors.New("bad key"))
	got := err.Error()
	if got != "set_leverage [fatal]: bad key" {
		t.Fatalf("got %q", got)
	}
	if !errors.Is(err, err.(*Error).Err) {
		t.Fatal("Unwrap should expose the inner error")
	}
}
