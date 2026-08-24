package grid

import (
	"errors"
	"testing"
)

func asIssue(err error, target **Issue) bool { return errors.As(err, target) }

// requireIssue 断言 err 是带指定 Code 的 *Issue。
func requireIssue(t *testing.T, err error, wantCode string) *Issue {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s, got nil", wantCode)
	}
	var issue *Issue
	if !errors.As(err, &issue) {
		t.Fatalf("expected *Issue, got %T: %v", err, err)
	}
	if issue.Code != wantCode {
		t.Fatalf("got code %s (%s), want %s", issue.Code, issue.Message, wantCode)
	}
	return issue
}

// hasWarning 判断派生结果里是否包含指定警告码。
func hasWarning(d Derived, code string) bool {
	for _, w := range d.Warnings {
		if w.Code == code {
			return true
		}
	}
	return false
}
