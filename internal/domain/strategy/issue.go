package strategy

import "fmt"

// Issue 是一条校验结果，既用作错误也用作警告。
type Issue struct {
	Code    string `json:"code"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

func (i *Issue) Error() string { return i.Message }

// Errf 构造一条阻断性校验错误。
func Errf(code, field, format string, args ...any) *Issue {
	return &Issue{Code: code, Field: field, Message: fmt.Sprintf(format, args...)}
}

// Warnf 构造一条不阻断保存的警告。
func Warnf(code, field, format string, args ...any) Issue {
	return Issue{Code: code, Field: field, Message: fmt.Sprintf(format, args...)}
}
