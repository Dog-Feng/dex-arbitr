package market

// 枚举的文本编解码。encoding/json 会自动使用 TextMarshaler/TextUnmarshaler，
// 因此配置文件与 API 里这些字段都是可读的字符串而不是数字。

func (m MarginMode) MarshalText() ([]byte, error) { return []byte(m.String()), nil }

func (m *MarginMode) UnmarshalText(b []byte) error {
	v, err := ParseMarginMode(string(b))
	if err != nil {
		return err
	}
	*m = v
	return nil
}

func (p PriceSource) MarshalText() ([]byte, error) { return []byte(p.String()), nil }

func (p *PriceSource) UnmarshalText(b []byte) error {
	v, err := ParsePriceSource(string(b))
	if err != nil {
		return err
	}
	*p = v
	return nil
}
