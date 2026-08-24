package order

import "fmt"

func ParseSide(s string) (Side, error) {
	switch s {
	case "buy":
		return Buy, nil
	case "sell":
		return Sell, nil
	default:
		return 0, fmt.Errorf("unknown side %q", s)
	}
}

func ParseTIF(s string) (TIF, error) {
	switch s {
	case "post_only", "":
		return PostOnly, nil
	case "gtc":
		return GTC, nil
	case "ioc":
		return IOC, nil
	default:
		return 0, fmt.Errorf("unknown time-in-force %q", s)
	}
}

func (s Side) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

func (s *Side) UnmarshalText(b []byte) error {
	v, err := ParseSide(string(b))
	if err != nil {
		return err
	}
	*s = v
	return nil
}

func (t TIF) MarshalText() ([]byte, error) { return []byte(t.String()), nil }

func (t *TIF) UnmarshalText(b []byte) error {
	v, err := ParseTIF(string(b))
	if err != nil {
		return err
	}
	*t = v
	return nil
}
