package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// envPattern 匹配 ${VAR} 形式的占位符。
var envPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Load 读取并校验配置文件。
//
// 顺序是：加载同目录 .env → 读文件 → 展开 ${VAR} → 解析 → 填默认值 → 校验。
// 已存在的系统环境变量优先级高于 .env。
func Load(path string) (*Config, error) {
	if err := LoadDotEnv(DefaultDotEnvPath()); err != nil {
		return nil, err
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件 %s 失败: %w", path, err)
	}

	expanded, missing := expandEnv(raw)

	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(string(expanded)))
	dec.KnownFields(true) // 配置项写错名字要报错，不能被静默忽略
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 %s 失败: %w", path, err)
	}

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		if len(missing) > 0 {
			return nil, fmt.Errorf("%w（提示：以下环境变量未设置，已展开为空：%s）",
				err, strings.Join(missing, ", "))
		}
		return nil, err
	}
	return &cfg, nil
}

// expandEnv 展开 ${VAR}。未设置的变量展开为空串，并记录下来供报错时提示。
func expandEnv(raw []byte) (out []byte, missing []string) {
	seen := map[string]bool{}
	out = envPattern.ReplaceAllFunc(raw, func(m []byte) []byte {
		name := string(envPattern.FindSubmatch(m)[1])
		if v, ok := os.LookupEnv(name); ok {
			return []byte(v)
		}
		if !seen[name] {
			seen[name] = true
			missing = append(missing, name)
		}
		return nil
	})
	return out, missing
}

// DefaultDotEnvPath 返回可执行文件同目录下的 .env 路径。
//
// 用可执行文件目录而不是当前工作目录：Windows 服务与计划任务的工作目录
// 常常不是安装目录，用 cwd 会找不到文件。
func DefaultDotEnvPath() string {
	exe, err := os.Executable()
	if err != nil {
		return ".env"
	}
	return filepath.Join(filepath.Dir(exe), ".env")
}

// LoadDotEnv 加载 .env 文件。文件不存在不算错误。
//
// 已存在的系统环境变量不会被覆盖，方便临时用 shell 变量压过文件里的值。
func LoadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("读取 %s 失败: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		text = strings.TrimPrefix(text, "export ")
		key, value, ok := strings.Cut(text, "=")
		if !ok {
			return fmt.Errorf("%s:%d 格式错误，应为 KEY=VALUE", path, line)
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}
	return sc.Err()
}

// ParseDuration 解析时长，额外支持纯 "Nd" 形式。
func ParseDuration(s string) (Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.ParseFloat(days, 64)
		if err != nil {
			return 0, fmt.Errorf("无法解析时长 %q: %w", s, err)
		}
		return Duration(time.Duration(n * float64(24*time.Hour))), nil
	}
	std, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("无法解析时长 %q: %w", s, err)
	}
	return Duration(std), nil
}

// ResolvePath 把相对路径解析为相对可执行文件目录的绝对路径。
func ResolvePath(p string) string {
	if p == "" || filepath.IsAbs(p) {
		return p
	}
	exe, err := os.Executable()
	if err != nil {
		return p
	}
	return filepath.Join(filepath.Dir(exe), p)
}
