package config

import (
	"encoding/json"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadStrategyFile 读取网格策略 YAML，转成 JSON，供 Supervisor.PutConfig 使用。
//
// 未写的字段由领域层 ApplyDefaults 补齐。价格/数量建议用字符串书写。
func LoadStrategyFile(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取策略文件 %s 失败: %w", path, err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("解析策略文件 %s 失败: %w", path, err)
	}
	if len(doc) == 0 {
		return nil, fmt.Errorf("策略文件 %s 是空的", path)
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("策略文件 %s 无法转为 JSON: %w", path, err)
	}
	return out, nil
}
