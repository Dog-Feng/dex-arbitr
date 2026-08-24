package rhlighter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"dex-grid/internal/exchange"

	lclient "github.com/elliottech/lighter-go/client"
	ltypes "github.com/elliottech/lighter-go/types"
	"github.com/elliottech/lighter-go/types/txtypes"
)

// txSender 负责签名并提交 L2 交易。
//
// Lighter 要求同一个 (account_index, api_key_index) 下的 nonce 严格递增且没有空洞。
// 因为本系统一个交易所只跑一个实例，这里用一把互斥锁把所有出站交易串行化就够了，
// 不需要跨实例的 nonce 分配器。
type txSender struct {
	tx              *lclient.TxClient
	rest            *restClient
	accountIndex    int64
	apiKeyIndex     uint8
	priceProtection bool

	mu         sync.Mutex
	nonce      int64
	hasNonce   bool
	cachedAuth string
	authExpiry time.Time
}

func newTxSender(rest *restClient, privateKey string, accountIndex int64, apiKeyIndex uint8, chainID uint32, priceProtection bool) (*txSender, error) {
	// 传 nil 作为 HTTP 客户端：nonce 由我们自己管理，绝不让 SDK 在签名路径上偷偷发请求。
	tx, err := lclient.NewTxClient(nil, privateKey, accountIndex, apiKeyIndex, chainID)
	if err != nil {
		return nil, fmt.Errorf("rh_lighter: 初始化签名客户端失败: %w", err)
	}
	return &txSender{
		tx:              tx,
		rest:            rest,
		accountIndex:    accountIndex,
		apiKeyIndex:     apiKeyIndex,
		priceProtection: priceProtection,
	}, nil
}

// builder 用给定的 nonce 构造一笔已签名的交易。
type builder func(ops *ltypes.TransactOpts) (txtypes.TxInfo, error)

// send 串行地取号、签名、提交。
//
// op 只用于错误分类与日志，不参与签名。
func (s *txSender) send(ctx context.Context, op string, build builder) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nonce, err := s.currentNonce(ctx)
	if err != nil {
		return "", classify(op, err)
	}

	ops := &ltypes.TransactOpts{
		FromAccountIndex: &s.accountIndex,
		ApiKeyIndex:      &s.apiKeyIndex,
		Nonce:            &nonce,
	}
	txInfo, err := build(ops)
	if err != nil {
		return "", exchange.Classify(exchange.ClassFatal, op,
			fmt.Errorf("rh_lighter: 构造交易失败: %w", err))
	}
	if err := txInfo.Validate(); err != nil {
		// 本地校验失败说明参数有问题，交易根本没发出去，nonce 不消耗。
		return "", exchange.Classify(exchange.ClassInvalidParam, op,
			fmt.Errorf("rh_lighter: 交易参数非法: %w", err))
	}
	payload, err := txInfo.GetTxInfo()
	if err != nil {
		return "", exchange.Classify(exchange.ClassFatal, op,
			fmt.Errorf("rh_lighter: 序列化交易失败: %w", err))
	}

	resp, err := s.rest.sendTx(ctx, txInfo.GetTxType(), payload, s.priceProtection)
	if err != nil {
		s.onSendFailure(err)
		return "", classify(op, err)
	}

	s.nonce = nonce + 1
	return resp.TxHash, nil
}

// currentNonce 返回下一个可用的 nonce，必要时向服务端校准。
func (s *txSender) currentNonce(ctx context.Context) (int64, error) {
	if s.hasNonce {
		return s.nonce, nil
	}
	n, err := s.rest.nextNonce(ctx, s.accountIndex, s.apiKeyIndex)
	if err != nil {
		return 0, fmt.Errorf("rh_lighter: 获取 nonce 失败: %w", err)
	}
	s.nonce = n
	s.hasNonce = true
	return s.nonce, nil
}

// onSendFailure 决定失败后 nonce 怎么处理。
//
// 服务端明确拒绝（4xx）说明交易没有被接受，nonce 可以原样复用。
// 网络层错误无法确认交易是否已被接受，此时绝不能猜——标记为需要重新校准，
// 下一笔交易会重新问服务端要 nonce。猜错的代价是后续所有交易连续失败。
func (s *txSender) onSendFailure(err error) {
	var ae *apiError
	if errorsAs(err, &ae) && ae.Status >= 400 && ae.Status < 500 {
		return
	}
	s.hasNonce = false
}

// ResetNonce 强制下一笔交易重新向服务端取号。
func (s *txSender) ResetNonce() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hasNonce = false
}

// authToken 生成访问私有 REST/WS 端点所需的令牌。
//
// 令牌有效期最长 8 小时且与 API key 绑定，这里缓存到过期前 30 分钟。
func (s *txSender) authToken() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cachedAuth != "" && time.Now().Before(s.authExpiry.Add(-30*time.Minute)) {
		return s.cachedAuth, nil
	}
	deadline := time.Now().Add(7 * time.Hour)
	token, err := s.tx.GetAuthToken(deadline)
	if err != nil {
		return "", fmt.Errorf("rh_lighter: 生成 auth token 失败: %w", err)
	}
	s.cachedAuth = token
	s.authExpiry = deadline
	return token, nil
}
