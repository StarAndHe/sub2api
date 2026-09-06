package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/tidwall/gjson"
)

const (
	openAIStaleRateLimitRecoveryProbeLimit = 2
	openAIStaleRateLimitRecoveryCooldown   = time.Minute
	openAIStaleRateLimitRecoveryTimeout    = 15 * time.Second
)

type openAIObservedModelRateLimitRecoveryRepository interface {
	ClearModelRateLimitIfObserved(ctx context.Context, id int64, scope string, observedLimitedAt, observedResetAt time.Time) (bool, error)
}

type openAIObservedModelRateLimit struct {
	scope     string
	limitedAt time.Time
	resetAt   time.Time
}

type openAIStaleRateLimitRecoveryResult struct {
	RecoveredAccountID int64
	Probed             int
}

func openAIResponsesStreamCompleted(body io.Reader) (bool, error) {
	if body == nil {
		return false, nil
	}
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" || data == "[DONE]" {
			continue
		}
		switch gjson.Get(data, "type").String() {
		case "response.completed", "response.done":
			return true, nil
		case "response.failed", "response.incomplete", "response.cancelled", "response.canceled", "error":
			return false, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func (s *OpenAIGatewayService) tryRecoverStaleOpenAIRateLimit(
	ctx context.Context,
	req OpenAIAccountScheduleRequest,
) (bool, error) {
	if s == nil || s.accountRepo == nil || s.httpUpstream == nil || normalizeOpenAICompatiblePlatform(req.Platform) != PlatformOpenAI {
		return false, nil
	}
	model := strings.TrimSpace(req.RequestedModel)
	if model == "" || req.RequireCompact {
		return false, nil
	}

	key := fmt.Sprintf("%d|%s", derefGroupID(req.GroupID), model)
	value, err, _ := s.openaiRateLimitRecoveryFlight.Do(key, func() (any, error) {
		return s.recoverOneStaleOpenAIRateLimit(ctx, req)
	})
	if err != nil {
		return false, err
	}
	result, _ := value.(openAIStaleRateLimitRecoveryResult)
	return result.RecoveredAccountID > 0, nil
}

func (s *OpenAIGatewayService) recoverOneStaleOpenAIRateLimit(
	ctx context.Context,
	req OpenAIAccountScheduleRequest,
) (openAIStaleRateLimitRecoveryResult, error) {
	result := openAIStaleRateLimitRecoveryResult{}
	queryGroupID := req.GroupID
	includeGrouped := false
	if s.cfg != nil && s.cfg.RunMode == config.RunModeSimple {
		queryGroupID = nil
		includeGrouped = true
	}
	accounts, err := s.accountRepo.ListModelAvailabilityCandidates(ctx, queryGroupID, []string{PlatformOpenAI}, includeGrouped)
	if err != nil {
		return result, err
	}

	now := time.Now()
	candidates := make([]*Account, 0, len(accounts))
	for i := range accounts {
		account := &accounts[i]
		if !s.isStaleOpenAIRateLimitRecoveryCandidate(ctx, account, req, now) {
			continue
		}
		candidates = append(candidates, account)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if left.Priority != right.Priority {
			return left.Priority < right.Priority
		}
		return left.ID < right.ID
	})

	for _, account := range candidates {
		if result.Probed >= openAIStaleRateLimitRecoveryProbeLimit {
			break
		}
		if !s.claimOpenAIStaleRateLimitRecoveryProbe(account.ID, now) {
			continue
		}
		result.Probed++
		recovered, probeErr := s.probeAndRecoverOpenAIAccount(ctx, account, req.RequestedModel)
		if probeErr != nil {
			slog.Warn("openai_stale_rate_limit_recovery_probe_failed", "account_id", account.ID, "model", req.RequestedModel, "error", probeErr)
			continue
		}
		if recovered {
			result.RecoveredAccountID = account.ID
			slog.Info("openai_stale_rate_limit_recovered", "account_id", account.ID, "model", req.RequestedModel)
			return result, nil
		}
	}
	return result, nil
}

func (s *OpenAIGatewayService) isStaleOpenAIRateLimitRecoveryCandidate(
	ctx context.Context,
	account *Account,
	req OpenAIAccountScheduleRequest,
	now time.Time,
) bool {
	if account == nil || !account.IsOpenAIOAuth() || account.IsShadow() || !account.IsActive() || !account.Schedulable {
		return false
	}
	if req.ExcludedIDs != nil {
		if _, excluded := req.ExcludedIDs[account.ID]; excluded {
			return false
		}
	}
	if account.AutoPauseOnExpired && account.ExpiresAt != nil && !now.Before(*account.ExpiresAt) {
		return false
	}
	if account.OverloadUntil != nil && now.Before(*account.OverloadUntil) {
		return false
	}
	if account.TempUnschedulableUntil != nil && now.Before(*account.TempUnschedulableUntil) {
		return false
	}
	runtimeBlock := s.openAIAccountRuntimeBlockSnapshot(account)
	if runtimeBlock.blocked && runtimeBlock.reason != "429" {
		return false
	}
	if s.isOpenAIAccountModelRuntimeBlocked(account, req.RequestedModel) {
		return false
	}
	if !account.IsModelSupported(req.RequestedModel) {
		return false
	}
	if !accountSupportsOpenAICapabilities(account, req.RequiredCapability, req.RequiredImageCapability) {
		return false
	}
	if paused, _ := shouldAutoPauseOpenAIAccountByQuota(ctx, account); paused {
		return false
	}
	globalLimited := account.RateLimitedAt != nil && account.RateLimitResetAt != nil && now.Before(*account.RateLimitResetAt)
	modelLimited := len(observedOpenAIModelRateLimits(ctx, account, req.RequestedModel, now)) > 0
	return globalLimited || modelLimited
}

func (s *OpenAIGatewayService) claimOpenAIStaleRateLimitRecoveryProbe(accountID int64, now time.Time) bool {
	actual, _ := s.openaiRateLimitRecoveryProbeLocks.LoadOrStore(accountID, &sync.Mutex{})
	mu, _ := actual.(*sync.Mutex)
	if mu == nil {
		return false
	}
	mu.Lock()
	defer mu.Unlock()
	if value, ok := s.openaiRateLimitRecoveryProbeAt.Load(accountID); ok {
		if last, ok := value.(time.Time); ok && now.Sub(last) < openAIStaleRateLimitRecoveryCooldown {
			return false
		}
	}
	s.openaiRateLimitRecoveryProbeAt.Store(accountID, now)
	return true
}

func (s *OpenAIGatewayService) probeAndRecoverOpenAIAccount(
	ctx context.Context,
	account *Account,
	requestedModel string,
) (bool, error) {
	if account == nil || account.RateLimitedAt == nil && len(observedOpenAIModelRateLimits(ctx, account, requestedModel, time.Now())) == 0 {
		return false, nil
	}
	observedLimitedAt := account.RateLimitedAt
	observedResetAt := account.RateLimitResetAt
	observedModelLimits := observedOpenAIModelRateLimits(ctx, account, requestedModel, time.Now())
	observedRuntimeBlock := s.openAIAccountRuntimeBlockSnapshot(account)

	probeCtx, cancel := context.WithTimeout(ctx, openAIStaleRateLimitRecoveryTimeout)
	defer cancel()
	accessToken, authMode, err := s.GetAccessToken(probeCtx, account)
	if err != nil {
		return false, err
	}
	if authMode != "oauth" || strings.TrimSpace(accessToken) == "" {
		return false, nil
	}

	upstreamModel := normalizeOpenAIModelForUpstream(account, account.GetMappedModel(requestedModel))
	payload := map[string]any{
		"model": upstreamModel,
		"input": []map[string]any{{
			"role":    "user",
			"content": []map[string]any{{"type": "input_text", "text": "Reply with OK."}},
		}},
		"instructions": "Reply briefly.",
		"stream":       true,
		"store":        false,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(probeCtx, http.MethodPost, chatgptCodexURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return false, err
	}
	req.Host = "chatgpt.com"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer "+accessToken)
	applyOpenAICodexProbeHeaders(req.Header)
	enforceCodexIdentityHeadersWithUA(req.Header, s.codexIdentityOverrideUA(account))
	setOpenAIChatGPTAccountHeaders(req.Header, account)
	setOpenAICodexRoutingHint(req.Header, account, upstreamModel, "")

	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.DoWithTLS(req, proxyURL, account.ID, account.Concurrency, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if updates, updateErr := extractOpenAICodexProbeUpdates(resp); updateErr == nil && len(updates) > 0 {
		if err := s.accountRepo.UpdateExtra(probeCtx, account.ID, updates); err != nil {
			slog.Warn("openai_stale_rate_limit_snapshot_update_failed", "account_id", account.ID, "error", err)
		}
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
		return false, nil
	}
	completed, err := openAIResponsesStreamCompleted(resp.Body)
	if err != nil || !completed {
		return false, err
	}

	recovered := false
	if observedLimitedAt != nil && observedResetAt != nil {
		if repo, ok := s.accountRepo.(grokRateLimitRecoveryRepository); ok {
			cleared, clearErr := repo.ClearRateLimitIfObserved(probeCtx, account.ID, *observedLimitedAt, *observedResetAt)
			if clearErr != nil {
				return false, clearErr
			}
			recovered = recovered || cleared
		}
	}
	if repo, ok := s.accountRepo.(openAIObservedModelRateLimitRecoveryRepository); ok {
		for _, observed := range observedModelLimits {
			cleared, clearErr := repo.ClearModelRateLimitIfObserved(probeCtx, account.ID, observed.scope, observed.limitedAt, observed.resetAt)
			if clearErr != nil {
				return recovered, clearErr
			}
			recovered = recovered || cleared
		}
	}
	if recovered && observedRuntimeBlock.reason == "429" {
		s.clearOpenAIAccountRuntimeBlockIfObserved(account.ID, observedRuntimeBlock)
	}
	return recovered, nil
}

func observedOpenAIModelRateLimits(ctx context.Context, account *Account, requestedModel string, now time.Time) []openAIObservedModelRateLimit {
	if account == nil {
		return nil
	}
	result := make([]openAIObservedModelRateLimit, 0, 2)
	for _, scope := range account.modelRateLimitKeysForRequest(ctx, requestedModel) {
		limitedAt, resetAt, ok := account.modelRateLimitGeneration(scope)
		if !ok || !now.Before(resetAt) {
			continue
		}
		result = append(result, openAIObservedModelRateLimit{scope: scope, limitedAt: limitedAt, resetAt: resetAt})
	}
	return result
}

// PatrolRecoverStaleOpenAIRateLimits 后台巡查一轮：列出所有 active + schedulable 的
// OpenAI OAuth 账号（跨分组），找出「当前仍被账号级 429 挡住」的候选，逐个探测上游；
// 上游已能正常完成响应时按既有 CAS 语义清除旧限流并同步调度缓存。返回 (探测数, 恢复数)。
//
// 与请求路径的 tryRecoverStaleOpenAIRateLimit 区别：
//   - 没有请求模型上下文，候选判定只关注账号级限流（RateLimitedAt/ResetAt），
//     不检查具体请求模型是否支持——429 是账号级额度窗口，与请求模型无关；
//   - 刻意不因 shouldAutoPauseOpenAIAccountByQuota 排除候选：外部重置额度后本地
//     快照可能仍显示耗尽，若因 auto-pause 跳过则永远不会被探测、永远无法自愈。
//
// maxAccounts <= 0 表示不限制本轮探测数量。探测模型沿用页面额度刷新的
// selectResponsesProbeModel（账号映射的上游模型，空映射回退 DefaultTestModel）。
func (s *OpenAIGatewayService) PatrolRecoverStaleOpenAIRateLimits(ctx context.Context, maxAccounts int) (int, int, error) {
	if s == nil || s.accountRepo == nil || s.httpUpstream == nil {
		return 0, 0, nil
	}
	accounts, err := s.accountRepo.ListModelAvailabilityCandidates(ctx, nil, []string{PlatformOpenAI}, true)
	if err != nil {
		return 0, 0, err
	}

	now := time.Now()
	candidates := make([]*Account, 0, len(accounts))
	for i := range accounts {
		account := &accounts[i]
		if !s.isOpenAI429PatrolCandidate(account, now) {
			continue
		}
		candidates = append(candidates, account)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if left.Priority != right.Priority {
			return left.Priority < right.Priority
		}
		return left.ID < right.ID
	})

	probed := 0
	recovered := 0
	for _, account := range candidates {
		if maxAccounts > 0 && probed >= maxAccounts {
			break
		}
		if !s.claimOpenAIStaleRateLimitRecoveryProbe(account.ID, now) {
			continue
		}
		probed++
		probeModel := selectResponsesProbeModel(account)
		ok, probeErr := s.probeAndRecoverOpenAIAccount(ctx, account, probeModel)
		if probeErr != nil {
			slog.Warn("openai_rate_limit_patrol.probe_failed",
				"account_id", account.ID,
				"model", probeModel,
				"error", probeErr,
			)
			continue
		}
		if ok {
			recovered++
			slog.Info("openai_rate_limit_patrol.account_recovered", "account_id", account.ID, "model", probeModel)
		}
	}
	return probed, recovered, nil
}

// isOpenAI429PatrolCandidate 判定账号是否为「仅被账号级 429 挡住」的巡查候选。
// 账号级限流记录（RateLimitedAt/ResetAt）在重置窗口内未过期才算候选；error、
// 手动停用、过载、401 引起的临时不可调度等其它阻断状态一律不纳入，避免巡查
// 用额度探测去解除非额度问题。
func (s *OpenAIGatewayService) isOpenAI429PatrolCandidate(account *Account, now time.Time) bool {
	if account == nil || !account.IsOpenAIOAuth() || account.IsShadow() || !account.IsActive() || !account.Schedulable {
		return false
	}
	if account.RateLimitedAt == nil || account.RateLimitResetAt == nil {
		return false
	}
	if !now.Before(*account.RateLimitResetAt) {
		return false
	}
	if account.AutoPauseOnExpired && account.ExpiresAt != nil && !now.Before(*account.ExpiresAt) {
		return false
	}
	if account.OverloadUntil != nil && now.Before(*account.OverloadUntil) {
		return false
	}
	if account.TempUnschedulableUntil != nil && now.Before(*account.TempUnschedulableUntil) {
		return false
	}
	return true
}
