package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

type rateLimitPatrolRepo struct {
	stubOpenAIAccountRepo
	mu         sync.Mutex
	clearCalls int
	clearedID  int64
}

func (r *rateLimitPatrolRepo) ListModelAvailabilityCandidates(context.Context, *int64, []string, bool) ([]Account, error) {
	return append([]Account(nil), r.accounts...), nil
}

func (r *rateLimitPatrolRepo) UpdateExtra(context.Context, int64, map[string]any) error {
	return nil
}

func (r *rateLimitPatrolRepo) ClearRateLimitIfObserved(_ context.Context, id int64, limitedAt, resetAt time.Time) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clearCalls++
	for i := range r.accounts {
		account := &r.accounts[i]
		if account.ID != id || account.RateLimitedAt == nil || account.RateLimitResetAt == nil {
			continue
		}
		if !account.RateLimitedAt.Equal(limitedAt) || !account.RateLimitResetAt.Equal(resetAt) {
			return false, nil
		}
		account.RateLimitedAt = nil
		account.RateLimitResetAt = nil
		r.clearedID = id
		return true, nil
	}
	return false, nil
}

func (r *rateLimitPatrolRepo) ClearModelRateLimitIfObserved(context.Context, int64, string, time.Time, time.Time) (bool, error) {
	return false, nil
}

// patrolUpstream 每次探测都返回一个全新的已完成 SSE 响应体，模拟多次独立探测。
type patrolUpstream struct {
	calls int
}

func (u *patrolUpstream) Do(*http.Request, string, int64, int) (*http.Response, error) {
	return u.freshResponse(), nil
}

func (u *patrolUpstream) DoWithTLS(*http.Request, string, int64, int, *tlsfingerprint.Profile) (*http.Response, error) {
	u.calls++
	return u.freshResponse(), nil
}

func (u *patrolUpstream) freshResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\"}\n\n")),
	}
}

func openAIRateLimitPatrolProbeAccount(platform, accountType, status string, schedulable bool, limitedAt, resetAt *time.Time) Account {
	return Account{
		ID:               100,
		Platform:         platform,
		Type:             accountType,
		Status:           status,
		Schedulable:      schedulable,
		Concurrency:      1,
		RateLimitedAt:    limitedAt,
		RateLimitResetAt: resetAt,
		Credentials: map[string]any{
			"access_token":       "patrol-test-token",
			"chatgpt_account_id": "patrol-test-account",
		},
	}
}

func TestIsOpenAI429PatrolCandidate(t *testing.T) {
	t.Parallel()
	now := time.Now()
	limitedAt := now.Add(-time.Minute)
	resetAt := now.Add(time.Hour)
	svc := &OpenAIGatewayService{}

	active := openAIRateLimitPatrolProbeAccount(PlatformOpenAI, AccountTypeOAuth, StatusActive, true, &limitedAt, &resetAt)
	if !svc.isOpenAI429PatrolCandidate(&active, now) {
		t.Fatal("expected active OAuth account with active account-level 429 to be a patrol candidate")
	}

	// 非 OpenAI / 非 OAuth / error / 手动停用 / 影子 / 过期限流均不应纳入。
	notOpenAI := active
	notOpenAI.Platform = PlatformGemini
	if svc.isOpenAI429PatrolCandidate(&notOpenAI, now) {
		t.Fatal("non-OpenAI account must not be a patrol candidate")
	}

	apiKey := active
	apiKey.Type = AccountTypeAPIKey
	if svc.isOpenAI429PatrolCandidate(&apiKey, now) {
		t.Fatal("API key account must not be a patrol candidate")
	}

	errorAcct := active
	errorAcct.Status = StatusError
	if svc.isOpenAI429PatrolCandidate(&errorAcct, now) {
		t.Fatal("error account must not be a patrol candidate")
	}

	paused := active
	paused.Schedulable = false
	if svc.isOpenAI429PatrolCandidate(&paused, now) {
		t.Fatal("manually paused account must not be a patrol candidate")
	}

	shadow := active
	parentID := int64(1)
	shadow.ParentAccountID = &parentID
	if svc.isOpenAI429PatrolCandidate(&shadow, now) {
		t.Fatal("shadow account must not be a patrol candidate")
	}

	expired := active
	expiredReset := now.Add(-time.Minute)
	expired.RateLimitResetAt = &expiredReset
	if svc.isOpenAI429PatrolCandidate(&expired, now) {
		t.Fatal("expired rate-limit window must not be a patrol candidate")
	}

	overloaded := active
	overloadedUntil := now.Add(2 * time.Minute)
	overloaded.OverloadUntil = &overloadedUntil
	if svc.isOpenAI429PatrolCandidate(&overloaded, now) {
		t.Fatal("overloaded account must not be a patrol candidate")
	}

	tempUnsched := active
	tempUntil := now.Add(2 * time.Minute)
	tempUnsched.TempUnschedulableUntil = &tempUntil
	if svc.isOpenAI429PatrolCandidate(&tempUnsched, now) {
		t.Fatal("temporarily unschedulable account must not be a patrol candidate")
	}

	// 429 冷却期内也仍视为候选（仅凭账号级 429 判定，不要求 auto-pause 排除）。
	quotaPausedAccount := active
	quotaPausedAccount.Extra = map[string]any{
		"codex_5h_used_percent":   float64(1),
		"auto_pause_5h_threshold": float64(0.9),
	}
	if !svc.isOpenAI429PatrolCandidate(&quotaPausedAccount, now) {
		t.Fatal("quota auto-pause must not exclude patrol candidate: external reset may not be reflected in local snapshot yet")
	}
}

func TestOpenAIGatewayPatrolRecoversOnly429BlockedAccounts(t *testing.T) {
	t.Parallel()

	now := time.Now()
	limitedAt := now.Add(-time.Minute)
	resetAt := now.Add(time.Hour)

	recoverable := openAIRateLimitPatrolProbeAccount(PlatformOpenAI, AccountTypeOAuth, StatusActive, true, &limitedAt, &resetAt)
	recoverable.ID = 1

	// error 与手动停用账号不应被探测恢复。
	errorAcct := openAIRateLimitPatrolProbeAccount(PlatformOpenAI, AccountTypeOAuth, StatusError, true, &limitedAt, &resetAt)
	errorAcct.ID = 2
	pausedAcct := openAIRateLimitPatrolProbeAccount(PlatformOpenAI, AccountTypeOAuth, StatusActive, false, &limitedAt, &resetAt)
	pausedAcct.ID = 3

	repo := &rateLimitPatrolRepo{}
	repo.accounts = []Account{recoverable, errorAcct, pausedAcct}
	upstream := &patrolUpstream{}
	svc := &OpenAIGatewayService{accountRepo: repo, httpUpstream: upstream}

	probed, recovered, err := svc.PatrolRecoverStaleOpenAIRateLimits(context.Background(), 0)
	if err != nil {
		t.Fatalf("PatrolRecoverStaleOpenAIRateLimits() error = %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1", recovered)
	}
	if probed != 1 {
		t.Fatalf("probed = %d, want 1 (error + paused accounts must not be probed)", probed)
	}
	if repo.clearedID != 1 {
		t.Fatalf("cleared account = %d, want 1", repo.clearedID)
	}
}

func TestOpenAIGatewayPatrolRespectsMaxAccountsPerCycle(t *testing.T) {
	t.Parallel()

	now := time.Now()
	limitedAt := now.Add(-time.Minute)
	resetAt := now.Add(time.Hour)

	repo := &rateLimitPatrolRepo{}
	var accounts []Account
	for i := int64(1); i <= 5; i++ {
		acct := openAIRateLimitPatrolProbeAccount(PlatformOpenAI, AccountTypeOAuth, StatusActive, true, &limitedAt, &resetAt)
		acct.ID = i
		accounts = append(accounts, acct)
	}
	repo.accounts = accounts
	upstream := &patrolUpstream{}
	svc := &OpenAIGatewayService{accountRepo: repo, httpUpstream: upstream}

	probed, recovered, err := svc.PatrolRecoverStaleOpenAIRateLimits(context.Background(), 2)
	if err != nil {
		t.Fatalf("PatrolRecoverStaleOpenAIRateLimits() error = %v", err)
	}
	if probed != 2 {
		t.Fatalf("probed = %d, want 2 (cycle cap)", probed)
	}
	if recovered != 2 {
		t.Fatalf("recovered = %d, want 2", recovered)
	}
}

// patrolGatewayStub 实现 openAIGatewayStaleRateLimitPatrol 窄接口，供巡查服务单测。
type patrolGatewayStub struct {
	mu        sync.Mutex
	calls     int
	probed    int
	recovered int
	err       error
}

func (g *patrolGatewayStub) PatrolRecoverStaleOpenAIRateLimits(context.Context, int) (int, int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls++
	return g.probed, g.recovered, g.err
}

func TestOpenAIStaleRateLimitPatrolServiceLifecycle(t *testing.T) {
	t.Parallel()

	gateway := &patrolGatewayStub{probed: 1, recovered: 1}
	cfg := &config.RateLimitPatrolConfig{Enabled: true, CheckIntervalMinutes: 30, MaxAccountsPerCycle: 20}
	svc := NewOpenAIStaleRateLimitPatrolService(cfg, gateway)
	svc.Start()

	// 等待首轮执行完成（patrolLoop 启动即跑一轮）。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		gateway.mu.Lock()
		calls := gateway.calls
		gateway.mu.Unlock()
		if calls >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	svc.Stop()

	gateway.mu.Lock()
	calls := gateway.calls
	gateway.mu.Unlock()
	if calls < 1 {
		t.Fatalf("patrol did not run at least one cycle, calls = %d", calls)
	}
}

func TestOpenAIStaleRateLimitPatrolServiceDisabled(t *testing.T) {
	t.Parallel()

	gateway := &patrolGatewayStub{}
	cfg := &config.RateLimitPatrolConfig{Enabled: false, CheckIntervalMinutes: 30, MaxAccountsPerCycle: 20}
	svc := NewOpenAIStaleRateLimitPatrolService(cfg, gateway)
	svc.Start()
	svc.Stop()

	gateway.mu.Lock()
	calls := gateway.calls
	gateway.mu.Unlock()
	if calls != 0 {
		t.Fatalf("disabled patrol must not call gateway, calls = %d", calls)
	}
}
