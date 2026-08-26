package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

func TestOpenAIResponsesStreamCompleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want bool
	}{
		{name: "completed", body: "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.completed\"}\n\n", want: true},
		{name: "done", body: "data:{\"type\":\"response.done\"}\n", want: true},
		{name: "failed inside HTTP 200", body: "data: {\"type\":\"response.failed\"}\n\n", want: false},
		{name: "done marker without terminal event", body: "data: [DONE]\n\n", want: false},
		{name: "truncated stream", body: "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := openAIResponsesStreamCompleted(strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("openAIResponsesStreamCompleted() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("openAIResponsesStreamCompleted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOpenAIStaleRateLimitRecoveryCandidateRequiresOnlyRateLimitBlock(t *testing.T) {
	t.Parallel()

	now := time.Now()
	limitedAt := now.Add(-time.Minute)
	resetAt := now.Add(time.Hour)
	base := Account{
		ID:               1,
		Platform:         PlatformOpenAI,
		Type:             AccountTypeOAuth,
		Status:           StatusActive,
		Schedulable:      true,
		RateLimitedAt:    &limitedAt,
		RateLimitResetAt: &resetAt,
		Credentials: map[string]any{
			"model_mapping": map[string]any{"gpt-5.6-sol": "gpt-5.6-sol"},
		},
	}
	req := OpenAIAccountScheduleRequest{Platform: PlatformOpenAI, RequestedModel: "gpt-5.6-sol"}
	svc := &OpenAIGatewayService{}
	if !svc.isStaleOpenAIRateLimitRecoveryCandidate(context.Background(), &base, req, now) {
		t.Fatal("expected active OAuth account blocked only by rate limit to be a recovery candidate")
	}

	errorAccount := base
	errorAccount.Status = StatusError
	if svc.isStaleOpenAIRateLimitRecoveryCandidate(context.Background(), &errorAccount, req, now) {
		t.Fatal("error account must not be recovered automatically")
	}

	pausedAccount := base
	pausedAccount.Schedulable = false
	if svc.isStaleOpenAIRateLimitRecoveryCandidate(context.Background(), &pausedAccount, req, now) {
		t.Fatal("manually paused account must not be recovered automatically")
	}

	overloadedAccount := base
	overloadedUntil := now.Add(time.Minute)
	overloadedAccount.OverloadUntil = &overloadedUntil
	if svc.isStaleOpenAIRateLimitRecoveryCandidate(context.Background(), &overloadedAccount, req, now) {
		t.Fatal("overloaded account must not be probed by stale 429 recovery")
	}

	svc.BlockAccountScheduling(&base, now.Add(time.Hour), "429")
	if !svc.isStaleOpenAIRateLimitRecoveryCandidate(context.Background(), &base, req, now) {
		t.Fatal("429 runtime block must remain eligible for stale rate-limit recovery")
	}
	svc.BlockAccountScheduling(&base, now.Add(2*time.Hour), "oauth_401")
	if svc.isStaleOpenAIRateLimitRecoveryCandidate(context.Background(), &base, req, now) {
		t.Fatal("non-429 runtime block must not be probed")
	}

	unsupportedAccount := base
	unsupportedAccount.ID = 2
	unsupportedAccount.Credentials = map[string]any{"model_mapping": map[string]any{"gpt-5.5": "gpt-5.5"}}
	if svc.isStaleOpenAIRateLimitRecoveryCandidate(context.Background(), &unsupportedAccount, req, now) {
		t.Fatal("model-incompatible account must not be probed")
	}
}

type staleRecoveryRepo struct {
	stubOpenAIAccountRepo
	clearCalls int
	clearedID  int64
}

func (r *staleRecoveryRepo) ListModelAvailabilityCandidates(context.Context, *int64, []string, bool) ([]Account, error) {
	return append([]Account(nil), r.accounts...), nil
}

func (r *staleRecoveryRepo) UpdateExtra(context.Context, int64, map[string]any) error {
	return nil
}

func (r *staleRecoveryRepo) ClearRateLimitIfObserved(_ context.Context, id int64, limitedAt, resetAt time.Time) (bool, error) {
	r.clearCalls++
	r.clearedID = id
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
		return true, nil
	}
	return false, nil
}

type staleRecoveryUpstream struct {
	response *http.Response
	calls    int
}

func (u *staleRecoveryUpstream) Do(*http.Request, string, int64, int) (*http.Response, error) {
	return u.response, nil
}

func (u *staleRecoveryUpstream) DoWithTLS(*http.Request, string, int64, int, *tlsfingerprint.Profile) (*http.Response, error) {
	u.calls++
	return u.response, nil
}

func TestProbeAndRecoverOpenAIAccountRequiresCompletedStream(t *testing.T) {
	t.Parallel()

	limitedAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	resetAt := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	account := &Account{
		ID:               77,
		Platform:         PlatformOpenAI,
		Type:             AccountTypeOAuth,
		Status:           StatusActive,
		Schedulable:      true,
		Concurrency:      1,
		RateLimitedAt:    &limitedAt,
		RateLimitResetAt: &resetAt,
		Credentials: map[string]any{
			"access_token":       "test-token",
			"chatgpt_account_id": "test-account-id",
			"model_mapping":      map[string]any{"gpt-5.6-sol": "gpt-5.6-sol"},
		},
	}

	tests := []struct {
		name        string
		stream      string
		wantClear   int
		wantRecover bool
	}{
		{name: "stream failure", stream: "data: {\"type\":\"response.failed\"}\n\n", wantClear: 0, wantRecover: false},
		{name: "stream completed", stream: "data: {\"type\":\"response.completed\"}\n\n", wantClear: 1, wantRecover: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probeAccount := *account
			repo := &staleRecoveryRepo{stubOpenAIAccountRepo: stubOpenAIAccountRepo{accounts: []Account{probeAccount}}}
			upstream := &staleRecoveryUpstream{response: &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(tt.stream)),
			}}
			svc := &OpenAIGatewayService{accountRepo: repo, httpUpstream: upstream}
			recovered, err := svc.probeAndRecoverOpenAIAccount(context.Background(), &probeAccount, "gpt-5.6-sol")
			if err != nil {
				t.Fatalf("probeAndRecoverOpenAIAccount() error = %v", err)
			}
			if recovered != tt.wantRecover {
				t.Fatalf("recovered = %v, want %v", recovered, tt.wantRecover)
			}
			if repo.clearCalls != tt.wantClear {
				t.Fatalf("clear calls = %d, want %d", repo.clearCalls, tt.wantClear)
			}
		})
	}
}

func TestOpenAIAccountSchedulerRecoversStaleRateLimitBeforeReturningNoAccounts(t *testing.T) {
	t.Parallel()

	limitedAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	resetAt := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	repo := &staleRecoveryRepo{stubOpenAIAccountRepo: stubOpenAIAccountRepo{accounts: []Account{{
		ID:               91,
		Platform:         PlatformOpenAI,
		Type:             AccountTypeOAuth,
		Status:           StatusActive,
		Schedulable:      true,
		Concurrency:      1,
		RateLimitedAt:    &limitedAt,
		RateLimitResetAt: &resetAt,
		Credentials: map[string]any{
			"access_token":       "test-token",
			"chatgpt_account_id": "test-account-id",
			"model_mapping":      map[string]any{"gpt-5.6-sol": "gpt-5.6-sol"},
		},
	}}}}
	upstream := &staleRecoveryUpstream{response: &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\"}\n\n")),
	}}
	svc := &OpenAIGatewayService{accountRepo: repo, httpUpstream: upstream, cfg: &config.Config{}}
	selection, _, err := svc.SelectAccountWithSchedulerForCapability(
		context.Background(), nil, "", "", "gpt-5.6-sol", nil,
		OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions,
		false, false, false,
	)
	if err != nil {
		t.Fatalf("SelectAccountWithSchedulerForCapability() error = %v", err)
	}
	if selection == nil || selection.Account == nil || selection.Account.ID != 91 {
		t.Fatalf("unexpected selection: %#v", selection)
	}
	if upstream.calls != 1 {
		t.Fatalf("probe calls = %d, want 1", upstream.calls)
	}
	if repo.clearCalls != 1 {
		t.Fatalf("clear calls = %d, want 1", repo.clearCalls)
	}
}

func TestOpenAIStaleRateLimitRecoveryProbeCooldown(t *testing.T) {
	t.Parallel()

	svc := &OpenAIGatewayService{}
	now := time.Now()
	if !svc.claimOpenAIStaleRateLimitRecoveryProbe(42, now) {
		t.Fatal("first probe should be allowed")
	}
	if svc.claimOpenAIStaleRateLimitRecoveryProbe(42, now.Add(30*time.Second)) {
		t.Fatal("probe inside cooldown should be rejected")
	}
	if !svc.claimOpenAIStaleRateLimitRecoveryProbe(42, now.Add(openAIStaleRateLimitRecoveryCooldown)) {
		t.Fatal("probe at cooldown boundary should be allowed")
	}
}
