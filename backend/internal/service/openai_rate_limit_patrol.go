package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

const openAIRateLimitPatrolCycleTimeout = 10 * time.Minute

// OpenAIStaleRateLimitPatrolService 后台巡查「仅被 429 限流挡住」的 OpenAI OAuth
// 账号：周期性探测一次上游，若外部（例如 ChatGPT 客户端）重置了额度、上游已能
// 正常完成响应，就按既有 CAS 语义清除该账号的旧限流并同步调度缓存。
//
// 与请求路径的 stale-429 恢复（tryRecoverStaleOpenAIRateLimit）复用同一套
// 候选判定 / 探测 / CAS 清除逻辑，但它是时间驱动的：不依赖页面或业务请求，
// 因此即使账号因限流长期不被调度、也没有管理页面在刷新，外部重置额度后也会在
// 一个巡查周期内被发现并恢复。
type OpenAIStaleRateLimitPatrolService struct {
	cfg           *config.RateLimitPatrolConfig
	gatewayPatrol openAIGatewayStaleRateLimitPatrol

	stopCh    chan struct{}
	stopOnce  sync.Once
	runCtx    context.Context
	runCancel context.CancelFunc
	wg        sync.WaitGroup
}

// openAIGatewayStaleRateLimitPatrol 是巡查调用 gateway 探测/恢复能力的窄接口，
// 避免后台服务直接持有庞大的 OpenAIGatewayService 具体类型。
type openAIGatewayStaleRateLimitPatrol interface {
	PatrolRecoverStaleOpenAIRateLimits(ctx context.Context, maxAccounts int) (probed int, recovered int, err error)
}

// NewOpenAIStaleRateLimitPatrolService 创建巡查服务。cfg 为 nil 时使用默认值
// （启用、30 分钟、每轮 20 个账号）。非 nil 配置按原样使用（config 层已提供
// viper 默认值，enabled=false 表示显式禁用）。
func NewOpenAIStaleRateLimitPatrolService(
	cfg *config.RateLimitPatrolConfig,
	gatewayPatrol openAIGatewayStaleRateLimitPatrol,
) *OpenAIStaleRateLimitPatrolService {
	runCtx, runCancel := context.WithCancel(context.Background())
	if cfg == nil {
		cfg = &config.RateLimitPatrolConfig{
			Enabled:              true,
			CheckIntervalMinutes: 30,
			MaxAccountsPerCycle:  20,
		}
	}
	if cfg.CheckIntervalMinutes <= 0 {
		cfg.CheckIntervalMinutes = 30
	}
	if cfg.MaxAccountsPerCycle <= 0 {
		cfg.MaxAccountsPerCycle = 20
	}
	return &OpenAIStaleRateLimitPatrolService{
		cfg:           cfg,
		gatewayPatrol: gatewayPatrol,
		stopCh:        make(chan struct{}),
		runCtx:        runCtx,
		runCancel:     runCancel,
	}
}

// Start 启动后台巡查。
func (s *OpenAIStaleRateLimitPatrolService) Start() {
	if s == nil || s.cfg == nil || !s.cfg.Enabled || s.gatewayPatrol == nil {
		slog.Info("openai_rate_limit_patrol.service_disabled")
		return
	}
	s.wg.Add(1)
	go s.patrolLoop()
	slog.Info("openai_rate_limit_patrol.service_started",
		"check_interval_minutes", s.cfg.CheckIntervalMinutes,
		"max_accounts_per_cycle", s.cfg.MaxAccountsPerCycle,
	)
}

// Stop 停止巡查（可安全多次调用）。
func (s *OpenAIStaleRateLimitPatrolService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		if s.runCancel != nil {
			s.runCancel()
		}
		close(s.stopCh)
	})
	s.wg.Wait()
	slog.Info("openai_rate_limit_patrol.service_stopped")
}

// patrolLoop 巡查主循环。
func (s *OpenAIStaleRateLimitPatrolService) patrolLoop() {
	defer s.wg.Done()
	ctx := s.runCtx
	if ctx == nil {
		ctx = context.Background()
	}

	interval := time.Duration(s.cfg.CheckIntervalMinutes) * time.Minute
	if interval < time.Minute {
		interval = 30 * time.Minute
	}
	maxAccounts := s.cfg.MaxAccountsPerCycle
	if maxAccounts <= 0 {
		maxAccounts = 20
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.runPatrolCycle(ctx, maxAccounts)

	for {
		select {
		case <-ticker.C:
			s.runPatrolCycle(ctx, maxAccounts)
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		}
	}
}

func (s *OpenAIStaleRateLimitPatrolService) runPatrolCycle(ctx context.Context, maxAccounts int) {
	if ctx.Err() != nil || s.gatewayPatrol == nil {
		return
	}
	cycleCtx, cancel := context.WithTimeout(ctx, openAIRateLimitPatrolCycleTimeout)
	defer cancel()
	probed, recovered, err := s.gatewayPatrol.PatrolRecoverStaleOpenAIRateLimits(cycleCtx, maxAccounts)
	if err != nil {
		if cycleCtx.Err() == nil {
			slog.Warn("openai_rate_limit_patrol.cycle_failed", "error", err)
		}
		return
	}
	slog.Info("openai_rate_limit_patrol.cycle_completed", "probed", probed, "recovered", recovered)
}
