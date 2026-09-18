//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type disconnectObservableBody struct {
	*io.PipeReader
	started chan struct{}
	closed  chan struct{}
	start   sync.Once
	close   sync.Once
}

func (b *disconnectObservableBody) Read(p []byte) (int, error) {
	b.start.Do(func() { close(b.started) })
	return b.PipeReader.Read(p)
}

func (b *disconnectObservableBody) Close() error {
	b.close.Do(func() { close(b.closed) })
	return b.PipeReader.Close()
}

func newDisconnectObservableBody() (*disconnectObservableBody, *io.PipeWriter) {
	reader, writer := io.Pipe()
	return &disconnectObservableBody{
		PipeReader: reader,
		started:    make(chan struct{}),
		closed:     make(chan struct{}),
	}, writer
}

func newDisconnectTestContext(t *testing.T) (context.Context, context.CancelFunc, *gin.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/test", nil).WithContext(ctx)
	return ctx, cancel, c
}

func assertDisconnectClosesBody(t *testing.T, body *disconnectObservableBody, run func() error) {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- run() }()

	select {
	case <-body.started:
	case <-time.After(time.Second):
		t.Fatal("handler did not begin reading upstream body")
	}

	// The caller owns cancellation; the body close must be what releases the blocked read.
	// The individual test cancels its request context before reaching this assertion.
	select {
	case <-body.closed:
	case <-time.After(time.Second):
		t.Fatal("handler did not close upstream body after client cancellation")
	}

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("handler did not return after client cancellation")
	}
}

func TestGatewayDisconnectClosesUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("Messages/no-keepalive", func(t *testing.T) {
		ctx, cancel, c := newDisconnectTestContext(t)
		defer cancel()
		body, writer := newDisconnectObservableBody()
		defer writer.Close()
		account := &Account{ID: 1, Name: "disconnect-test", Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
		resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}
		svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: nil}

		result := make(chan error, 1)
		go func() {
			_, err := svc.handleAnthropicStreamingResponse(resp, c, account, "gpt-test", "gpt-test", "gpt-test", time.Now())
			result <- err
		}()
		select {
		case <-body.started:
		case <-time.After(time.Second):
			t.Fatal("handler did not begin reading upstream body")
		}
		cancel()
		select {
		case <-body.closed:
		case <-time.After(time.Second):
			t.Fatal("messages handler did not close upstream body")
		}
		select {
		case err := <-result:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("messages handler did not return")
		}
		_ = ctx
	})

	t.Run("Messages/keepalive", func(t *testing.T) {
		ctx, cancel, c := newDisconnectTestContext(t)
		defer cancel()
		body, writer := newDisconnectObservableBody()
		defer writer.Close()
		account := &Account{ID: 1, Name: "disconnect-test", Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
		resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}
		svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{StreamKeepaliveInterval: 1}}}

		result := make(chan error, 1)
		go func() {
			_, err := svc.handleAnthropicStreamingResponse(resp, c, account, "gpt-test", "gpt-test", "gpt-test", time.Now())
			result <- err
		}()
		select {
		case <-body.started:
		case <-time.After(time.Second):
			t.Fatal("handler did not begin reading upstream body")
		}
		cancel()
		select {
		case <-body.closed:
		case <-time.After(time.Second):
			t.Fatal("messages keepalive handler did not close upstream body")
		}
		select {
		case err := <-result:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("messages keepalive handler did not return")
		}
		_ = ctx
	})

	t.Run("Responses/no-keepalive", func(t *testing.T) {
		ctx, cancel, c := newDisconnectTestContext(t)
		defer cancel()
		body, writer := newDisconnectObservableBody()
		defer writer.Close()
		account := &Account{ID: 1, Name: "disconnect-test", Platform: PlatformAnthropic, Type: AccountTypeOAuth}
		resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}
		svc := &GatewayService{cfg: &config.Config{}, rateLimitService: &RateLimitService{}}
		result := make(chan error, 1)
		go func() {
			_, err := svc.handleStreamingResponse(ctx, resp, c, account, time.Now(), "claude-test", "claude-test", false)
			result <- err
		}()
		select {
		case <-body.started:
		case <-time.After(time.Second):
			t.Fatal("responses handler did not begin reading upstream body")
		}
		cancel()
		select {
		case <-body.closed:
		case <-time.After(time.Second):
			t.Fatal("responses handler did not close upstream body")
		}
		select {
		case err := <-result:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("responses handler did not return")
		}
	})

	t.Run("Responses/keepalive", func(t *testing.T) {
		ctx, cancel, c := newDisconnectTestContext(t)
		defer cancel()
		body, writer := newDisconnectObservableBody()
		defer writer.Close()
		account := &Account{ID: 1, Name: "disconnect-test", Platform: PlatformAnthropic, Type: AccountTypeOAuth}
		resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}
		svc := &GatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{StreamKeepaliveInterval: 1}}, rateLimitService: &RateLimitService{}}
		result := make(chan error, 1)
		go func() {
			_, err := svc.handleStreamingResponse(ctx, resp, c, account, time.Now(), "claude-test", "claude-test", false)
			result <- err
		}()
		select {
		case <-body.started:
		case <-time.After(time.Second):
			t.Fatal("responses keepalive handler did not begin reading upstream body")
		}
		cancel()
		select {
		case <-body.closed:
		case <-time.After(time.Second):
			t.Fatal("responses keepalive handler did not close upstream body")
		}
		select {
		case err := <-result:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("responses keepalive handler did not return")
		}
	})

	t.Run("Raw", func(t *testing.T) {
		_, cancel, c := newDisconnectTestContext(t)

		defer cancel()
		body, writer := newDisconnectObservableBody()
		defer writer.Close()
		account := rawChatCompletionsTestAccount()
		resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}
		svc := &OpenAIGatewayService{cfg: &config.Config{}}
		result := make(chan error, 1)
		go func() {
			_, err := svc.streamRawChatCompletions(c, resp, account, "gpt-test", "gpt-test", "gpt-test", nil, nil, time.Now(), 1)
			result <- err
		}()
		select {
		case <-body.started:
		case <-time.After(time.Second):
			t.Fatal("raw handler did not begin reading upstream body")
		}
		cancel()
		select {
		case <-body.closed:
		case <-time.After(time.Second):
			t.Fatal("raw handler did not close upstream body")
		}
		select {
		case err := <-result:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("raw handler did not return")
		}
	})
}
