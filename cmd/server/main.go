package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/arleyar/market-price-pipeline/internal/binance"
	"github.com/arleyar/market-price-pipeline/internal/config"
	httpapi "github.com/arleyar/market-price-pipeline/internal/http"
	"github.com/arleyar/market-price-pipeline/internal/kafka"
	"github.com/arleyar/market-price-pipeline/internal/pipeline"
	"github.com/arleyar/market-price-pipeline/internal/storage"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "err", err)
		os.Exit(1)
	}

	log := newLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Per-dependency startup timeouts: the parent signal ctx has no deadline,
	// so without these a bad DNS / DSN could hang the boot indefinitely.
	const startupTimeout = 10 * time.Second

	pgCtx, pgCancel := context.WithTimeout(ctx, startupTimeout)
	pg, err := storage.NewPostgres(pgCtx, cfg.PostgresDSN)
	pgCancel()
	if err != nil {
		log.Error("postgres init failed", "err", err)
		os.Exit(1)
	}
	defer pg.Close()

	rd := storage.NewRedis(cfg.RedisAddr, cfg.LatestTTL)
	defer func() { _ = rd.Close() }()
	rdCtx, rdCancel := context.WithTimeout(ctx, startupTimeout)
	if err := rd.Ping(rdCtx); err != nil {
		log.Warn("redis ping at startup failed", "err", err)
	}
	rdCancel()

	kp := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer func() { _ = kp.Close() }()
	kpCtx, kpCancel := context.WithTimeout(ctx, startupTimeout)
	if err := kp.Ping(kpCtx); err != nil {
		log.Warn("kafka ping at startup failed", "err", err)
	}
	kpCancel()

	pipe := pipeline.New(pg, rd, kp, log)

	router := httpapi.NewRouter(pg, rd, kp, log)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	var wg sync.WaitGroup

	// HTTP server
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			stop()
		}
	}()

	// Binance WS loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		stream := binance.NewStream(cfg.BinanceWSURL, cfg.Symbols, log)
		err := stream.Run(ctx, pipe.HandleTick)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("binance stream stopped", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutdown initiated")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown error", "err", err)
	}

	// Bound wg.Wait so a stuck WS read can't outlive the SIGTERM grace period
	// and force the orchestrator to SIGKILL us.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Info("shutdown complete")
	case <-time.After(15 * time.Second):
		log.Warn("shutdown timeout, forcing exit")
	}
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
