package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"subscription-reconciler/internal/api"
	"subscription-reconciler/internal/db"
	"subscription-reconciler/internal/metrics"
	"subscription-reconciler/internal/repository"
	"subscription-reconciler/internal/service"
	"subscription-reconciler/internal/worker"
)

func main() {
	databaseURL := env("DATABASE_URL", "postgres://reconciler:reconciler@localhost:5432/reconciler?sslmode=disable")
	addr := env("ADDR", ":8080")

	database, err := db.Connect(databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	if err := db.Migrate(database); err != nil {
		log.Fatal(err)
	}

	repo := repository.New(database)
	entitlements := service.NewEntitlementService(repo)
	metricsRegistry := metrics.NewRegistry()
	middleware := api.DefaultMiddlewareConfig()
	middleware.RateLimit = envInt("API_RATE_LIMIT_PER_MINUTE", middleware.RateLimit)
	handler := api.NewHandler(entitlements, repo, middleware, metricsRegistry)
	if err := handler.WarmSeenCache(context.Background()); err != nil {
		log.Printf("warn: could not warm seen-event cache: %v", err)
	}
	carrierPoller := worker.NewCarrierPoller(repo, entitlements, env("BASE_URL", "http://localhost"+addr), envInt("CARRIER_WORKER_POOL_SIZE", 8), metricsRegistry)
	notificationSender := worker.NewNotificationSender(repo, metricsRegistry)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go carrierPoller.Run(ctx, 5*time.Minute)
	go notificationSender.Run(ctx, 30*time.Second)

	server := &http.Server{
		Addr:              addr,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("listening on %s", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
