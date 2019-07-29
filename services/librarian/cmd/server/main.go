package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-chi/chi"
	"github.com/pkg/errors"
	"github.com/shardhub/shards/services/librarian"
	v1 "github.com/shardhub/shards/services/librarian/api/v1"
	"github.com/shardhub/shards/services/librarian/databases/postgres"

	_ "github.com/lib/pq"
)

func main() {
	// Main context
	g, ctx := errgroup.WithContext(context.Background())

	// Signals
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)

	// Create logger
	logger, err := zap.NewProduction() // TODO
	if err != nil {
		panic(err)
	}
	defer logger.Sync() // nolint:errcheck

	// Create librarian
	l := librarian.New()

	// TODO: Replace with real config
	pg := postgres.New(
		postgres.WithHost("localhost"),
		postgres.WithPort(5432),
		postgres.WithUsername("postgres"),
		postgres.WithPassword(""),
		postgres.WithSoftDelete(),
	)
	logger.Info("Register postgres")
	if err := l.Register("postgres", pg); err != nil {
		logger.Fatal("Cannot register postgres", zap.Error(err))
	}

	// Create API
	api := v1.New(l, logger)

	// Create router
	r := chi.NewRouter()
	r.Mount("/api/v1", api)

	// Create server
	srv := &http.Server{
		Addr:         "localhost" + ":" + strconv.Itoa(8080),
		Handler:      r,
		ReadTimeout:  time.Second,
		WriteTimeout: 10 * time.Second,
	}

	g.Go(func() error {
		logger.Info("Connect to postgres")
		if err := pg.Connect(ctx); err != nil {
			logger.Error("Cannot connect to postgres", zap.Error(err))
			return errors.Wrap(err, "cannot connect to postgres")
		}
		logger.Info("Connected to postgres")

		logger.Info("Init postgres")
		if err := pg.Init(ctx); err != nil {
			logger.Error("Cannot init postgres", zap.Error(err))
			return errors.Wrap(err, "cannot init postgres")
		}
		logger.Info("Postgres was inited")

		return nil
	})

	g.Go(func() error {
		<-ctx.Done()

		logger.Info("Disconnect from postgres")
		if err := pg.Disconnect(); err != nil {
			logger.Error("Cannot disconnect from postgres", zap.Error(err))
			return errors.Wrap(err, "cannot disconnect from postgres")
		}
		logger.Info("Disconnected from postgres")

		return nil
	})

	g.Go(func() error {
		logger.Info("Start server", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Server was stopped with error", zap.Error(err))
			return errors.Wrap(err, "server was stopped with error")
		}
		logger.Info("Server was stopped")

		return nil
	})

	g.Go(func() error {
		<-ctx.Done()

		logger.Info("Close server")
		if err := srv.Close(); err != nil {
			logger.Error("Cannot close server", zap.Error(err))
			return errors.Wrap(err, "cannot close server")
		}
		logger.Info("Server was closed")

		return nil
	})

	// Handle signal chan
	g.Go(func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-sigch:
			return context.Canceled
		}
	})

	if err := g.Wait(); err != nil && err != context.Canceled {
		logger.Fatal("Unhandled error", zap.Error(err))
	}
}
