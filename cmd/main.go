package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/umohsamuel/transactional-outbox/internal/adapters/db/repository"
	httpserver "github.com/umohsamuel/transactional-outbox/internal/adapters/transport/http"
	"github.com/umohsamuel/transactional-outbox/internal/services"
	"github.com/umohsamuel/transactional-outbox/pkg/env"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := env.LoadEnv()
	if err != nil {
		return err
	}

	db, err := openDB(ctx, cfg.DB)
	if err != nil {
		return err
	}
	defer db.Close()

	repos := repository.New(db)
	svcs := services.New(repos.Order)
	server := httpserver.NewHttpServer(svcs)

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.Port),
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	log.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func openDB(ctx context.Context, e env.PostgresEnv) (*sql.DB, error) {
	q := url.Values{}
	if e.DB_SSLMODE != "" {
		q.Set("sslmode", e.DB_SSLMODE)
	}
	if e.DB_SCHEMA != "" {
		q.Set("search_path", e.DB_SCHEMA)
	}

	dsn := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(e.DB_USER, e.DB_PASSWORD),
		Host:     fmt.Sprintf("%s:%d", e.DB_HOST, e.DB_PORT),
		Path:     e.DB_DATABASE,
		RawQuery: q.Encode(),
	}

	db, err := sql.Open("pgx", dsn.String())
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}

	return db, nil
}
