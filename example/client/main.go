package main

import (
	"context"
	"errors"
	"fmt"
	rnp "github.com/Autodoc-Technology/redis-nats-proxy"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		slog.Error("connect to nats", "err", err)
		return
	}
	defer nc.Close()

	// read first argument as the address of the Redis server
	// if no argument is provided, use the default address
	addr := "localhost:6379"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	slog.Info("redis address", "addr", addr)
	// read second argument as the password of the Redis server
	// if no argument is provided, use the default password
	password := ""
	if len(os.Args) > 2 {
		password = os.Args[2]
	}

	redisOptions := &redis.UniversalOptions{
		// to connect to a Redis cluster, provide more than one address or use the following hack []string{addr, addr}
		Addrs:    []string{addr},
		Password: password,
		// This is the IMPORTANT part of using the NATS proxy connection pool.
		// To prevent errors, set the pool size to 1
		PoolSize: 1,
		// To prevent Redis Cluster errors, also disable the following options
		//RouteRandomly:  false,
		//RouteByLatency: false,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			tcpAddr, err := net.ResolveTCPAddr(network, addr)
			if err != nil {
				return nil, err
			}
			slog.Debug("new connection", "addr", tcpAddr.String())
			var netConn net.Conn = rnp.NewNatsNetConn(nc, "proxy-redis", tcpAddr)
			// to log the connection, use the following line
			//netConn = rnp.NewDebugLogNetConn(netConn)
			return netConn, nil
		},
	}
	rc := redis.NewUniversalClient(redisOptions)
	defer func(rc redis.UniversalClient) {
		if err := rc.Close(); err != nil {
			slog.Error("close redis client", "err", err)
		}
	}(rc)

	now := time.Now()
	result, err := rc.Ping(ctx).Result()
	if err != nil {
		slog.Error("ping redis", "err", err)
		return
	}
	slog.Info("ping redis", "result", result, "duration", time.Since(now))

	doRequest := func(i int) {
		key := fmt.Sprintf("key-%d", i)
		s, err := rc.Get(ctx, key).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			slog.Error("get", "key", key, "err", err, "duration", time.Since(now))
			return
		}
		slog.Info("get", "key", key, "value", s, "duration", time.Since(now))
	}

	var wg sync.WaitGroup
	cnt := 10
	wg.Add(cnt)
	for i := 0; i < 10; i++ {
		i := i
		go func() {
			defer wg.Done()
			doRequest(i)
		}()
	}
	wg.Wait()
}
