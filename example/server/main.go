package main

import (
	"context"
	rnp "github.com/Autodoc-Technology/redis-nats-proxy"
	"github.com/nats-io/nats.go"
	"log/slog"
	"net"
	"os"
	"os/signal"
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

	// Create a pull manager with a custom dial function that logs the connection
	var dialFunc rnp.DialFn = func(network, addr string) (net.Conn, error) {
		conn, err := rnp.DefaultDial(network, addr)
		if err != nil {
			return nil, err
		}
		return rnp.NewDebugLogNetConn(conn), nil
	}
	pm := rnp.NewNetConnPullManager(dialFunc)
	// Simplest way to create a pull manager
	//pm := rnp.NewNetConnPullManager(rnp.DefaultDial)
	defer func(pm *rnp.NetConnPullManager) {
		err := pm.Close()
		if err != nil {
			slog.Error("close pull manager", "err", err)
		}
	}(pm)
	// Create a proxy with the custom pull manager
	proxy := rnp.NewNatsConnProxy(nc, "netconn", pm)
	// Create a proxy with the default pull manager
	//proxy := redis_nats_proxy.NewNatsConnProxyWithDefaultConnManager(nc, "netconn")
	go func() {
		err := proxy.Start(ctx)
		if err != nil {
			slog.Error("start proxy", "err", err)
		}
	}()

	<-ctx.Done()
}
