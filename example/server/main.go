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
	// To create a pool with the default Dial function use the following argument rnp.DefaultDial
	//pm := rnp.NewNetConnPullManager(rnp.DefaultDial)
	// Create a proxy with the custom pull manager
	proxy := rnp.NewNatsConnProxy(nc, "proxy-redis", pm)
	go func() {
		if err := proxy.Start(ctx); err != nil {
			slog.Error("start proxy", "err", err)
		}
	}()

	<-ctx.Done()
}
