package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"go.uber.org/zap"
)

var (
	isDev  bool
	logger *zap.Logger
	replay *replayService
)

// initLogger initializes the zap logger based on the mode (dev or prod)
func initLogger(dev bool) (*zap.Logger, error) {
	var l *zap.Logger
	var err error

	if dev {
		l, err = zap.NewDevelopment()
		if err != nil {
			return nil, err
		}
		l.Info("initialized development logger")
	} else {
		l, err = zap.NewProduction()
		if err != nil {
			return nil, err
		}
		l.Info("initialized production logger")
	}

	return l, nil
}

func main() {
	dev := flag.Bool("dev", false, "enable dev mode")
	defaultPort := envInt("PORT", 8080)
	port := flag.Int("port", defaultPort, "port to listen on")
	host := flag.String("host", envOrDefault("HOST", ""), "host/IP to bind (empty means all interfaces)")
	webDir := flag.String("web-dir", envOrDefault("WEB_DIST_DIR", "../web/dist"), "path to web dist directory")
	parseWorkers := flag.Int("parse-workers", envInt("PARSE_WORKERS", 1), "number of concurrent demo parse workers")
	flag.Parse()
	isDev = *dev

	// Initialize logger
	var err error
	logger, err = initLogger(isDev)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
	defer logger.Sync()

	replay, err = newReplayService()
	if err != nil {
		logger.Fatal("failed to initialize replay service", zap.Error(err))
	}
	replay.startWorkers(*parseWorkers)

	http.HandleFunc("/download", downloadHandler)
	http.HandleFunc("/webhooks/faceit/demo-ready", replay.webhookDemoReadyHandler)
	http.HandleFunc("/replays/", replay.replaysHandler)
	http.HandleFunc("/admin/replays/reprocess", replay.adminReprocessHandler)
	http.HandleFunc("/admin/replays/reprocess-failed", replay.adminReprocessFailedHandler)
	http.HandleFunc("/admin/replays/delete", replay.adminDeleteReplayHandler)
	http.Handle("/", spaHandler(*webDir))

	if *dev {
		http.Handle("/testdemos/", http.StripPrefix("/testdemos/", http.FileServer(http.Dir("./testdemos"))))
	}

	listenAddr := fmt.Sprintf(":%d", *port)
	if *host != "" {
		listenAddr = fmt.Sprintf("%s:%d", *host, *port)
	}

	logger.Info("starting server",
		zap.String("mode", map[bool]string{true: "dev", false: "prod"}[isDev]),
		zap.Int("port", *port),
		zap.String("host", *host),
		zap.String("listen_addr", listenAddr),
		zap.String("web_dir", *webDir))
	if err := http.ListenAndServe(listenAddr, nil); err != nil {
		logger.Fatal("server failed to start", zap.Error(err))
	}
}

func envOrDefault(name string, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func envInt(name string, fallback int) int {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return parsed
}
