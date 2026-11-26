package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
)

type cpuServerCallbacks struct {
	get   func(*zap.Logger) (*api.VCPUCgroup, int, error)
	set   func(context.Context, *zap.Logger, api.VCPUChange) (int, error)
	ready func(*zap.Logger) bool
}

func listenForHTTPRequests(
	ctx context.Context,
	logger *zap.Logger,
	port int32,
	callbacks cpuServerCallbacks,
	wg *sync.WaitGroup,
	networkMonitoring bool,
) {
	defer wg.Done()
	mux := http.NewServeMux()
	loggerHandlers := logger.Named("http-handlers")
	cpuChangeLogger := loggerHandlers.Named("cpu_change")
	mux.HandleFunc("/cpu_change", func(w http.ResponseWriter, r *http.Request) {
		handleCPUChange(cpuChangeLogger, w, r, callbacks.set)
	})
	cpuCurrentLogger := loggerHandlers.Named("cpu_current")
	mux.HandleFunc("/cpu_current", func(w http.ResponseWriter, r *http.Request) {
		handleCPUCurrent(cpuCurrentLogger, w, r, callbacks.get)
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if callbacks.ready(logger) {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	if networkMonitoring {
		reg := prometheus.NewRegistry()
		metrics := NewMonitoringMetrics(reg)
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			metrics.update(logger)
			h := promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
			h.ServeHTTP(w, r)
		})
	}
	server := http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", port),
		Handler:           mux,
		ReadTimeout:       5 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      5 * time.Second,
	}
	errChan := make(chan error)
	go func() {
		errChan <- server.ListenAndServe()
	}()
	select {
	case err := <-errChan:
		if errors.Is(err, http.ErrServerClosed) {
			logger.Info("http server closed")
		} else if err != nil {
			logger.Fatal("http server exited with error", zap.Error(err))
		}
	case <-ctx.Done():
		err := server.Shutdown(context.Background())
		logger.Info("shut down http server", zap.Error(err))
	}
}

func handleCPUChange(
	logger *zap.Logger,
	w http.ResponseWriter,
	r *http.Request,
	set func(context.Context, *zap.Logger, api.VCPUChange) (int, error),
) {
	if r.Method != "POST" {
		logger.Error("unexpected method", zap.String("method", r.Method))
		w.WriteHeader(400)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 4097))
	if err != nil || len(body) > 4096 {
		logger.Error("could not read bounded CPU body", zap.Error(err))
		w.WriteHeader(400)
		return
	}

	var parsed api.VCPUChange
	if err = json.Unmarshal(body, &parsed); err != nil {
		logger.Error("could not parse body", zap.Error(err))
		w.WriteHeader(400)
		return
	}

	// update cgroup
	logger.Info("got CPU update", zap.Float64("CPU", parsed.VCPUs.AsFloat64()))
	status, err := set(r.Context(), logger, parsed)
	if err != nil {
		logger.Error("could not set cgroup limit", zap.Error(err))
		if status == 0 {
			status = http.StatusInternalServerError
		}
		w.WriteHeader(status)
		return
	}

	w.WriteHeader(200)
}

func handleCPUCurrent(
	logger *zap.Logger,
	w http.ResponseWriter,
	r *http.Request,
	get func(*zap.Logger) (*api.VCPUCgroup, int, error),
) {
	if r.Method != "GET" {
		logger.Error("unexpected method", zap.String("method", r.Method))
		w.WriteHeader(400)
		return
	}

	values, present := r.URL.Query()["observation_nonce"]
	if present && (len(values) != 1 || runnercpu.ValidateObservationNonce(values[0]) != nil) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	cpus, status, err := get(logger)
	if err != nil {
		logger.Error("could not get cgroup quota", zap.Error(err))
		if status == 0 {
			status = http.StatusInternalServerError
		}
		w.WriteHeader(status)
		return
	}
	nonce := ""
	if present {
		if len(values) != 1 || runnercpu.ValidateObservationNonce(values[0]) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		nonce = values[0]
		if cpus == nil || cpus.Managed == nil || cpus.Managed.ObservedBootTimeNS == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		// 不改原观察指针；仅在真实读取完成之后回显本次challenge。
		copyCPU := *cpus
		copyManaged := *cpus.Managed
		copyManaged.ObservationNonce = nonce
		copyCPU.Managed = &copyManaged
		cpus = &copyCPU
	}
	resp := cpus
	body, err := json.Marshal(resp)
	if err != nil {
		logger.Error("could not marshal body", zap.Error(err))
		w.WriteHeader(500)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.Write(body) //nolint:errcheck // Not much to do with the error here. TODO: log it?
}
