//go:build linux

package resourceguard

import (
	"context"
	"testing"
)

// raw只用来达到cheap生命周期谓词，不能建立完整FS/proof或Source正控。
func stockMemoryObservationRawLifecycle(t *testing.T) *normalMemoryBrokerConsumerSource {
	t.Helper()
	g := &Guard{}
	system := &normalSystemLifecycleSource{}
	fs := &normalFilesystemOriginSource{g: g, system: system}
	proof := &normalSystemRoleBrokerExecProof{system: system}
	kernel := &normalKernelPrepV2BrokerConsumerSource{g: g}
	o := &normalMemoryBrokerConsumerSource{g: g, filesystem: fs, proof: proof, kernel: kernel, reference: normalMemoryOriginConfirmedReference{InitialWALFamily: normalInitialWALFamilyKernelPrepV2}}
	fs.memoryConsumer = o
	fs.kernelConsumer = kernel
	o.witness = normalMemoryBrokerConsumerWitness{Guard: g, Memory: o.reference}
	normalMemoryBrokerConsumerWitnesses.Store(o, o.witness)
	t.Cleanup(func() { normalMemoryBrokerConsumerWitnesses.Delete(o) })
	return o
}
func TestNormalMemoryBrokerStockObservationSourceLifecycle(t *testing.T) {
	for _, name := range []string{"raw-live-only-not-Source", "nil-context", "cancelled", "retired-witness", "witness-changed", "memory-changed", "filesystem-closed", "proof-closed", "kernel-closed", "filesystem-other-source", "filesystem-other-kernel", "filesystem-other-guard", "proof-other-system", "kernel-other-guard"} {
		t.Run(name, func(t *testing.T) {
			o := stockMemoryObservationRawLifecycle(t)
			ctx := context.Background()
			switch name {
			case "nil-context":
				ctx = nil
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "retired-witness":
				normalMemoryBrokerConsumerWitnesses.Delete(o)
			case "witness-changed":
				normalMemoryBrokerConsumerWitnesses.Store(o, normalMemoryBrokerConsumerWitness{})
			case "memory-changed":
				o.reference.ProtocolVersion++
			case "filesystem-closed":
				o.filesystem.mu.Lock()
				o.filesystem.closed = true
				o.filesystem.mu.Unlock()
			case "proof-closed":
				o.proof.mu.Lock()
				o.proof.closed = true
				o.proof.mu.Unlock()
			case "kernel-closed":
				o.kernel.mu.Lock()
				o.kernel.closed = true
				o.kernel.mu.Unlock()
			case "filesystem-other-source":
				o.filesystem.memoryConsumer = &normalMemoryBrokerConsumerSource{}
			case "filesystem-other-kernel":
				o.filesystem.kernelConsumer = &normalKernelPrepV2BrokerConsumerSource{}
			case "filesystem-other-guard":
				o.filesystem.g = &Guard{}
			case "proof-other-system":
				o.proof.system = &normalSystemLifecycleSource{}
			case "kernel-other-guard":
				o.kernel.g = &Guard{}
			}
			e := o.stockSourcesLive(ctx)
			if name == "raw-live-only-not-Source" {
				if e != nil {
					t.Fatal(e)
				}
			} else if e == nil {
				t.Fatal("closed/retired/rebound source accepted")
			}
		})
	}
}
func TestNormalMemoryBrokerStockObservationRawCannotProduceTuple(t *testing.T) {
	for _, name := range []string{"nil-source", "nil-context", "cancelled", "missing-state", "raw-no-witness"} {
		t.Run(name, func(t *testing.T) {
			o := stockMemoryObservationRawLifecycle(t)
			normalMemoryBrokerConsumerWitnesses.Delete(o)
			ctx := context.Background()
			state := &lockedState{}
			g := o.g
			switch name {
			case "nil-source":
				o = nil
			case "nil-context":
				ctx = nil
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "missing-state":
				state = nil
			}
			got, e := o.stockObservationLocked(ctx, state, g)
			if e == nil || got != (normalMemoryBrokerStockObservation{}) {
				t.Fatal("raw failure returned observation", e)
			}
		})
	}
}
