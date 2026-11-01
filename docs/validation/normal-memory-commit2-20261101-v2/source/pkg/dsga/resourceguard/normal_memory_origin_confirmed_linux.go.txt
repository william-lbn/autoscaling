//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// 仅原R1、本进程、同一Guard的实际对象。不是WAL缓存/JSON反序列化入口；
// 重建Guard或新Root缺该指针即Unknown。对象不持OS FD，原state/Root见证仍逐次重核。
var normalMemoryOriginalConfirmedSources sync.Map

func (g *Guard) normalMemoryInstallOriginalConfirmedSourceLocked(ctx context.Context, s *lockedState, o *normalMemoryOriginSource) error {
	if g == nil || ctx == nil || ctx.Err() != nil || s == nil || s.dir == nil || s.lock == nil || o == nil || o.commit2 == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("original confirmed memory2 pointer absent"))
	}
	if e := o.recheckLocked(ctx, s, g); e != nil {
		return e
	}
	if _, loaded := normalMemoryOriginalConfirmedSources.LoadOrStore(g, o); loaded {
		return errors.Join(ErrUnknown, fmt.Errorf("Guard already accepted original memory source; replacement/repeated admission forbidden"))
	}
	return nil
}

// Root FS只能在同原resource flock中取此原指针；不sync、不按当前计数或JSON补签。
func (g *Guard) normalMemoryOriginalConfirmedSourceLocked(ctx context.Context, s *lockedState) (*normalMemoryOriginSource, error) {
	if g == nil || ctx == nil || ctx.Err() != nil || s == nil || s.dir == nil || s.lock == nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("same original Guard/resource lock required"))
	}
	value, ok := normalMemoryOriginalConfirmedSources.Load(g)
	if !ok {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original R1 confirmed memory source missing in this Guard"))
	}
	o, ok := value.(*normalMemoryOriginSource)
	if !ok || o == nil || o.commit2 == nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("invalid private confirmed memory binding"))
	}
	if e := o.recheckLocked(ctx, s, g); e != nil {
		return nil, e
	}
	return o, nil
}

// initial-boot的统一清理：后继失败或正常把原引用交给broker后，R1不留下可再用
// 的Guard槽。只清本进程指针，不动原11WAL/原预算；失败绝不制造ready。
func (g *Guard) normalMemoryDiscardOriginalConfirmedSource() {
	if g != nil {
		normalMemoryOriginalConfirmedSources.Delete(g)
	}
}
