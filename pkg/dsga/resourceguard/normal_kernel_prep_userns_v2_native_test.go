//go:build linux && dsga_native_kernel_prep_v2

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

const kernelPrepV2ProbeC = `#define _GNU_SOURCE
#include <errno.h>
#include <grp.h>
#include <linux/capability.h>
#include <linux/sched.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <unistd.h>
static void fail(const char *s) { fprintf(stderr,"V2_USERNS_WORKER_FAIL %s errno=%d\n",s,errno);exit(90); }
static void child_done(long p) { int st=0;if(p==0){_exit(0);}if(p<0||waitpid((pid_t)p,&st,0)!=p||!WIFEXITED(st)||WEXITSTATUS(st)!=0){fail("dedicated child Wait");} }
static void caps_zero(void) {
 FILE *f=fopen("/proc/self/status","r");char line[256];int caps=0,nnp=0,sec=0;if(!f){fail("status");}
 while(fgets(line,sizeof(line),f)) {
  if(strncmp(line,"Cap",3)==0&&(strstr(line,"Inh:")||strstr(line,"Prm:")||strstr(line,"Eff:")||strstr(line,"Bnd:")||strstr(line,"Amb:"))){unsigned long long v=1;char *p=strchr(line,':');if(!p||sscanf(p+1,"%llx",&v)!=1||v!=0){fail("cap sets not zero");}caps++;}
  if(strncmp(line,"NoNewPrivs:",11)==0){int v=-1;if(sscanf(line+11,"%d",&v)!=1||v!=0){fail("NNP must remain0");}nnp++;}
  if(strncmp(line,"Seccomp:",8)==0){int v=-1;if(sscanf(line+8,"%d",&v)!=1||v!=0){fail("worker must be unfiltered");}sec++;}
 }
 fclose(f);if(caps!=5||nnp!=1||sec!=1){fail("status fields");}
}
int main(int argc,char **argv) {
 struct utsname u;struct __user_cap_header_struct h={_LINUX_CAPABILITY_VERSION_3,0};struct __user_cap_data_struct d[2]={{0}};
 if(argc!=2||uname(&u)||strcmp(u.release,"6.12.26-william-normal-split-metadata-v1-c67181104ec1")||getuid()!=0||geteuid()!=0||getpid()==1){fail("dedicated exact18c Guest only");}
 for(int i=0;i<=40;i++){if(prctl(PR_CAPBSET_DROP,i,0,0,0)){fail("drop bounding");}}
 if(prctl(PR_CAP_AMBIENT,PR_CAP_AMBIENT_CLEAR_ALL,0,0,0)||setgroups(0,NULL)||setresgid(1000,1000,1000)||setresuid(1000,1000,1000)||syscall(SYS_capset,&h,d)){fail("dedicated nonroot drop");}
 if(getuid()!=1000||geteuid()!=1000||getgid()!=1000||getegid()!=1000){fail("uid gid");}caps_zero();
 if(strcmp(argv[1],"before")==0){if(syscall(SYS_unshare,CLONE_NEWUSER)){fail("positive before zero");}puts("V2_USERNS_BEFORE_POSITIVE UID1000 fiveCaps0 NNP0 Seccomp0 actual unshare NEWUSER accepted");return 0;}
 if(strcmp(argv[1],"after")){fail("fixed worker mode");}
 errno=0;if(syscall(SYS_unshare,CLONE_NEWUSER)!=-1||errno!=ENOSPC){fail("unshare not denied by zero");}
 errno=0;long p=syscall(SYS_clone,(unsigned long)(CLONE_NEWUSER|SIGCHLD),0,0,0,0);if(p>=0){child_done(p);fail("clone NEWUSER unexpectedly accepted");}if(errno!=ENOSPC){fail("clone wrong denial");}
 struct clone_args a;memset(&a,0,sizeof(a));a.flags=CLONE_NEWUSER;a.exit_signal=SIGCHLD;errno=0;p=syscall(SYS_clone3,&a,sizeof(a));if(p>=0){child_done(p);fail("clone3 NEWUSER unexpectedly accepted");}if(errno!=ENOSPC){fail("clone3 wrong denial");}
 child_done(fork());caps_zero();puts("V2_USERNS_AFTER_NEGATIVE UID1000 fiveCaps0 NNP0 Seccomp0 unshare/clone/clone3 NEWUSER ENOSPC; plainfork actual Wait0");return 0;
}
`

func TestExportKernelPrepUserNSV2Fixture(t *testing.T) {
	out := os.Getenv("DSGA_KERNEL_PREP_V2_EXPORT")
	require.Equal(t, "/out/fixture", out)
	_, in := kernelPrepV2Inputs(t)
	require.NoError(t, os.MkdirAll(filepath.Join(out, "isofiles"), 0755))
	for name, raw := range map[string][]byte{"dsga-resource.json": in.RuntimeRaw, "dsga-physical-kernel.json": in.PhysicalPolicyRaw, "dsga-kernel-bootstrap.json": in.PolicyRaw} {
		require.NoError(t, os.WriteFile(filepath.Join(out, "isofiles", name), raw, 0444))
	}
	require.NoError(t, os.WriteFile(filepath.Join(out, "userns-probe.c"), []byte(kernelPrepV2ProbeC), 0444))
}

type nativeKernelPrepV2ZeroFault struct {
	*normalKernelPrepV2LinuxBackend
	injected bool
}

func (b *nativeKernelPrepV2ZeroFault) kernelPrepV2ZeroUserNamespaces(w normalKernelPrepV2Object) error {
	if e := b.normalKernelPrepV2LinuxBackend.kernelPrepV2ZeroUserNamespaces(w); e != nil {
		return e
	}
	if !b.injected {
		b.injected = true
		return errors.New("DEV isolated error after real fixed-zero syscall")
	}
	return nil
}
func nativeKernelPrepV2Worker(t *testing.T, mode string) {
	t.Helper()
	cmd := exec.Command("/userns-probe", mode)
	cmd.Env = []string{}
	output, e := cmd.CombinedOutput()
	t.Logf("dedicated C worker %s: %s", mode, output)
	require.NoError(t, e)
	if mode == "before" {
		require.Contains(t, string(output), "V2_USERNS_BEFORE_POSITIVE")
	} else {
		require.Contains(t, string(output), "V2_USERNS_AFTER_NEGATIVE")
	}
}
func TestNativeKernelPrepUserNSV2ForeignRoot(t *testing.T) {
	require.Equal(t, "isolated18c-userns-v2-foreign", os.Getenv("DSGA_KERNEL_PREP_V2_FOREIGN"))
	s, _, e := runtimeSource()
	require.NoError(t, e)
	g, e := Open(s.Config)
	require.NoError(t, e)
	_, e = g.ReplayKernelBeforeOriginV2(context.Background())
	require.Error(t, e)
	fmt.Println("V2_USERNS_FOREIGN_PRODUCER_REFUSED")
}

// 只有新的无PG隔离18c VM执行；正例worker在设置前已真实Wait退出。
func TestNativeKernelPrepUserNSV2Component(t *testing.T) {
	require.Equal(t, "isolated18c-userns-v2", os.Getenv("DSGA_KERNEL_PREP_V2_NATIVE"))
	require.Equal(t, 1, os.Getppid())
	mode := os.Getenv("DSGA_KERNEL_PREP_V2_MODE")
	require.Contains(t, []string{"positive", "partial-replay"}, mode)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	s, _, e := runtimeSource()
	require.NoError(t, e)
	g, e := Open(s.Config)
	require.NoError(t, e)
	b, e := newNormalKernelPrepV2LinuxBackend(ctx, g.kernel.(*linuxKernel))
	require.NoError(t, e)
	before, e := b.kernelPrepV2Read()
	require.NoError(t, e)
	require.Positive(t, uint64(before.MaxUserNamespaces))
	nativeKernelPrepV2Worker(t, "before")
	var first NormalV2KernelBootstrapEvidence
	if mode == "partial-replay" {
		in, e := g.normalKernelPrepV2Inputs()
		require.NoError(t, e)
		state, e := lockState(ctx, g.config.StateDir)
		require.NoError(t, e)
		_, e = g.normalKernelPreparationV2Locked(ctx, in, kernelPrepPrepare, state, &nativeKernelPrepV2ZeroFault{normalKernelPrepV2LinuxBackend: b})
		state.close()
		require.ErrorIs(t, e, ErrUnknown)
		raw, e := os.ReadFile(filepath.Join(g.config.StateDir, normalKernelPrepV2IntentName))
		require.NoError(t, e)
		for _, call := range []func(context.Context) (NormalV2KernelBootstrapEvidence, error){g.PrepareKernelBeforeOriginV2, g.ObserveKernelBeforeOriginV2} {
			_, e = call(ctx)
			require.ErrorIs(t, e, ErrUnknown)
		}
		first, e = g.ReplayKernelBeforeOriginV2(ctx)
		require.NoError(t, e)
		after, e := os.ReadFile(filepath.Join(g.config.StateDir, normalKernelPrepV2IntentName))
		require.NoError(t, e)
		require.Equal(t, raw, after)
	} else {
		b.close()
		first, e = g.PrepareKernelBeforeOriginV2(ctx)
		require.NoError(t, e)
	}
	require.True(t, first.DurabilityConfirmed())
	require.Zero(t, first.After.MaxUserNamespaces)
	require.False(t, first.AllSourcesClosed)
	require.False(t, first.VMShrinkPermit)
	source, e := first.originalUserNamespaceBarrierSource()
	require.NoError(t, e)
	require.ErrorIs(t, source.requireContinuousQualification(ctx), ErrUnknown)
	nativeKernelPrepV2Worker(t, "after")
	observed, e := g.ObserveKernelBeforeOriginV2(ctx)
	require.NoError(t, e)
	require.False(t, observed.DurabilityConfirmed())
	require.Nil(t, observed.source)
	again, e := g.PrepareKernelBeforeOriginV2(ctx)
	require.NoError(t, e)
	require.False(t, again.DurabilityConfirmed())
	replayed, e := g.ReplayKernelBeforeOriginV2(ctx)
	require.NoError(t, e)
	require.True(t, replayed.DurabilityConfirmed())
	require.Equal(t, first.Nonce, replayed.Nonce)
	// 真实Go新M正控，无更改调度/内存参数；namespace禁创建不应破坏普通线程。
	release := make(chan struct{})
	ids := make(chan int, 4)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			ids <- unix.Gettid()
			<-release
		}()
	}
	seen := map[int]bool{}
	for i := 0; i < 4; i++ {
		seen[<-ids] = true
	}
	close(release)
	wg.Wait()
	require.Len(t, seen, 4)
	original, e := os.ReadFile(filepath.Join(g.config.StateDir, normalKernelPrepV2CompleteName))
	require.NoError(t, e)
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeKernelPrepUserNSV2ForeignRoot$", "-test.v")
	cmd.Env = append(os.Environ(), "DSGA_KERNEL_PREP_V2_FOREIGN=isolated18c-userns-v2-foreign")
	output, e := cmd.CombinedOutput()
	require.NoError(t, e, string(output))
	require.Contains(t, string(output), "V2_USERNS_FOREIGN_PRODUCER_REFUSED")
	same, e := os.ReadFile(filepath.Join(g.config.StateDir, normalKernelPrepV2CompleteName))
	require.NoError(t, e)
	require.Equal(t, original, same)
	for _, name := range []string{kernelPrepIntentName, kernelPrepCompleteName, "state.json", physicalOriginDataName} {
		require.NoFileExists(t, filepath.Join(g.config.StateDir, name))
	}
	result, e := json.Marshal(struct {
		Mode         string                          `json:"mode"`
		Before       normalKernelPrepV2Snapshot      `json:"before"`
		Evidence     NormalV2KernelBootstrapEvidence `json:"evidence"`
		GoThreads    int                             `json:"go_threads"`
		PG           bool                            `json:"PG_started"`
		SourceClosed bool                            `json:"all_sources_closed"`
	}{mode, before, replayed, len(seen), false, false})
	require.NoError(t, e)
	fmt.Printf("V2_USERNS_COMPONENT=%s\n", result)
}
