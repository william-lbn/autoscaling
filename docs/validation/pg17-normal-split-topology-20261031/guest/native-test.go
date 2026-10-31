//go:build linux
package resourceguard
import (
 "context"
 "encoding/json"
 "fmt"
 "os"
 "path/filepath"
 "strings"
 "syscall"
 "testing"
 control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)
// Validation-only extra build context, never linked into production resourceguard.
// Source/ISO policies below are synthetic fixture identities; kernel/cgroup/WAL are real.
func TestNormalSplitTopologyActualGuestKernel(t *testing.T){
 if os.Getenv("PG17_NATIVE_SPLIT_TOPOLOGY")!="isolated-369-kvm-component-v1"{t.Fatal("dedicated exact Guest environment required; no skip")}
 read:=func(p string)string{t.Helper();b,e:=os.ReadFile(p);if e!=nil{t.Fatal(e)};return strings.TrimSpace(string(b))}
 if !strings.Contains(" "+read("/proc/cmdline")+" "," pg17_split_topology_component=1 "){t.Fatal("dedicated kernel marker missing")}
 ref,e:=LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID);if e!=nil{t.Fatal(e)}
 notes,e:=os.ReadFile("/sys/kernel/notes");if e!=nil||physicalDigest(notes)!=ref.KernelNotesSHA256||read("/proc/sys/kernel/osrelease")!=ref.KernelRelease{t.Fatal("exact369 kernel notes/release differ",e)}
 f:=newSplitFixture(t);cfg:=f.g.config
 var fs syscall.Statfs_t;if e:=syscall.Statfs(FixedStateDir,&fs);e!=nil||uint64(fs.Type)!=0xef53{t.Fatal("dedicated ext4 state required",e)}
 k:=&linuxKernel{config:cfg};id,e:=k.Identity();if e!=nil{t.Fatal(e)}
 actual,e:=k.physicalOriginRead();if e!=nil{t.Fatal(e)}
 f.in.origin.First=actual;f.in.origin.ExpectedBootstrap.Identity=id
 f.in.origin.Config=cfg;f.in.source.Config=cfg;f.in.sourceRaw,e=json.Marshal(f.in.source);if e!=nil{t.Fatal(e)}
 f.in.origin.ResourceSourceSHA256=physicalDigest(f.in.sourceRaw);f.in.policy.ResourceSourceSHA256=f.in.origin.ResourceSourceSHA256
 f.in.policyRaw,e=json.Marshal(f.in.policy);if e!=nil{t.Fatal(e)};f.in.originSHA,e=f.in.origin.ReferenceSHA256();if e!=nil{t.Fatal(e)}
 s,e:=lockState(context.Background(),FixedStateDir);if e!=nil{t.Fatal(e)}
 raw,e:=json.Marshal(f.in.origin);if e!=nil{t.Fatal(e)};if e:=s.createPhysicalFile(physicalOriginDataName,raw);e!=nil{t.Fatal(e)};if e:=s.confirmPhysical(raw);e!=nil{t.Fatal(e)};s.close()
 g:=&Guard{config:cfg,kernel:k};before,e:=k.normalSplitRead();if e!=nil{t.Fatal(e)}
 out,e:=g.normalSplit(context.Background(),f.in,normalSplitPrepare);if e!=nil{t.Fatal(e)}
 if out.NormalDecreasePermit||!out.DurabilityConfirmed||out.Current.Parent.MemoryMaxBytes!=before.Parent.MemoryMaxBytes||out.Current.Parent.PidsMaxTasks!=before.Parent.PidsMaxTasks{t.Fatal("preparation altered parent bound or authorized normal")}
 if out.Current.Private.MemoryMaxBytes!=f.in.policy.PrivateMemoryMaxBytes||out.Current.Arrays.MemoryMaxBytes!=f.in.policy.InitialArraysMemoryMaxBytes||!out.Current.Private.Empty||!out.Current.Arrays.Empty{t.Fatal("actual leaf caps/empty differ")}
 var wal syscall.Stat_t;if e:=syscall.Stat(FixedStateDir,&wal);e!=nil{t.Fatal(e)}
 if out.Current.StateDevice!=control.Uint64(wal.Dev)||out.Current.StateInode!=control.Uint64(wal.Ino){t.Fatal("actual ext4 WAL inode differs")}
 original:=out.Original.Nonce;intent,e:=os.ReadFile(filepath.Join(FixedStateDir,normalSplitIntentName));if e!=nil{t.Fatal(e)}
 fmt.Println("PG17-S-SPLIT-001 PASS actual369_cgroup2_ext4_WAL_exact_two_leaves_parent_unchanged")
 for _,op:=range []normalSplitOperation{normalSplitPrepare,normalSplitReplay,normalSplitObserve}{r,e:=g.normalSplit(context.Background(),f.in,op);if e!=nil||r.Original.Nonce!=original||r.NormalDecreasePermit||r.DurabilityConfirmed!=(op!=normalSplitObserve){t.Fatal("same original idempotence",e)}}
 fmt.Println("PG17-S-SPLIT-002 PASS actual_original_identity_WAL_idempotence_non_authorizing")
 if e:=writeNumber(pgCgroup+"/arrays/memory.max",32<<20);e!=nil{t.Fatal(e)}
 changed,e:=g.normalSplit(context.Background(),f.in,normalSplitObserve);if e!=nil||changed.Current.Arrays.MemoryMaxBytes!=32<<20||changed.NormalDecreasePermit{t.Fatal("finite arrays observation",e)}
 fmt.Println("PG17-S-SPLIT-003 PASS finite_arrays_change_is_fact_only")
 if e:=writeNumber(pgCgroup+"/private/memory.max",192<<20);e!=nil{t.Fatal(e)}
 if _,e:=g.normalSplit(context.Background(),f.in,normalSplitReplay);e==nil{t.Fatal("private P drift accepted")}
 if e:=writeNumber(pgCgroup+"/private/memory.max",uint64(f.in.policy.PrivateMemoryMaxBytes));e!=nil{t.Fatal(e)}
 fmt.Println("PG17-S-SPLIT-004 PASS original_private_P_drift_rejected_no_repair")
 if e:=os.Mkdir(pgCgroup+"/private/unknown",0755);e!=nil{t.Fatal(e)}
 if _,e:=g.normalSplit(context.Background(),f.in,normalSplitObserve);e==nil{t.Fatal("unknown descendant accepted")}
 fmt.Println("PG17-S-SPLIT-005 PASS unknown_descendant_rejected_left_for_Guest_teardown")
 now,e:=os.ReadFile(filepath.Join(FixedStateDir,normalSplitIntentName));if e!=nil||string(intent)!=string(now){t.Fatal("intent changed")}
 if _,e:=os.Stat(filepath.Join(FixedStateDir,"state.json"));!os.IsNotExist(e){t.Fatal("resource state created")}
 files:=map[string]string{};for _,name:=range []string{normalSplitIntentName,normalSplitCompleteName,splitIdentityName("private"),splitIdentityName("arrays")}{b,e:=os.ReadFile(filepath.Join(FixedStateDir,name));if e!=nil{t.Fatal(e)};files[name]=string(b)}
 result:=map[string]any{"status":"PASS_COMPONENT_ONLY","kernel":ref,"initial":before,"prepared":out,"finiteArraysObserved":changed,"WAL":files,"scope":"actual kernel+topology+ext4 WAL, synthetic immutableSource and policy; fixed ISO loader NOT exercised","normalDecreaseExecuted":false,"normalDecreasePermit":false};b,e:=json.Marshal(result);if e!=nil{t.Fatal(e)}
 fmt.Println("PG17_SPLIT_TOPOLOGY_RESULT="+string(b));fmt.Println("PG17-S-SPLIT-006 PASS original_journal_retained_no_Birth_normal_or_parent_memory_write")
}
