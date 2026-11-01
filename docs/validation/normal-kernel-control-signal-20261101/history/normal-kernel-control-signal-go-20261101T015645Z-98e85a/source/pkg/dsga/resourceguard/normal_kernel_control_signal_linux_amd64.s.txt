//go:build linux && amd64

#include "textflag.h"
#include "funcdata.h"
#include "go_asm.h"

// 单原leader末门，没有CALL/RET/Go错误返回；SIGURG不能在此unsafe asm插入调度。
// Landlock是单线程属性：此门之后立即exec清旧M，新业务线程从唯一domain继承。
TEXT ·normalKernelSignalScopeExecRaw(SB),NOSPLIT|NOFRAME,$0-16
    PCDATA $PCDATA_UnsafePoint, $-2
    MOVQ fd+0(FP), DI
    XORQ SI, SI
    MOVQ $446, AX // landlock_restrict_self(fd,0)
    SYSCALL
    TESTQ AX, AX
    JNE scopeFailed
    MOVQ image+8(FP), R8
    MOVQ normalKernelSignalExecImage_Path(R8), DI
    MOVQ normalKernelSignalExecImage_Argv(R8), SI
    MOVQ normalKernelSignalExecImage_Environ(R8), DX
    MOVQ $59, AX // execve；全部C向量仍由typed image指针保持存活。
    SYSCALL
    MOVQ $126, DI
    JMP failClosed
scopeFailed:
    MOVQ $125, DI
failClosed:
    MOVQ $231, AX // exit_group，不能回Go碰已分离的原线程domain。
    SYSCALL
    BYTE $0x0f; BYTE $0x0b // UD2；即使exit被未知filter拒绝也不进入业务。
    JMP failClosed
