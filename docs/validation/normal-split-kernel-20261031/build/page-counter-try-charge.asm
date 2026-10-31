
vmlinux:     file format elf64-x86-64


Disassembly of section .text:

ffffffff813333b0 <page_counter_try_charge>:
ffffffff813333b0:	55                   	push   %rbp
ffffffff813333b1:	48 89 e5             	mov    %rsp,%rbp
ffffffff813333b4:	41 57                	push   %r15
ffffffff813333b6:	49 89 ff             	mov    %rdi,%r15
ffffffff813333b9:	41 56                	push   %r14
ffffffff813333bb:	41 55                	push   %r13
ffffffff813333bd:	49 89 f5             	mov    %rsi,%r13
ffffffff813333c0:	41 54                	push   %r12
ffffffff813333c2:	53                   	push   %rbx
ffffffff813333c3:	48 89 fb             	mov    %rdi,%rbx
ffffffff813333c6:	48 83 ec 08          	sub    $0x8,%rsp
ffffffff813333ca:	48 85 ff             	test   %rdi,%rdi
ffffffff813333cd:	48 89 55 d0          	mov    %rdx,-0x30(%rbp)
ffffffff813333d1:	44 0f b6 b7 c0 00 00 	movzbl 0xc0(%rdi),%r14d
ffffffff813333d8:	00 
ffffffff813333d9:	75 28                	jne    ffffffff81333403 <page_counter_try_charge+0x53>
ffffffff813333db:	eb 4e                	jmp    ffffffff8133342b <page_counter_try_charge+0x7b>
ffffffff813333dd:	48 8b 43 78          	mov    0x78(%rbx),%rax
ffffffff813333e1:	4c 39 e0             	cmp    %r12,%rax
ffffffff813333e4:	73 11                	jae    ffffffff813333f7 <page_counter_try_charge+0x47>
ffffffff813333e6:	4c 89 63 78          	mov    %r12,0x78(%rbx)
ffffffff813333ea:	48 8b 43 70          	mov    0x70(%rbx),%rax
ffffffff813333ee:	4c 39 e0             	cmp    %r12,%rax
ffffffff813333f1:	73 04                	jae    ffffffff813333f7 <page_counter_try_charge+0x47>
ffffffff813333f3:	4c 89 63 70          	mov    %r12,0x70(%rbx)
ffffffff813333f7:	48 8b 9b e8 00 00 00 	mov    0xe8(%rbx),%rbx
ffffffff813333fe:	48 85 db             	test   %rbx,%rbx
ffffffff81333401:	74 28                	je     ffffffff8133342b <page_counter_try_charge+0x7b>
ffffffff81333403:	4c 89 e8             	mov    %r13,%rax
ffffffff81333406:	f0 48 0f c1 03       	lock xadd %rax,(%rbx)
ffffffff8133340b:	4d 8d 64 05 00       	lea    0x0(%r13,%rax,1),%r12
ffffffff81333410:	4c 39 a3 e0 00 00 00 	cmp    %r12,0xe0(%rbx)
ffffffff81333417:	72 2a                	jb     ffffffff81333443 <page_counter_try_charge+0x93>
ffffffff81333419:	45 84 f6             	test   %r14b,%r14b
ffffffff8133341c:	74 bf                	je     ffffffff813333dd <page_counter_try_charge+0x2d>
ffffffff8133341e:	4c 89 e6             	mov    %r12,%rsi
ffffffff81333421:	48 89 df             	mov    %rbx,%rdi
ffffffff81333424:	e8 e7 fd ff ff       	call   ffffffff81333210 <propagate_protected_usage>
ffffffff81333429:	eb b2                	jmp    ffffffff813333dd <page_counter_try_charge+0x2d>
ffffffff8133342b:	48 83 c4 08          	add    $0x8,%rsp
ffffffff8133342f:	b8 01 00 00 00       	mov    $0x1,%eax
ffffffff81333434:	5b                   	pop    %rbx
ffffffff81333435:	41 5c                	pop    %r12
ffffffff81333437:	41 5d                	pop    %r13
ffffffff81333439:	41 5e                	pop    %r14
ffffffff8133343b:	41 5f                	pop    %r15
ffffffff8133343d:	5d                   	pop    %rbp
ffffffff8133343e:	e9 1d dd 86 00       	jmp    ffffffff81ba1160 <__x86_return_thunk>
ffffffff81333443:	f0 4c 29 2b          	lock sub %r13,(%rbx)
ffffffff81333447:	48 8b 45 d0          	mov    -0x30(%rbp),%rax
ffffffff8133344b:	48 83 83 80 00 00 00 	addq   $0x1,0x80(%rbx)
ffffffff81333452:	01 
ffffffff81333453:	48 89 18             	mov    %rbx,(%rax)
ffffffff81333456:	9c                   	pushf
ffffffff81333457:	5b                   	pop    %rbx
ffffffff81333458:	fa                   	cli
ffffffff81333459:	4c 39 38             	cmp    %r15,(%rax)
ffffffff8133345c:	74 1b                	je     ffffffff81333479 <page_counter_try_charge+0xc9>
ffffffff8133345e:	4c 89 ff             	mov    %r15,%rdi
ffffffff81333461:	4c 89 ee             	mov    %r13,%rsi
ffffffff81333464:	e8 37 fe ff ff       	call   ffffffff813332a0 <page_counter_cancel>
ffffffff81333469:	48 8b 45 d0          	mov    -0x30(%rbp),%rax
ffffffff8133346d:	4d 8b bf e8 00 00 00 	mov    0xe8(%r15),%r15
ffffffff81333474:	4c 39 38             	cmp    %r15,(%rax)
ffffffff81333477:	75 e5                	jne    ffffffff8133345e <page_counter_try_charge+0xae>
ffffffff81333479:	80 e7 02             	and    $0x2,%bh
ffffffff8133347c:	74 01                	je     ffffffff8133347f <page_counter_try_charge+0xcf>
ffffffff8133347e:	fb                   	sti
ffffffff8133347f:	48 83 c4 08          	add    $0x8,%rsp
ffffffff81333483:	31 c0                	xor    %eax,%eax
ffffffff81333485:	5b                   	pop    %rbx
ffffffff81333486:	41 5c                	pop    %r12
ffffffff81333488:	41 5d                	pop    %r13
ffffffff8133348a:	41 5e                	pop    %r14
ffffffff8133348c:	41 5f                	pop    %r15
ffffffff8133348e:	5d                   	pop    %rbp
ffffffff8133348f:	e9 cc dc 86 00       	jmp    ffffffff81ba1160 <__x86_return_thunk>

Disassembly of section .init.text:

Disassembly of section .altinstr_aux:

Disassembly of section .altinstr_replacement:

Disassembly of section .exit.text:
