
vmlinux:     file format elf64-x86-64


Disassembly of section .text:

ffffffff813334b0 <page_counter_uncharge>:
ffffffff813334b0:	55                   	push   %rbp
ffffffff813334b1:	48 89 e5             	mov    %rsp,%rbp
ffffffff813334b4:	41 56                	push   %r14
ffffffff813334b6:	41 55                	push   %r13
ffffffff813334b8:	41 54                	push   %r12
ffffffff813334ba:	53                   	push   %rbx
ffffffff813334bb:	9c                   	pushf
ffffffff813334bc:	41 5d                	pop    %r13
ffffffff813334be:	fa                   	cli
ffffffff813334bf:	49 89 f6             	mov    %rsi,%r14
ffffffff813334c2:	48 89 fb             	mov    %rdi,%rbx
ffffffff813334c5:	49 89 f4             	mov    %rsi,%r12
ffffffff813334c8:	49 f7 de             	neg    %r14
ffffffff813334cb:	48 85 ff             	test   %rdi,%rdi
ffffffff813334ce:	75 0e                	jne    ffffffff813334de <page_counter_uncharge+0x2e>
ffffffff813334d0:	eb 36                	jmp    ffffffff81333508 <page_counter_uncharge+0x58>
ffffffff813334d2:	48 8b 9b e8 00 00 00 	mov    0xe8(%rbx),%rbx
ffffffff813334d9:	48 85 db             	test   %rbx,%rbx
ffffffff813334dc:	74 2a                	je     ffffffff81333508 <page_counter_uncharge+0x58>
ffffffff813334de:	4c 89 f6             	mov    %r14,%rsi
ffffffff813334e1:	f0 48 0f c1 33       	lock xadd %rsi,(%rbx)
ffffffff813334e6:	4c 29 e6             	sub    %r12,%rsi
ffffffff813334e9:	78 34                	js     ffffffff8133351f <page_counter_uncharge+0x6f>
ffffffff813334eb:	80 bb c0 00 00 00 00 	cmpb   $0x0,0xc0(%rbx)
ffffffff813334f2:	74 de                	je     ffffffff813334d2 <page_counter_uncharge+0x22>
ffffffff813334f4:	48 89 df             	mov    %rbx,%rdi
ffffffff813334f7:	e8 14 fd ff ff       	call   ffffffff81333210 <propagate_protected_usage>
ffffffff813334fc:	48 8b 9b e8 00 00 00 	mov    0xe8(%rbx),%rbx
ffffffff81333503:	48 85 db             	test   %rbx,%rbx
ffffffff81333506:	75 d6                	jne    ffffffff813334de <page_counter_uncharge+0x2e>
ffffffff81333508:	41 f7 c5 00 02 00 00 	test   $0x200,%r13d
ffffffff8133350f:	74 01                	je     ffffffff81333512 <page_counter_uncharge+0x62>
ffffffff81333511:	fb                   	sti
ffffffff81333512:	5b                   	pop    %rbx
ffffffff81333513:	41 5c                	pop    %r12
ffffffff81333515:	41 5d                	pop    %r13
ffffffff81333517:	41 5e                	pop    %r14
ffffffff81333519:	5d                   	pop    %rbp
ffffffff8133351a:	e9 41 dc 86 00       	jmp    ffffffff81ba1160 <__x86_return_thunk>
ffffffff8133351f:	80 3d bd 75 26 01 00 	cmpb   $0x0,0x12675bd(%rip)        # ffffffff8259aae3 <__already_done.0>
ffffffff81333526:	74 0b                	je     ffffffff81333533 <page_counter_uncharge+0x83>
ffffffff81333528:	48 c7 03 00 00 00 00 	movq   $0x0,(%rbx)
ffffffff8133352f:	31 f6                	xor    %esi,%esi
ffffffff81333531:	eb b8                	jmp    ffffffff813334eb <page_counter_uncharge+0x3b>
ffffffff81333533:	4c 89 e2             	mov    %r12,%rdx
ffffffff81333536:	48 c7 c7 40 43 1f 82 	mov    $0xffffffff821f4340,%rdi
ffffffff8133353d:	c6 05 9f 75 26 01 01 	movb   $0x1,0x126759f(%rip)        # ffffffff8259aae3 <__already_done.0>
ffffffff81333544:	e8 07 ce d5 ff       	call   ffffffff81090350 <__warn_printk>
ffffffff81333549:	0f 0b                	ud2
ffffffff8133354b:	eb db                	jmp    ffffffff81333528 <page_counter_uncharge+0x78>

Disassembly of section .init.text:

Disassembly of section .altinstr_aux:

Disassembly of section .altinstr_replacement:

Disassembly of section .exit.text:
