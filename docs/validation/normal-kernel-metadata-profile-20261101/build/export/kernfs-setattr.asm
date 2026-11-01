
vmlinux:     file format elf64-x86-64


Disassembly of section .text:

ffffffff813fe7d0 <kernfs_iop_setattr>:
ffffffff813fe7d0:	55                   	push   %rbp
ffffffff813fe7d1:	48 89 e5             	mov    %rsp,%rbp
ffffffff813fe7d4:	41 57                	push   %r15
ffffffff813fe7d6:	41 56                	push   %r14
ffffffff813fe7d8:	41 55                	push   %r13
ffffffff813fe7da:	41 54                	push   %r12
ffffffff813fe7dc:	49 89 d4             	mov    %rdx,%r12
ffffffff813fe7df:	53                   	push   %rbx
ffffffff813fe7e0:	4c 8b 76 30          	mov    0x30(%rsi),%r14
ffffffff813fe7e4:	48 89 f3             	mov    %rsi,%rbx
ffffffff813fe7e7:	f6 02 07             	testb  $0x7,(%rdx)
ffffffff813fe7ea:	4d 8b be 58 02 00 00 	mov    0x258(%r14),%r15
ffffffff813fe7f1:	75 7d                	jne    ffffffff813fe870 <kernfs_iop_setattr+0xa0>
ffffffff813fe7f3:	4d 85 ff             	test   %r15,%r15
ffffffff813fe7f6:	0f 84 8d 00 00 00    	je     ffffffff813fe889 <kernfs_iop_setattr+0xb9>
ffffffff813fe7fc:	49 8b 47 08          	mov    0x8(%r15),%rax
ffffffff813fe800:	48 85 c0             	test   %rax,%rax
ffffffff813fe803:	49 0f 44 c7          	cmove  %r15,%rax
ffffffff813fe807:	4c 8b 68 50          	mov    0x50(%rax),%r13
ffffffff813fe80b:	49 81 c5 88 00 00 00 	add    $0x88,%r13
ffffffff813fe812:	4c 89 ef             	mov    %r13,%rdi
ffffffff813fe815:	e8 c6 e2 79 00       	call   ffffffff81b9cae0 <down_write>
ffffffff813fe81a:	48 89 de             	mov    %rbx,%rsi
ffffffff813fe81d:	4c 89 e2             	mov    %r12,%rdx
ffffffff813fe820:	48 c7 c7 a0 a6 50 82 	mov    $0xffffffff8250a6a0,%rdi
ffffffff813fe827:	e8 c4 6c f7 ff       	call   ffffffff813754f0 <setattr_prepare>
ffffffff813fe82c:	85 c0                	test   %eax,%eax
ffffffff813fe82e:	89 c3                	mov    %eax,%ebx
ffffffff813fe830:	74 19                	je     ffffffff813fe84b <kernfs_iop_setattr+0x7b>
ffffffff813fe832:	4c 89 ef             	mov    %r13,%rdi
ffffffff813fe835:	e8 96 c3 d0 ff       	call   ffffffff8110abd0 <up_write>
ffffffff813fe83a:	89 d8                	mov    %ebx,%eax
ffffffff813fe83c:	5b                   	pop    %rbx
ffffffff813fe83d:	41 5c                	pop    %r12
ffffffff813fe83f:	41 5d                	pop    %r13
ffffffff813fe841:	41 5e                	pop    %r14
ffffffff813fe843:	41 5f                	pop    %r15
ffffffff813fe845:	5d                   	pop    %rbp
ffffffff813fe846:	e9 95 29 7a 00       	jmp    ffffffff81ba11e0 <__x86_return_thunk>
ffffffff813fe84b:	4c 89 e6             	mov    %r12,%rsi
ffffffff813fe84e:	4c 89 ff             	mov    %r15,%rdi
ffffffff813fe851:	e8 ca fe ff ff       	call   ffffffff813fe720 <__kernfs_setattr>
ffffffff813fe856:	85 c0                	test   %eax,%eax
ffffffff813fe858:	89 c3                	mov    %eax,%ebx
ffffffff813fe85a:	75 d6                	jne    ffffffff813fe832 <kernfs_iop_setattr+0x62>
ffffffff813fe85c:	4c 89 e2             	mov    %r12,%rdx
ffffffff813fe85f:	4c 89 f6             	mov    %r14,%rsi
ffffffff813fe862:	48 c7 c7 a0 a6 50 82 	mov    $0xffffffff8250a6a0,%rdi
ffffffff813fe869:	e8 52 6a f7 ff       	call   ffffffff813752c0 <setattr_copy>
ffffffff813fe86e:	eb c2                	jmp    ffffffff813fe832 <kernfs_iop_setattr+0x62>
ffffffff813fe870:	bf 15 00 00 00       	mov    $0x15,%edi
ffffffff813fe875:	e8 f6 03 ca ff       	call   ffffffff8109ec70 <capable>
ffffffff813fe87a:	84 c0                	test   %al,%al
ffffffff813fe87c:	0f 85 71 ff ff ff    	jne    ffffffff813fe7f3 <kernfs_iop_setattr+0x23>
ffffffff813fe882:	bb ff ff ff ff       	mov    $0xffffffff,%ebx
ffffffff813fe887:	eb b1                	jmp    ffffffff813fe83a <kernfs_iop_setattr+0x6a>
ffffffff813fe889:	bb ea ff ff ff       	mov    $0xffffffea,%ebx
ffffffff813fe88e:	eb aa                	jmp    ffffffff813fe83a <kernfs_iop_setattr+0x6a>

Disassembly of section .init.text:

Disassembly of section .altinstr_aux:

Disassembly of section .altinstr_replacement:

Disassembly of section .exit.text:
