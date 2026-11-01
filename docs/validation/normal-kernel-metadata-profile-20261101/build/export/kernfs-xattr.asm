
vmlinux:     file format elf64-x86-64


Disassembly of section .text:

ffffffff813fe600 <kernfs_vfs_xattr_set>:
ffffffff813fe600:	55                   	push   %rbp
ffffffff813fe601:	4c 89 c6             	mov    %r8,%rsi
ffffffff813fe604:	48 89 e5             	mov    %rsp,%rbp
ffffffff813fe607:	41 55                	push   %r13
ffffffff813fe609:	49 89 cd             	mov    %rcx,%r13
ffffffff813fe60c:	41 54                	push   %r12
ffffffff813fe60e:	4d 89 cc             	mov    %r9,%r12
ffffffff813fe611:	53                   	push   %rbx
ffffffff813fe612:	e8 79 68 f8 ff       	call   ffffffff81384e90 <xattr_full_name>
ffffffff813fe617:	bf 15 00 00 00       	mov    $0x15,%edi
ffffffff813fe61c:	4d 8b ad 58 02 00 00 	mov    0x258(%r13),%r13
ffffffff813fe623:	48 89 c3             	mov    %rax,%rbx
ffffffff813fe626:	e8 45 06 ca ff       	call   ffffffff8109ec70 <capable>
ffffffff813fe62b:	84 c0                	test   %al,%al
ffffffff813fe62d:	74 46                	je     ffffffff813fe675 <kernfs_vfs_xattr_set+0x75>
ffffffff813fe62f:	be 01 00 00 00       	mov    $0x1,%esi
ffffffff813fe634:	4c 89 ef             	mov    %r13,%rdi
ffffffff813fe637:	e8 64 fb ff ff       	call   ffffffff813fe1a0 <__kernfs_iattrs>
ffffffff813fe63c:	48 85 c0             	test   %rax,%rax
ffffffff813fe63f:	74 3b                	je     ffffffff813fe67c <kernfs_vfs_xattr_set+0x7c>
ffffffff813fe641:	44 8b 45 18          	mov    0x18(%rbp),%r8d
ffffffff813fe645:	48 8d 78 38          	lea    0x38(%rax),%rdi
ffffffff813fe649:	4c 89 e2             	mov    %r12,%rdx
ffffffff813fe64c:	48 89 de             	mov    %rbx,%rsi
ffffffff813fe64f:	48 8b 4d 10          	mov    0x10(%rbp),%rcx
ffffffff813fe653:	e8 d8 8a f8 ff       	call   ffffffff81387130 <simple_xattr_set>
ffffffff813fe658:	48 3d 00 f0 ff ff    	cmp    $0xfffffffffffff000,%rax
ffffffff813fe65e:	48 89 c7             	mov    %rax,%rdi
ffffffff813fe661:	77 07                	ja     ffffffff813fe66a <kernfs_vfs_xattr_set+0x6a>
ffffffff813fe663:	e8 68 89 f8 ff       	call   ffffffff81386fd0 <simple_xattr_free>
ffffffff813fe668:	31 c0                	xor    %eax,%eax
ffffffff813fe66a:	5b                   	pop    %rbx
ffffffff813fe66b:	41 5c                	pop    %r12
ffffffff813fe66d:	41 5d                	pop    %r13
ffffffff813fe66f:	5d                   	pop    %rbp
ffffffff813fe670:	e9 6b 2b 7a 00       	jmp    ffffffff81ba11e0 <__x86_return_thunk>
ffffffff813fe675:	b8 ff ff ff ff       	mov    $0xffffffff,%eax
ffffffff813fe67a:	eb ee                	jmp    ffffffff813fe66a <kernfs_vfs_xattr_set+0x6a>
ffffffff813fe67c:	b8 f4 ff ff ff       	mov    $0xfffffff4,%eax
ffffffff813fe681:	eb e7                	jmp    ffffffff813fe66a <kernfs_vfs_xattr_set+0x6a>

Disassembly of section .init.text:

Disassembly of section .altinstr_aux:

Disassembly of section .altinstr_replacement:

Disassembly of section .exit.text:
