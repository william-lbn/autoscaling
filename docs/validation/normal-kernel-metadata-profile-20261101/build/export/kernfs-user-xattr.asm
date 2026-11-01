
vmlinux:     file format elf64-x86-64


Disassembly of section .text:

ffffffff813fe4a0 <kernfs_vfs_user_xattr_set>:
ffffffff813fe4a0:	55                   	push   %rbp
ffffffff813fe4a1:	4c 89 c6             	mov    %r8,%rsi
ffffffff813fe4a4:	48 89 e5             	mov    %rsp,%rbp
ffffffff813fe4a7:	41 57                	push   %r15
ffffffff813fe4a9:	41 56                	push   %r14
ffffffff813fe4ab:	4c 8b 75 10          	mov    0x10(%rbp),%r14
ffffffff813fe4af:	41 55                	push   %r13
ffffffff813fe4b1:	41 54                	push   %r12
ffffffff813fe4b3:	4d 89 cc             	mov    %r9,%r12
ffffffff813fe4b6:	53                   	push   %rbx
ffffffff813fe4b7:	48 89 cb             	mov    %rcx,%rbx
ffffffff813fe4ba:	e8 d1 69 f8 ff       	call   ffffffff81384e90 <xattr_full_name>
ffffffff813fe4bf:	bf 15 00 00 00       	mov    $0x15,%edi
ffffffff813fe4c4:	48 8b 9b 58 02 00 00 	mov    0x258(%rbx),%rbx
ffffffff813fe4cb:	49 89 c5             	mov    %rax,%r13
ffffffff813fe4ce:	e8 9d 07 ca ff       	call   ffffffff8109ec70 <capable>
ffffffff813fe4d3:	84 c0                	test   %al,%al
ffffffff813fe4d5:	0f 84 07 01 00 00    	je     ffffffff813fe5e2 <kernfs_vfs_user_xattr_set+0x142>
ffffffff813fe4db:	48 8b 43 08          	mov    0x8(%rbx),%rax
ffffffff813fe4df:	48 85 c0             	test   %rax,%rax
ffffffff813fe4e2:	48 0f 44 c3          	cmove  %rbx,%rax
ffffffff813fe4e6:	48 8b 40 50          	mov    0x50(%rax),%rax
ffffffff813fe4ea:	f6 40 08 08          	testb  $0x8,0x8(%rax)
ffffffff813fe4ee:	0f 84 e7 00 00 00    	je     ffffffff813fe5db <kernfs_vfs_user_xattr_set+0x13b>
ffffffff813fe4f4:	be 01 00 00 00       	mov    $0x1,%esi
ffffffff813fe4f9:	48 89 df             	mov    %rbx,%rdi
ffffffff813fe4fc:	e8 9f fc ff ff       	call   ffffffff813fe1a0 <__kernfs_iattrs>
ffffffff813fe501:	48 85 c0             	test   %rax,%rax
ffffffff813fe504:	0f 84 df 00 00 00    	je     ffffffff813fe5e9 <kernfs_vfs_user_xattr_set+0x149>
ffffffff813fe50a:	4d 85 e4             	test   %r12,%r12
ffffffff813fe50d:	48 8b 5b 70          	mov    0x70(%rbx),%rbx
ffffffff813fe511:	74 7f                	je     ffffffff813fe592 <kernfs_vfs_user_xattr_set+0xf2>
ffffffff813fe513:	ba 01 00 00 00       	mov    $0x1,%edx
ffffffff813fe518:	f0 0f c1 53 48       	lock xadd %edx,0x48(%rbx)
ffffffff813fe51d:	83 c2 01             	add    $0x1,%edx
ffffffff813fe520:	83 c2 80             	add    $0xffffff80,%edx
ffffffff813fe523:	7f 53                	jg     ffffffff813fe578 <kernfs_vfs_user_xattr_set+0xd8>
ffffffff813fe525:	45 89 f7             	mov    %r14d,%r15d
ffffffff813fe528:	44 89 f2             	mov    %r14d,%edx
ffffffff813fe52b:	f0 0f c1 53 4c       	lock xadd %edx,0x4c(%rbx)
ffffffff813fe530:	44 01 f2             	add    %r14d,%edx
ffffffff813fe533:	bf e4 ff ff ff       	mov    $0xffffffe4,%edi
ffffffff813fe538:	81 fa 00 00 02 00    	cmp    $0x20000,%edx
ffffffff813fe53e:	7f 31                	jg     ffffffff813fe571 <kernfs_vfs_user_xattr_set+0xd1>
ffffffff813fe540:	44 8b 45 18          	mov    0x18(%rbp),%r8d
ffffffff813fe544:	48 8d 78 38          	lea    0x38(%rax),%rdi
ffffffff813fe548:	4c 89 f1             	mov    %r14,%rcx
ffffffff813fe54b:	4c 89 e2             	mov    %r12,%rdx
ffffffff813fe54e:	4c 89 ee             	mov    %r13,%rsi
ffffffff813fe551:	e8 da 8b f8 ff       	call   ffffffff81387130 <simple_xattr_set>
ffffffff813fe556:	48 85 c0             	test   %rax,%rax
ffffffff813fe559:	48 89 c7             	mov    %rax,%rdi
ffffffff813fe55c:	74 6a                	je     ffffffff813fe5c8 <kernfs_vfs_user_xattr_set+0x128>
ffffffff813fe55e:	48 3d 00 f0 ff ff    	cmp    $0xfffffffffffff000,%rax
ffffffff813fe564:	77 0b                	ja     ffffffff813fe571 <kernfs_vfs_user_xattr_set+0xd1>
ffffffff813fe566:	4c 8b 78 20          	mov    0x20(%rax),%r15
ffffffff813fe56a:	e8 61 8a f8 ff       	call   ffffffff81386fd0 <simple_xattr_free>
ffffffff813fe56f:	31 ff                	xor    %edi,%edi
ffffffff813fe571:	f0 44 29 7b 4c       	lock sub %r15d,0x4c(%rbx)
ffffffff813fe576:	eb 05                	jmp    ffffffff813fe57d <kernfs_vfs_user_xattr_set+0xdd>
ffffffff813fe578:	bf e4 ff ff ff       	mov    $0xffffffe4,%edi
ffffffff813fe57d:	f0 ff 4b 48          	lock decl 0x48(%rbx)
ffffffff813fe581:	5b                   	pop    %rbx
ffffffff813fe582:	89 f8                	mov    %edi,%eax
ffffffff813fe584:	41 5c                	pop    %r12
ffffffff813fe586:	41 5d                	pop    %r13
ffffffff813fe588:	41 5e                	pop    %r14
ffffffff813fe58a:	41 5f                	pop    %r15
ffffffff813fe58c:	5d                   	pop    %rbp
ffffffff813fe58d:	e9 4e 2c 7a 00       	jmp    ffffffff81ba11e0 <__x86_return_thunk>
ffffffff813fe592:	44 8b 45 18          	mov    0x18(%rbp),%r8d
ffffffff813fe596:	48 8d 78 38          	lea    0x38(%rax),%rdi
ffffffff813fe59a:	31 d2                	xor    %edx,%edx
ffffffff813fe59c:	4c 89 f1             	mov    %r14,%rcx
ffffffff813fe59f:	4c 89 ee             	mov    %r13,%rsi
ffffffff813fe5a2:	e8 89 8b f8 ff       	call   ffffffff81387130 <simple_xattr_set>
ffffffff813fe5a7:	48 85 c0             	test   %rax,%rax
ffffffff813fe5aa:	48 89 c7             	mov    %rax,%rdi
ffffffff813fe5ad:	74 19                	je     ffffffff813fe5c8 <kernfs_vfs_user_xattr_set+0x128>
ffffffff813fe5af:	48 3d 00 f0 ff ff    	cmp    $0xfffffffffffff000,%rax
ffffffff813fe5b5:	77 ca                	ja     ffffffff813fe581 <kernfs_vfs_user_xattr_set+0xe1>
ffffffff813fe5b7:	48 8b 40 20          	mov    0x20(%rax),%rax
ffffffff813fe5bb:	f0 29 43 4c          	lock sub %eax,0x4c(%rbx)
ffffffff813fe5bf:	f0 ff 4b 48          	lock decl 0x48(%rbx)
ffffffff813fe5c3:	e8 08 8a f8 ff       	call   ffffffff81386fd0 <simple_xattr_free>
ffffffff813fe5c8:	31 ff                	xor    %edi,%edi
ffffffff813fe5ca:	5b                   	pop    %rbx
ffffffff813fe5cb:	89 f8                	mov    %edi,%eax
ffffffff813fe5cd:	41 5c                	pop    %r12
ffffffff813fe5cf:	41 5d                	pop    %r13
ffffffff813fe5d1:	41 5e                	pop    %r14
ffffffff813fe5d3:	41 5f                	pop    %r15
ffffffff813fe5d5:	5d                   	pop    %rbp
ffffffff813fe5d6:	e9 05 2c 7a 00       	jmp    ffffffff81ba11e0 <__x86_return_thunk>
ffffffff813fe5db:	bf a1 ff ff ff       	mov    $0xffffffa1,%edi
ffffffff813fe5e0:	eb 9f                	jmp    ffffffff813fe581 <kernfs_vfs_user_xattr_set+0xe1>
ffffffff813fe5e2:	bf ff ff ff ff       	mov    $0xffffffff,%edi
ffffffff813fe5e7:	eb 98                	jmp    ffffffff813fe581 <kernfs_vfs_user_xattr_set+0xe1>
ffffffff813fe5e9:	bf f4 ff ff ff       	mov    $0xfffffff4,%edi
ffffffff813fe5ee:	eb 91                	jmp    ffffffff813fe581 <kernfs_vfs_user_xattr_set+0xe1>

Disassembly of section .init.text:

Disassembly of section .altinstr_aux:

Disassembly of section .altinstr_replacement:

Disassembly of section .exit.text:
