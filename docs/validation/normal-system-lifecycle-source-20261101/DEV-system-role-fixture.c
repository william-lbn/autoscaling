void _start(void){for(;;)__asm__ volatile("mov $34, %%rax; syscall":::"rax","rcx","r11","memory");}
