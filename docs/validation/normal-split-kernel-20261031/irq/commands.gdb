set confirm off
set pagination off
set print thread-events off
set auto-load safe-path /nonexistent
set architecture i386:x86-64
file /kernel/kernel-symbols.elf
set remotetimeout 90
set remotelogfile /results/remote-rsp.log
target remote /results/gdb.sock
set scheduler-locking step
python
exec(open('/results/irq-driver.py').read())
end
quit
