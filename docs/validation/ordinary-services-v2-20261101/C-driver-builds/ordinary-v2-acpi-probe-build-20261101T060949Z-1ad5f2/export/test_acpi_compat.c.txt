/* TEST_ONLY: exact ACPI package after final fixed-path/ACK checks, independent of udev known unsupported broadcast. */
#define main ordinary_compat_other_cases
#include "test_compat.c"
#undef main
int main(void) {
    setbuf(stdout, 0);
    negative();
    test_acpi();
    puts("ORDINARY_ACPI_COMPAT_PASS_NOT_GATE_OR_NORMAL");
    return 0;
}
