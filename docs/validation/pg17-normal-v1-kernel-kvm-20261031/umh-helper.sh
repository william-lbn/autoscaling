#!/bin/busybox sh
if test "${ACTION:-}" = add && test "${SUBSYSTEM:-}" = mem && test "${DEVPATH:-}" = /devices/virtual/mem/null; then
  printf 'PG17_UMH_TARGET_HELPER_EXECUTED=%s:%s:%s\n' "$ACTION" "$SUBSYSTEM" "$DEVPATH" >/umh-helper-marker
fi
