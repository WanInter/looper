#!/bin/sh
set -eu
# The parent test process still reports race errors; avoid its default 1s exit
# delay for every fake-CLI subprocess when the test binary was built with -race.
export GORACE="${GORACE:-} atexit_sleep_ms=0"
exec "$LOOPER_VERIFIED_DELIVERY_TEST_BINARY" -test.run='^TestVerifiedDeliveryFakeGH$' -- "$@"
