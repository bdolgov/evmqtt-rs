#!/bin/sh
set -e

if ! getent group evmqtt-rs >/dev/null 2>&1; then
    addgroup -S evmqtt-rs
fi

if ! id -u evmqtt-rs >/dev/null 2>&1; then
    adduser -S -D -H \
        -h /var/lib/evmqtt-rs \
        -s /sbin/nologin \
        -G evmqtt-rs \
        -g "evmqtt-rs daemon" \
        evmqtt-rs
fi

addgroup evmqtt-rs input 2>/dev/null || true

install -d -m 0750 -o evmqtt-rs -g evmqtt-rs /var/lib/evmqtt-rs
