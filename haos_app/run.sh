#!/usr/bin/with-contenv bashio
export EVMQTT_MQTT_HOST="$(bashio::services mqtt "host")"
export EVMQTT_MQTT_USERNAME="$(bashio::services mqtt "username")"
export EVMQTT_MQTT_PASSWORD="$(bashio::services mqtt "password")"
export EVMQTT_DB=/data/db.toml
exec /evmqtt-rs --daemon
