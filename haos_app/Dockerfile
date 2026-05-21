FROM ghcr.io/home-assistant/base:latest

ARG TARGETOS
ARG TARGETARCH

COPY haos_app/run.sh /
COPY ${TARGETOS}/${TARGETARCH}/evmqtt-rs /


LABEL \
  io.hass.version="0.2.0" \
  io.hass.type="app" \
  io.hass.name="evmqtt-rs" \
  io.hass.description="Turns keyboard key presses into MQTT messages" \
  io.hass.url="https://github.com/bdolgov/evmqtt-rs"

CMD [ "/run.sh" ]
