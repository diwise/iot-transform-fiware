FROM --platform=$BUILDPLATFORM docker.io/golang:1.27 AS builder

# Set the Current Working Directory inside the container
WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

WORKDIR /app/cmd/iot-transform-fiware

ARG TARGETARCH
ARG TARGETOS
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build


FROM registry.access.redhat.com/ubi9/ubi-minimal
WORKDIR /opt/diwise

COPY --from=builder --chown=1001 /app/cmd/iot-transform-fiware/iot-transform-fiware /opt/diwise

# Ingen seed bakas in i imagen: regelmallar tillförs via mount på
# MODELS_DIR eller skapas via regel-API:t. Saknad/tom katalog betyder
# tom regeluppsättning vid start (varning i logg, ingen startspärr).
ENV MODELS_DIR=/opt/diwise/config/models

RUN chown -R 1001 /opt/diwise

EXPOSE 8000 8080
USER 1001

ENTRYPOINT ["/opt/diwise/iot-transform-fiware"]
