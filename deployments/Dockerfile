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

# Bakade standardmodeller. En mount på MODELS_DIR ersätter dem vid drift.
COPY --chown=1001 deployments/models /opt/diwise/config/models
ENV MODELS_DIR=/opt/diwise/config/models

RUN chown -R 1001 /opt/diwise

EXPOSE 8000 8080
USER 1001

ENTRYPOINT ["/opt/diwise/iot-transform-fiware"]
