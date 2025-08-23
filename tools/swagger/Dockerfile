ARG GO_VERSION=1.24.1
ARG SWAG_VERSION=v1.16.5

FROM golang:${GO_VERSION}-alpine AS builder
ARG SWAG_VERSION

RUN apk add --no-cache git \
 && go install github.com/swaggo/swag/cmd/swag@${SWAG_VERSION}

FROM golang:${GO_VERSION}-alpine
COPY --from=builder /go/bin/swag /usr/local/bin/swag
WORKDIR /workspace
ENTRYPOINT ["swag"]
