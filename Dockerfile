# First step, building
FROM golang:alpine

RUN mkdir /root/builder \
    && apk update \
    && apk add git build-base \
    && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

COPY go.mod go.sum /root/builder/
RUN cd /root/builder \
 && go mod download

COPY . /root/builder
RUN cd /root/builder \
    && make lint build

# Second step, final image
FROM alpine:latest

WORKDIR /usr/sbin
RUN apk update \
    && apk --no-cache add tzdata ca-certificates \
    && update-ca-certificates

COPY --from=0 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=0 /root/builder/dist/timescaledb-event-streamer .

CMD /usr/sbin/timescaledb-event-streamer
