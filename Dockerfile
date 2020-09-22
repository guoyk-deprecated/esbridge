FROM golang:1.14 AS builder
ENV CGO_ENABLED 0
WORKDIR /go/src/app
ADD . .
RUN go build -mod vendor -o /esbridge

FROM alpine:3.12
RUN apk add --no-cache ca-certificates
COPY --from=builder /esbridge /esbridge
ADD run.sh /run.sh
CMD ["sh", "/run.sh"]