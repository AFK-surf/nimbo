# Builds nimbod into a small image. Run it like the binary, e.g.
#   docker run -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... -e NIMBOD_NODE_KEY=... \
#     -p 8443:8443 -p 41641:41641/udp nimbo run -bucket B -public-name broker.example.com ...
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /nimbod ./cmd/nimbod

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && adduser -D -H -u 10001 nimbo
COPY --from=build /nimbod /usr/local/bin/nimbod
USER nimbo
# WebSocket endpoints, the mesh's UDP port, and metrics (bind -metrics-listen
# to :9090 to expose it).
EXPOSE 8443 41641/udp 9090
ENTRYPOINT ["/usr/local/bin/nimbod"]
CMD ["run"]
