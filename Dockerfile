FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -o /out/memoryd ./cmd/memoryd && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -o /out/memoryworker ./cmd/memoryworker && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -o /out/memoryrelay ./cmd/memoryrelay && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -o /out/memoryprojector ./cmd/memoryprojector
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/memoryd /memoryd
COPY --from=build /out/memoryworker /memoryworker
COPY --from=build /out/memoryrelay /memoryrelay
COPY --from=build /out/memoryprojector /memoryprojector
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/memoryd"]
