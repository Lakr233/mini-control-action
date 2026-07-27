# Build a static minicontrol-runner binary and ship it on distroless.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/minicontrol-runner ./cmd/minicontrol-runner \
    && mkdir -p /out/state-dir

# distroless/static ships CA certificates (needed for TLS to GitHub and the
# mini-control API) and nothing else.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/minicontrol-runner /usr/local/bin/minicontrol-runner
# Pre-create the state dir owned by nonroot (65532) so the named volume
# inherits writable ownership instead of root:root.
COPY --from=build --chown=65532:65532 /out/state-dir /var/lib/minicontrol-runner
VOLUME ["/var/lib/minicontrol-runner"]
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/minicontrol-runner"]
CMD ["--config", "/etc/minicontrol-runner/config.yaml"]
