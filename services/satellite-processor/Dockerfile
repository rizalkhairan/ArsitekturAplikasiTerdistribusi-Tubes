FROM golang:1.26-alpine AS build
WORKDIR /src
COPY shared/go.mod shared/go.sum ./shared/
COPY satellite-processor/go.mod satellite-processor/go.sum ./satellite-processor/
WORKDIR /src/satellite-processor
RUN go mod download
COPY shared/ /src/shared/
COPY satellite-processor/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/satellite-processor .
RUN mkdir -p /out/data /out/config && touch /out/data/.keep && chmod 0750 /out/data /out/config && chown -R 65532:65532 /out/data /out/config

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build --chown=nonroot:nonroot /out/satellite-processor /app/satellite-processor
COPY --from=build --chown=nonroot:nonroot /out/data/ /app/data/
COPY --from=build --chown=nonroot:nonroot /out/config/ /app/config/
COPY --chown=nonroot:nonroot satellite-processor/testdata/ /app/config/
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/app/satellite-processor"]
