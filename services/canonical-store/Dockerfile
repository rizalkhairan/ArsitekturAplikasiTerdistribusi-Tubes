FROM golang:1.24-alpine AS build
WORKDIR /src
COPY libs/observability ./libs/observability
COPY services/canonical-store ./services/canonical-store
WORKDIR /src/services/canonical-store
RUN CGO_ENABLED=0 go build -trimpath -o /out/canonical-store .
FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -S app && adduser -S -G app app && mkdir -p /data && chown app:app /data
WORKDIR /app
COPY --from=build /out/canonical-store ./canonical-store
ENV DATA_PATH=/data/events.json ADDR=:8080
USER app
EXPOSE 8080
CMD ["./canonical-store"]
