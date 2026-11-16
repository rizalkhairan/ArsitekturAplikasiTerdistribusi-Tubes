FROM golang:1.24-alpine AS build
WORKDIR /src
COPY libs/observability ./libs/observability
COPY services/coordinator ./services/coordinator
WORKDIR /src/services/coordinator
RUN CGO_ENABLED=0 go build -trimpath -o /out/coordinator .
FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -S app && adduser -S -G app app && mkdir -p /data && chown app:app /data
WORKDIR /app
COPY --from=build /out/coordinator ./coordinator
ENV STATE_PATH=/data/coordinator.json ADDR=:8080
USER app
EXPOSE 8080
CMD ["./coordinator"]
