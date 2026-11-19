FROM golang:1.26-alpine AS builder

WORKDIR /src/services/client-api
COPY libs/observability/go.mod /src/libs/observability/
COPY services/shared/go.mod services/shared/go.sum /src/services/shared/
COPY services/client-api/go.mod services/client-api/go.sum ./
RUN go mod download

COPY libs/observability /src/libs/observability
COPY services/shared /src/services/shared
COPY services/client-api/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/client-api .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -S clientapi && adduser -S -G clientapi clientapi
WORKDIR /app
COPY --from=builder /out/client-api ./client-api
USER clientapi
CMD ["./client-api"]
