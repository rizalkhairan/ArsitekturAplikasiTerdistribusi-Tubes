FROM golang:1.26-alpine AS builder

WORKDIR /src/auth
COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/auth .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -S auth && adduser -S -G auth auth \
    && mkdir -p /app/data && chown auth:auth /app/data
WORKDIR /app
COPY --from=builder /out/auth ./auth
ENV DB_PATH=/app/data/auth.db
USER auth
CMD ["./auth"]
