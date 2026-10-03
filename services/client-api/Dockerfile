FROM golang:1.26-alpine AS builder

WORKDIR /src/client-api
COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/client-api .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -S clientapi && adduser -S -G clientapi clientapi
WORKDIR /app
COPY --from=builder /out/client-api ./client-api
USER clientapi
CMD ["./client-api"]
