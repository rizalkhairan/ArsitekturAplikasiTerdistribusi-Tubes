FROM golang:1.26-alpine AS builder

WORKDIR /src/client-api
COPY client-api/go.mod client-api/go.sum ./
COPY shared/go.mod shared/go.sum /src/shared/
RUN go mod download

COPY client-api/ ./
COPY shared/ /src/shared/
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/client-api .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -S clientapi && adduser -S -G clientapi clientapi
WORKDIR /app
COPY --from=builder /out/client-api ./client-api
USER clientapi
CMD ["./client-api"]
