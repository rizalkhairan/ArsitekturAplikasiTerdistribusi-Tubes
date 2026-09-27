FROM golang:1.26-alpine AS builder

WORKDIR /src/casdoor-bootstrap
COPY casdoor-bootstrap/go.mod casdoor-bootstrap/go.sum ./
RUN go mod download

COPY casdoor-bootstrap/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/casdoor-bootstrap .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates curl
WORKDIR /app
COPY --from=builder /out/casdoor-bootstrap ./casdoor-bootstrap
CMD ["./casdoor-bootstrap"]
