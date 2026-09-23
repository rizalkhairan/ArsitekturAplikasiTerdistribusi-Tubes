FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY main.go .

RUN CGO_ENABLED=0 GOOS=linux go build -o pvmbg .

FROM alpine:3.22

WORKDIR /app

COPY --from=builder /app/pvmbg .

CMD ["./pvmbg"]