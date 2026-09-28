FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/notifier .

FROM alpine:3.22
RUN addgroup -S notifier && adduser -S -G notifier notifier
WORKDIR /app
COPY --from=builder /out/notifier ./notifier
EXPOSE 8080
USER notifier
CMD ["./notifier"]
