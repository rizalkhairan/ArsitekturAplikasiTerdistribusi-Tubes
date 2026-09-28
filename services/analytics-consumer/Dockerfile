FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/analytics-consumer .

FROM alpine:3.22
RUN addgroup -S analytics && adduser -S -G analytics analytics
WORKDIR /app
COPY --from=builder /out/analytics-consumer ./analytics-consumer
EXPOSE 8080
USER analytics
CMD ["./analytics-consumer"]
