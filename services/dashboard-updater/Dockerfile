FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/dashboard-updater .

FROM alpine:3.22
RUN addgroup -S dashboard && adduser -S -G dashboard dashboard
WORKDIR /app
COPY --from=builder /out/dashboard-updater ./dashboard-updater
EXPOSE 8080
USER dashboard
CMD ["./dashboard-updater"]
