FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/ingest-pipeline .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -S ingest && adduser -S -G ingest ingest && mkdir -p /app/data && chown ingest:ingest /app/data
WORKDIR /app
COPY --from=builder /out/ingest-pipeline ./ingest-pipeline
ENV PIPELINE_DB_PATH=/app/data/ingest.db
EXPOSE 8094
USER ingest
CMD ["./ingest-pipeline"]
