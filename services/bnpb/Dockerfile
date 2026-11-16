FROM golang:1.26-alpine AS builder

# Root context: services/ and libs/ are both needed.
WORKDIR /src/services/bnpb
COPY services/bmkg/go.mod services/bmkg/go.sum /src/services/bmkg/
COPY libs/observability/go.mod /src/libs/observability/
COPY services/bnpb/go.mod services/bnpb/go.sum ./
RUN go mod download

COPY libs/observability /src/libs/observability
COPY services/bmkg/proto /src/services/bmkg/proto
COPY services/bnpb/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/bnpb .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -S bnpb && adduser -S -G bnpb bnpb \
    && mkdir -p /app/data && chown bnpb:bnpb /app/data
WORKDIR /app
COPY --from=builder /out/bnpb ./bnpb
ENV DB_PATH=/app/data/bnpb.db
EXPOSE 8080 8085
USER bnpb
CMD ["./bnpb"]
