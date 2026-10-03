FROM golang:1.26-alpine AS builder

# Build with services/ as the context to reuse BMKG's generated API contract.
WORKDIR /src/bnpb
COPY bmkg/go.mod bmkg/go.sum /src/bmkg/
COPY bnpb/go.mod bnpb/go.sum ./
RUN go mod download

COPY bmkg/proto /src/bmkg/proto
COPY bnpb/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/bnpb .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -S bnpb && adduser -S -G bnpb bnpb \
    && mkdir -p /app/data && chown bnpb:bnpb /app/data
WORKDIR /app
COPY --from=builder /out/bnpb ./bnpb
ENV DB_PATH=/app/data/bnpb.db
EXPOSE 8080
USER bnpb
CMD ["./bnpb"]
