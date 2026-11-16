FROM golang:1.24-alpine AS build
WORKDIR /src
COPY libs/observability ./libs/observability
COPY services/field-notifier ./services/field-notifier
WORKDIR /src/services/field-notifier
RUN CGO_ENABLED=0 go build -trimpath -o /out/field-notifier .
FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=build /out/field-notifier ./field-notifier
ENV ADDR=:8080
USER app
EXPOSE 8080
CMD ["./field-notifier"]
