module github.com/rizalkhairan/distributed-system/client-api

go 1.26.1

require (
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/google/uuid v1.6.0
	github.com/rizalkhairan/distributed-system/observability v0.0.0
)

replace github.com/rizalkhairan/distributed-system/observability => ../../libs/observability
