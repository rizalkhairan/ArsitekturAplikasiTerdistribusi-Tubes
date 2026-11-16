SHELL := /bin/sh

.PHONY: dev-up dev-down dev-reset test fmt k8s-up k8s-down k8s-render sensor-exam sensor-check images load-sustained load-throughput load-rolling duplicate gameday

dev-up:
	docker compose up --build -d
	@echo "coordinator :8099  canonical-store :8095  field-notifier :8097  ingest :8094  sensor :8087"

dev-down:
	docker compose down

dev-reset:
	docker compose down -v
	docker compose up --build -d

test:
	cd libs/observability && go test ./...
	cd services/coordinator && go test ./...
	cd services/canonical-store && go test ./...
	cd services/field-notifier && go test ./...
	cd services/bnpb && go test ./...
	cd services/client-api && go test ./...
	cd services/auth && go test ./...
	cd services/bmkg && go test ./...
	cd services/pvmbg && go test ./...
	cd services/ingest-pipeline && go test ./...
	cd services/notifier && go test ./...
	cd services/dashboard-updater && go test ./...
	cd services/analytics-consumer && go test ./...
	cd services/shared && go test ./...

fmt:
	cd libs/observability && gofmt -w .
	cd services/coordinator && gofmt -w .
	cd services/canonical-store && gofmt -w .
	cd services/field-notifier && gofmt -w .
	cd services/bnpb && gofmt -w .
	cd services/client-api && gofmt -w .

k8s-up:
	kubectl apply -k deploy

k8s-down:
	kubectl delete -k deploy --ignore-not-found

k8s-render:
	kubectl kustomize deploy > /dev/null
	kubectl kustomize deploy/sensor-pantai/overlays/exam > /dev/null

sensor-exam:
	kubectl kustomize deploy/sensor-pantai/overlays/exam | kubectl apply -f -

sensor-check:
	./scripts/check-sensor-base.sh

images:
	./scripts/build-images.sh

duplicate:
	./loadtest/duplicate-detector.sh

load-sustained:
	./loadtest/scaling.sh

load-throughput:
	./loadtest/throughput.sh

load-rolling:
	./loadtest/rolling-update.sh

gameday:
	./scripts/gameday.sh
