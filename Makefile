PYTHON ?= python3
KUBECTL ?= kubectl
export KUBECTL
export IMAGE_ARCHIVE IMAGE_TAG IMAGE_ARCH BUILD_IMAGES
export BOOTSTRAP_PROVIDER DEV_NODES

.DEFAULT_GOAL := help

.PHONY: help config bootstrap bootstrap-plan host-tools vm-stop vm-destroy provision images deploy render platform-check platform-status topology dev-up dev-render dev-down dev-smoke dev-forward

help:
	@echo 'make host-tools     Install Linux virtualization prerequisites (sudo)'
	@echo 'make bootstrap-plan Preview the VM plan without making changes'
	@echo 'make bootstrap      Create/configure worker VMs and join k3s'
	@echo 'make images         Build an image bundle (Docker build machine)'
	@echo 'make deploy         Deploy the image bundle onto the k3s cluster'
	@echo 'make dev-up         Create kind development cluster and deploy bundle'
	@echo 'make dev-up BUILD_IMAGES=1  Build images and start development profile'
	@echo 'make dev-up DEV_NODES=2    Use two container nodes (1, 2 or 3 supported)'
	@echo 'make dev-smoke      Check login, ingestion and access scopes in development'
	@echo 'make dev-forward    Expose development API at http://localhost:9080'
	@echo 'make vm-stop        Gracefully stop managed worker VMs'
	@echo 'make observability-forward  Open a localhost Grafana tunnel (OBSERVABILITY_PROFILE=--dev for kind)'
	@echo 'make observability-check    Check telemetry backend readiness and application scrapes'
	@echo 'make platform-check Run deployment checks'
	@echo 'make canonical-render  Preview the opt-in PostgreSQL/Citus/Patroni overlay'
	@echo 'make canonical-integration  Test canonical SQL with three synchronous replica pairs'
	@echo 'make dev-network    Add NetworkPolicy enforcement to the kind profile'
	@echo 'make tester-check   Verify Tester Agent on every node and list practice scenarios'
	@echo 'make isolation-check Verify admission, RBAC and network enforcement'
	@echo '                    Add SECURITY_PROFILE=--dev for development checks'

host-tools:
	bash deploy/k3s/host-tools.sh

bootstrap-plan:
	$(PYTHON) deploy/k3s/bootstrap.py plan

bootstrap:
	$(PYTHON) deploy/k3s/bootstrap.py up

vm-stop:
	$(PYTHON) deploy/k3s/bootstrap.py stop

vm-destroy:
	$(PYTHON) deploy/k3s/bootstrap.py destroy

dev-up:
	$(PYTHON) deploy/k3s/dev.py up

dev-render:
	@$(PYTHON) deploy/k3s/dev.py render

dev-down:
	$(PYTHON) deploy/k3s/dev.py down

dev-smoke:
	$(PYTHON) deploy/k3s/dev.py smoke

dev-forward:
	$(PYTHON) deploy/k3s/dev.py forward

config:
	$(PYTHON) deploy/k3s/manage.py init

provision:
	$(PYTHON) deploy/k3s/manage.py provision

images:
	$(PYTHON) deploy/k3s/manage.py images

deploy:
	$(PYTHON) deploy/k3s/manage.py deploy

render:
	@$(PYTHON) deploy/k3s/manage.py render

platform-check:
	$(PYTHON) -m unittest discover -s deploy/tests -p 'test_*.py'

platform-status:
	$(PYTHON) deploy/k3s/manage.py status

topology:
	@$(PYTHON) deploy/k3s/manage.py topology

.PHONY: tester-install tester-check isolation-check
# Use SECURITY_PROFILE=--dev for the dedicated kind kubeconfig.
tester-install:
	$(PYTHON) deploy/k3s/security.py install $(SECURITY_PROFILE)

tester-check:
	$(PYTHON) deploy/k3s/security.py agent-check $(SECURITY_PROFILE)

isolation-check:
	$(PYTHON) deploy/k3s/security.py isolation-check $(SECURITY_PROFILE)

.PHONY: dev-network
dev-network:
	$(PYTHON) deploy/k3s/dev.py network

.PHONY: observability-render observability-install observability-check observability-forward
# Use OBSERVABILITY_PROFILE=--dev for the dedicated kind kubeconfig.
observability-render:
	@$(PYTHON) deploy/k3s/observability.py render

observability-install:
	$(PYTHON) deploy/k3s/observability.py install $(OBSERVABILITY_PROFILE)

observability-check:
	$(PYTHON) deploy/k3s/observability.py check $(OBSERVABILITY_PROFILE)

observability-forward:
	$(PYTHON) deploy/k3s/observability.py forward $(OBSERVABILITY_PROFILE)

.PHONY: observability-validate
observability-validate:
	$(PYTHON) deploy/k3s/observability.py validate

.PHONY: telemetry-smoke
telemetry-smoke:
	$(PYTHON) deploy/k3s/telemetry_smoke.py $(OBSERVABILITY_PROFILE)

.PHONY: canonical-render canonical-check canonical-test canonical-integration
canonical-render:
	@$(PYTHON) deploy/k3s/canonical.py render

canonical-check:
	$(PYTHON) deploy/k3s/canonical.py check $(SECURITY_PROFILE)

canonical-test:
	cd services/bnpb && go test -race ./internal/canonical ./internal/api

canonical-integration:
	$(PYTHON) deploy/k3s/canonical_smoke.py

.PHONY: canonical-recovery canonical-recovery-integration
# Example: make canonical-recovery RECOVERY_ACTION=status SECURITY_PROFILE=--dev
canonical-recovery:
	$(PYTHON) deploy/k3s/recovery.py $(or $(RECOVERY_ACTION),status) $(SECURITY_PROFILE) $(RECOVERY_ARGS)

canonical-recovery-integration:
	$(PYTHON) deploy/k3s/canonical_smoke.py --recovery
