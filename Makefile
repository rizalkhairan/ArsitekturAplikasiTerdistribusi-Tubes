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
	@echo 'make platform-check Run deployment checks'

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
