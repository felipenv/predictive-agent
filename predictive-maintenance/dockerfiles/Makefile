REGISTRY ?= nschenonemck
IMAGE ?= mlrun-oai
TAG ?= 1.9.0rc3

.PHONY: help
help: ## Display available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

build: ## Build image
	docker build --platform linux/amd64 --build-context root=../.. -t $(REGISTRY)/$(IMAGE):$(TAG) .

push: ## Push image
	docker push $(REGISTRY)/$(IMAGE):$(TAG)

update: build push ## Update image