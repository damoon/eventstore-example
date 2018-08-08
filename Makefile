include ./hack/help.mk
include ./hack/minikube.mk

.PHONY: environment
environment: ##@setup render service yaml files and apply to current kubernetes namespace
	$(MAKE) -C environment all
	kubectl apply -R -f environment/manifests

lint:
	golangci-lint run -e ./vendor ./...

pkg/pb/products.pb.go: pkg/pb/products.proto
	protoc --go_out=. pkg/pb/products.proto
