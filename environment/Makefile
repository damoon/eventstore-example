# from https://blog.giantswarm.io/what-you-yaml-is-what-you-get/

all: manifests/kafka manifests/redis

manifests/kafka: values/kafka.yaml $(shell find charts/kafka -type f)
	mkdir -p ./manifests/kafka
	rm -r ./manifests/kafka
	helm template \
	  --name kafka \
	  --values ./values/kafka.yaml \
	  --output-dir ./manifests \
	    ./charts/kafka

manifests/redis: values/redis.yaml $(shell find charts/redis -type f)
	mkdir -p ./manifests/redis
	rm -r ./manifests/redis
	helm template \
	  --name redis \
	  --values ./values/redis.yaml \
	  --output-dir ./manifests \
	    ./charts/redis
	
values/kafka.yaml:
	mkdir -p values
	touch values/kafka.yaml
	
values/redis.yaml:
	mkdir -p values
	touch values/redis.yaml

charts/kafka:
	rm -r ./charts/kafka || true
	helm fetch \
	  --repo http://storage.googleapis.com/kubernetes-charts-incubator \
	  --untar \
	  --untardir ./charts \
	  --version 0.8.5 \
	    kafka

charts/redis:
	rm -r ./charts/redis || true
	helm fetch \
	  --repo https://kubernetes-charts.storage.googleapis.com \
	  --untar \
	  --untardir ./charts \
	  --version 3.7.3 \
	    redis
