
manifests/kafka: values/kafka.yaml $(shell find charts/kafka -type f)
	mkdir -p ./manifests/kafka
	rm -r ./manifests/kafka
	helm template \
	  --name kafka \
	  --values ./values/kafka.yaml \
	  --output-dir ./manifests \
	    ./charts/kafka
	
values/kafka.yaml:
	mkdir -p values
	touch values/kafka.yaml

charts/kafka:
	rm -r ./charts/kafka || true
	helm fetch \
	  --repo http://storage.googleapis.com/kubernetes-charts-incubator \
	  --untar \
	  --untardir ./charts \
	  --version 0.8.5 \
	    kafka

