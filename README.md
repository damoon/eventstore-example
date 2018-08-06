# setup environment

- install docker, kubectl, minikube, chrome, curl...
- disable dnsmasq cache form ubuntu network manager (it breaks kubedns)
- start minikube `make minikube-start`
- deploy kafka `make environment`

# example

github.com/vsouza/go-kafka-example


# demo

go run ./cmd/lorem-ipsum/main.go --brokerList=$KAFKA:9092 --redisAddress=$REDIS:6379
go run ./cmd/product-details/main.go --brokerList=$KAFKA:9092 --redisAddress=$REDIS:6379

go run ./cmd/products-create/main.go -seed 0 -rows 1000000 > products-1.csv
go run ./cmd/products-alternate/main.go -seed 0 < products-1.csv > products-2.csv

time go run ./cmd/products-import/main.go --brokerList=$KAFKA:9092 ./products-1.csv

docker exec $REDISCONTAINER redis-cli SMEMBERS lorem-ipsum
