# setup environment

- install docker, kubectl, minikube, chrome, curl...
- disable dnsmasq cache form ubuntu network manager (it breaks kubedns)
- start minikube `make minikube-start`
- deploy kafka `make environment`

# example

github.com/vsouza/go-kafka-example


# kafka architecture

http://cloudurable.com/blog/kafka-architecture/index.html

https://blog.scottlogic.com/2018/04/17/comparing-big-data-messaging.html

csv -> product importer (producer) -> kafka -.
                                             |-> product consumer -> redis
                                             `-> categories consumer -> redis

# demo

go run ./inventory/csv-fake-create/main.go    -seed 0 -rows 100 > products-100-1.csv
go run ./inventory/csv-fake-alternate/main.go -seed 0           < products-100-1.csv > products-100-2.csv
meld products-100-1.csv products-100-2.csv

go run ./inventory/csv-fake-create/main.go    -seed 0 -rows 1000000 > products-1m-1.csv
go run ./inventory/csv-fake-alternate/main.go -seed 0               < products-1m-1.csv > products-1m-2.csv

time go run ./inventory/csv-import/main.go --brokerList=$KAFKA:9092 ./products-1m-1.csv

go run ./inventory/products/main.go --brokerList=$KAFKA:9092 --redisAddress=$REDIS:6379
go run ./inventory/categories/main.go --brokerList=$KAFKA:9092 --redisAddress=$REDIS:6379 --verbose

csvtool format '%(1)\n' products-1.csv | head
kubectl exec -ti redis-master-0 -- redis-cli get da32648d-38ee-4b92-a534-1cbf062e4707


csvtool format '%(5)\n' products-1.csv | sort | uniq -c | grep -v "      1 " | sort -h -r | head
kubectl exec -ti redis-master-0 -- redis-cli smembers excellentiam/cura
kubectl exec -ti redis-master-0 -- redis-cli smembers abditioribus/apud
kubectl exec -ti redis-master-0 -- redis-cli smembers abditioribus/admiratio
kubectl exec -ti redis-master-0 -- redis-cli smembers bla

time go run ./inventory/csv-import/main.go --brokerList=$KAFKA:9092 --verbose ./products-1m-2.csv ./products-1m-1.csv

time bash -c 'cp products-1m-1.csv /tmp/dontcare && sync'



# possible service grouping

discovery
    cross selling
    topseller
    landing pages
    search

inventory
    stock
    products
    categories

checkout
    cart session
    history
    status

customers
    login
    registration
    password recovery
    profile
    adressesbook
    payment methods

fullfillment
    retoure
    payment
    shipping
