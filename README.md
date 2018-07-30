# setup environment

- install minikube
- remove ubuntu network manager (it breaks kubedns)
- start minikube with `sudo CHANGE_MINIKUBE_NONE_USER=true minikube start --vm-driver=none`
- install helm
- setup helm with `helm init`
- add incubator helm repo `helm repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator`
- create namespace `kubectl create ns kafka`
- deploy kafka `helm install --name my-kafka --namespace kafka incubator/kafka`

# minimal helm
- render helm to filesystem
- commit result to git
- deploy like everything else

# example

github.com/vsouza/go-kafka-example
