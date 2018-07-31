
.PHONY: minikube-start
minikube-start: ##@minikube start minikube
	sudo CHANGE_MINIKUBE_NONE_USER=true minikube start --vm-driver=none
	sudo minikube addons enable freshpod
	sudo minikube addons enable ingress
	sudo minikube addons enable heapster

.PHONY: minikube-stop
minikube-stop: ##@minikube stop minikube
	sudo minikube stop

.PHONY: minikube-delete
minikube-delete: ##@minikube remove minikube
	sudo minikube delete
