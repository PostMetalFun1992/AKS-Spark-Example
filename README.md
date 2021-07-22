* Setup needed requirements into your env `pip install -r requirements.txt`
* Add your code in `src/main/`
* Test your code with `src/tests/`
* Package your artifacts
* Modify dockerfile if needed
* Build and push docker image
* Deploy infrastructure with terraform
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
....
terraform destroy
```
* Launch Spark app in cluster mode on AKS
```
spark-submit \
    --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
    --deploy-mode cluster \
    --name sparkbasics \
    --conf spark.kubernetes.container.image=<spark-image> \
    ...
```
* Build local spark runtime:
```
docker build -f ./docker/Dockerfile -t spark-azure .
```
* Run spark-script:
```
docker run -it --name spaz -v `pwd`/src:/home/spark-app spark-azure:latest spark-submit /home/spark-app/main/spark_main.py
```
