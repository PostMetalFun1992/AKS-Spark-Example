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
docker build -f ./docker/Dockerfile.k8s -t spark-azure-k8s .
```
* Before run spark scripts:
```
cp ./docker/storage-creds.ini.sample ./docker/storage-creds.ini  # copy and fill all required credentials
```
* Run spark-script:
```
docker run --rm --name spaz \
    -v `pwd`/src:/opt/spark/work-dir/spark-app \
    -v `pwd`/docker:/etc/secrets \
    spark-azure:latest spark-submit local:///opt/spark/work-dir/spark-app/main/spark_main.py

docker run --rm --name spaz \
    -v `pwd`/src:/opt/spark/work-dir/spark-app \
    -v `pwd`/docker:/etc/secrets \
    spark-azure-k8s:latest spark-submit local:///opt/spark/work-dir/spark-app/main/spark_main.py
```
* Setup venv for nvim:
```
python3 -m venv ~/.python-envs/de-course-env/
source ~/.python-envs/de-course-env/bin/activate
python3 -m pip install flake8 black isort
```
* Push containers into remote registry:
```
az login
az acr login --name crkkabanovwesteurope

docker tag spark-azure:latest crkkabanovwesteurope.azurecr.io/spark/spark-azure
docker push crkkabanovwesteurope.azurecr.io/spark/spark-azure

docker tag spark-azure-k8s:latest crkkabanovwesteurope.azurecr.io/spark/spark-azure-k8s
docker push crkkabanovwesteurope.azurecr.io/spark/spark-azure-k8s

az acr repository list --name crkkabanovwesteurope --output table

az acr repository show-tags --name crkkabanovwesteurope --repository spark/spark-azure --output table
az acr repository show-tags --name crkkabanovwesteurope --repository spark/spark-azure-k8s --output table

```
* Interact with aks:
```
az aks install-cli
az aks get-credentials --resource-group rg-kkabanov-westeurope --name aks-kkabanov-westeurope
kubectl get nodes
```
* Check acr - aks intergration:
```
az aks check-acr --name aks-kkabanov-westeurope --resource-group rg-kkabanov-westeurope --acr crkkabanovwesteurope.azurecr.io
```
