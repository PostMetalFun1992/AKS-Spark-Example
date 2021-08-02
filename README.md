# Spark Basic Homework

## 1. Setup infrastructure via Terraform:
```
# You must have Azure account
az login
cd ./terraform

terraform init
terraform plan -out ./state/terraform.plan
terraform apply ./state/terraform.plan

cd ../

# Destroy all necessary infrastructure after completing the homework:
terraform destroy
```
* IMPORTANT: Do not forget to add Role Assignment "Storage Blob Data Contributor" to your application registration
on your storage account. (TODO: Automate this step with Terraform)

## 2. Setup local runtime:
* Build docker container for local launch:
```
docker build -f ./docker/Dockerfile -t spark-azure .
```
* Then setup all necessary credentials:
```
cp ./config/storage-creds.ini.sample ./config/storage-creds.ini  # fill credentials inside the copied file
```
* (Optional) Setup venv for nvim:
```
python3 -m venv ~/.python-envs/de-course-env/
source ~/.python-envs/de-course-env/bin/activate
python3 -m pip install flake8 black isort
```
* Run spark application locally inside docker container:
```
docker run --rm --name spaz \
    -v `pwd`/src:/opt/spark/work-dir/spark-app \
    -v `pwd`/config:/etc/secrets \
    spark-azure:latest spark-submit local:///opt/spark/work-dir/spark-app/main/spark_main.py
```

## 3. Setup to run inside aks:
* Build docker with all necessary sources:
```
docker build -f ./docker/Dockerfile.k8s -t spark-azure-k8s .
```
* Push container into remote registry:
```
az login
az acr login --name crkkabanovwesteurope

docker tag spark-azure-k8s:latest crkkabanovwesteurope.azurecr.io/spark/spark-azure-k8s
docker push crkkabanovwesteurope.azurecr.io/spark/spark-azure-k8s

# Checking that container is inside remote registry:
az acr repository show-tags --name crkkabanovwesteurope --repository spark/spark-azure-k8s --output table
```
* Setup local kubectl:
```
az aks install-cli
az aks get-credentials --resource-group rg-kkabanov-westeurope --name aks-kkabanov-westeurope
# Checking that your local kubectl has been set up to interact with aks:
kubectl get nodes
```
* Healthcheck: Your k8s cluster can pull images from your remote docker registry:
```
az aks check-acr --name aks-kkabanov-westeurope --resource-group rg-kkabanov-westeurope --acr crkkabanovwesteurope.azurecr.io
```
* Add cluster role binding:
```
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
```
* Add k8s secret with all necessary credentials:
```
cp ./config/secret.yaml.sample ./config/secret.yaml  # fill the same fields as in storage-creds.ini
kubectl apply -f ./config/secret.yaml
```
* Launch your application (IMPORTANT: do not forget to install Spark locally and add $SPARK_HOME/bit into $PATH):
```
spark-submit \
    --master k8s://https://<API server address>:443 \
    --deploy-mode cluster \
    --name spark-basics-hw \
    --conf spark.kubernetes.driver.secrets.sparkcreds=/etc/secrets \
    --conf spark.kubernetes.executor.secrets.sparkcreds=/etc/secrets \
    --conf spark.kubernetes.container.image=crkkabanovwesteurope.azurecr.io/spark/spark-azure-k8s:latest \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    local:///opt/spark/work-dir/spark-app/main/spark_main.py
```
* Check driver's and executors' state:
```
kubectl get pods
kubectl logs -f <DRIVER_POD_NAME>
```
