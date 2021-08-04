# Spark Basic Homework

## 0. Prerequisites
- Docker
- Terraform
- Local spark installation (You need it in order to launch your Spark app in AKS)
- Azure account
- Registered application in Azure AD

## 1. Setup infrastructure via Terraform:
```
az login
cd ./terraform

terraform init
terraform plan -out ./state/terraform.plan
terraform apply ./state/terraform.plan

cd ../

# Destroy all necessary infrastructure after completing the homework:
terraform destroy
```
* **IMPORTANT:** Do not forget to add Role Assignment "Storage Blob Data Contributor" to the application registration
in your storage account. (**TODO:** Automate this step with Terraform)

## 2. Setup to launch locally via Docker:
* Build docker container to launch the application locally:
```
docker build -f ./docker/Dockerfile -t spark-azure .
```
* Then provide all necessary credentials:
```
cp ./config/storage-creds.ini.sample ./config/storage-creds.ini  # Fill credentials inside the copied file
```
* Launch spark application locally inside a docker container:
```
docker run --rm --name spark-app \
    -v `pwd`/src:/opt/spark/work-dir \
    -v `pwd`/config:/etc/secrets \
    spark-azure:latest spark-submit local:///opt/spark/work-dir/spark_main.py
```
* (Optional) Setup venv for nvim:
```
python3 -m venv ~/.python-envs/de-course-env/
source ~/.python-envs/de-course-env/bin/activate
python3 -m pip install flake8 black isort
```

## 3. Setup to launch inside AKS:
* Build derived docker container with all necessary source files:
```
docker build -f ./docker/Dockerfile.k8s -t spark-azure-k8s .
```
* Push container into your ACR:
```
az login
az acr login --name crkkabanovwesteurope

docker tag spark-azure-k8s:latest crkkabanovwesteurope.azurecr.io/spark/spark-azure-k8s
docker push crkkabanovwesteurope.azurecr.io/spark/spark-azure-k8s

# Check that container has been pushed successfully:
az acr repository show-tags --name crkkabanovwesteurope --repository spark/spark-azure-k8s --output table
```
* Setup local kubectl:
```
az aks install-cli
az aks get-credentials --resource-group rg-kkabanov-westeurope --name aks-kkabanov-westeurope

# Check that your local kubectl has been set up to interact with AKS:
kubectl get nodes
```
* Healthcheck: Your AKS cluster can pull images from your ACR:
```
az aks check-acr --name aks-kkabanov-westeurope --resource-group rg-kkabanov-westeurope --acr crkkabanovwesteurope.azurecr.io
```
* Add necessary cluster role binding:
```
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
```
* Fill all necessary credentials inside k8s secret and send it to AKS:
```
cp ./config/secret.yaml.sample ./config/secret.yaml  # The same fields as in storage-creds.ini
kubectl apply -f ./config/secret.yaml
```
* Launch your application inside AKS (**IMPORTANT:** Do not forget to install Spark locally and add `$SPARK_HOME/bin` to `$PATH`):
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
