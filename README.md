# genetics_data_revision
This repository hosts the code to adapt the genetics output data to their data and schema iterations

## Set up

```bash
# Provision env
git clone https://github.com/opentargets/genetics_data_revision.git
cd genetics_data_revision
python3 -m venv env
source env/bin/activate
python3 -m pip install -r requirements.txt

# Cluster initialization
export CLUSTER_NAME=genetics-data-revision
export CLUSTER_REGION=europe-west1

gcloud dataproc clusters create ${CLUSTER_NAME} \
    --image-version=2.0 \
    --region=${CLUSTER_REGION} \
    --metadata 'PIP_PACKAGES=pyspark==3.2.0 hydra-core==1.2.0 pathlib==1.0.1' \
    --initialization-actions gs://goog-dataproc-initialization-actions-europe-west1/python/pip-install.sh                                                  \
    --master-machine-type=n1-standard-32 \
    --master-boot-disk-size=100 \
    --max-idle=1h

# Package the repo structure to provision the cluster with it
zip -x src/transform.py -r code_bundle.zip .

# Job submission
gcloud dataproc jobs submit pyspark \
  src/transform.py \
  --cluster=${CLUSTER_NAME} \
  --py-files code_bundle.zip \
  --files='config/config.yaml'
```