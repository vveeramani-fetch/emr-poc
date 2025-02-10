# EMR POC REPO

## Steps to Manually Execute
- Copy the job to pyspark job repo on S3
- Update spark-submit cli to point to the job and specify appropriate run window if necessary
- Add spark-submit step to corresponding EMR cluster

## Legacy Job
```
spark-submit 
    --deploy-mode cluster 
    --conf spark.executor.memory=32g 
    --conf spark.executor.memory.overhead=3g 
    --conf spark.executor.cores=4 
    --conf spark.yarn.am.memory=20g 
    --conf spark.yarn.am.memory.overhead=2g 
    --conf spark.yarn.am.cores=4 
    --conf spark.driver.memory=20g 
    --conf spark.dynamicAllocation.enabled=true 
    --conf spark.executor.extraJavaOptions=-=UseG1GC 
    --conf spark.driver.extraJavaOptions=-=UseG1GC 
    --conf spark.rpc.message.maxSize=1024 
    <absolutePathToJobScript>/offer-eligiblity.v1.py --raw-data s3://data-lake-857967394368-us-east-1-prod/data/stream/offer-service/offer-eligibility-snapshots/landing/

```
## Kafka Job (With run window)
```
spark-submit 
    --deploy-mode cluster 
    --conf spark.executor.memory=32g 
    --conf spark.executor.memory.overhead=3g 
    --conf spark.executor.cores=4 
    --conf spark.yarn.am.memory=20g 
    --conf spark.yarn.am.memory.overhead=2g 
    --conf spark.yarn.am.cores=4 
    --conf spark.driver.memory=20g 
    --conf spark.dynamicAllocation.enabled=true 
    --conf spark.executor.extraJavaOptions=-=UseG1GC 
    --conf spark.driver.extraJavaOptions=-=UseG1GC 
    --conf spark.rpc.message.maxSize=1024 
    <absolutePathToJobScript>/offer-eligiblity.v1.py --raw-data s3://data-lake-857967394368-us-east-1-prod/data/stream/offer-service/offer-eligibility-snapshots/landing/ --raw-data s3://data-lake-857967394368-us-east-1-prod/data/kafka-stream/offer-eligibility-snapshots/ -s "2025-01-15 20" -e "2024-01-15 21"

```
