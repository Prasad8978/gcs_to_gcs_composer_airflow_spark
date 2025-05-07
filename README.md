gcs_to_bq_load_dag_using_spark.py
the above file is the composer dag file and need to be upload into dags folder

gcs_to_gcs_spark_script.py
the abive file is the spark script and we need to load into one bucket, uploading into any bucket is fine

emp8_data.csv
the above csv data file also need to be upload into the bucket.




Note: Sometimes dataproc cluster is not crated from UI at that time we need to create it by CLI commands

gcloud dataproc clusters create cluster-abc `
  --enable-component-gateway `
  --region us-central1 `
  --subnet default `
  --no-address `
  --master-machine-type n2-standard-2 `
  --master-boot-disk-type pd-standard `
  --master-boot-disk-size 30 `
  --num-workers 2 `
  --worker-machine-type n2-standard-2 `
  --worker-boot-disk-type pd-standard `
  --worker-boot-disk-size 50 `
  --image-version 2.2-debian12 `
  --optional-components JUPYTER `
  --project rare-tome-458105-n0
