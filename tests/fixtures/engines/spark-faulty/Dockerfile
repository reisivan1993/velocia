# Test-only engine for the planted-fault suite (plan T23): vanilla Apache Spark plus a Python
# startup hook (a .pth file; Ubuntu's own sitecustomize.py shadows ours) that misbehaves
# according to spark.velocia.test.fault. Never shipped.
FROM velocia/engine-spark-vanilla:3.5.9
USER root
COPY velocia_faults.py velocia_faults.pth /usr/local/lib/python3.10/dist-packages/
USER spark
