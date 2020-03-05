Setup
-

```
mkvirtualenv cdr-spark
pip install pyspark

gsutil cp gs://spark-lib/bigquery/spark-bigquery-latest.jar .
```

Run
-

```
spark-submit --jars spark-bigquery-latest.jar main.py
```
