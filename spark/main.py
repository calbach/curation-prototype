from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

DOB_CONCEPT_ID = 4083587

def run_pipeline():
  # XXX: This hardcoding of creds was required to get the project plumbing to work properly :(
  spark = SparkSession \
    .builder \
    .appName("curation-prototype") \
    .config("spark.master", "local") \
    .config("credentialsFile", "/usr/local/google/home/calbach/Downloads/aou-res-curation-test-d99cf02b87dd.json") \
    .getOrCreate()

  # Use the Cloud Storage bucket for temporary BigQuery export data used
  # by the connector.
  bucket = "gs://calbach-prototype-temp"
  spark.conf.set("temporaryGcsBucket", bucket)

  # Load data from BigQuery.
  domain_dfs = {}
  for tbl in ["person", "measurement", "condition_occurrence"]:
    for site in ["nyc", "pitt"]:
      df = spark.read.format("bigquery") \
        .option("table", f"calbach_prototype.{tbl}") \
        .option("project", "aou-res-curation-test") \
        .option("parentProject", "aou-res-curation-test") \
        .load()
      # Merge domains across sites.
      if tbl not in domain_dfs:
        domain_dfs[tbl] = df
      else:
        domain_dfs[tbl] = domain_dfs[tbl].union(df)


  # 1. Move data from person table elsewhere.
  person = domain_dfs["person"]
  extracted_meas = person.select(
      "person_id",
      lit(DOB_CONCEPT_ID).alias("measurement_concept_id").cast("bigint"),
      person.birth_datetime.alias("measurement_datetime")
  )
  person = person \
    .withColumn("year_of_birth", lit(None)) \
    .withColumn("month_of_birth", lit(None)) \
    .withColumn("day_of_birth", lit(None)) \
    .withColumn("birth_datetime", lit(None))

  # Force same columns in the same order.
  for f in domain_dfs["measurement"].columns:
    if f not in extracted_meas.columns:
      extracted_meas = extracted_meas.withColumn(f, lit(None))
  extracted_meas = extracted_meas.select(domain_dfs["measurement"].columns)

  # Finally, append the new measurement rows
  domain_dfs["measurement"] = domain_dfs["measurement"].union(extracted_meas)

  # Saving the data to JSON
  for (k, df) in domain_dfs.items():
    df.printSchema()
    df.write \
      .json(f"output/{k}.json")

if __name__ == "__main__":
  run_pipeline()
