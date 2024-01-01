import sys
from pyspark.sql import SparkSession as sps
from pyspark.sql.functions import count


def spark_job():
    # Create a New Spark Session.
    spark = sps.builder.appName("DATABRICKS_MMCOUNT_SPARK_JOB").getOrCreate()

    # Read the File as a CSV into Spark Dataframe by inferring Schema.
    # And specifying that the file has Header.
    #
    # Example: Run spark_job in DataBricks LakeHouse.
    # Upload the mnm_dataset.csv in the Databricks DBFS.
    # Copy the URL of the File in the DBFS in the Read Function.
    df = spark.read.format("csv").option("header", "true").option("mergeSchema", "true").option("inferSchema", "true").load(
        "dbfs:/FileStore/mnm_dataset.csv")

    # SQL Query:
    # SELECT Columns:   -->> "State", "Color", "Count"
    # GROUP By Columns: -->> "State", "Color"
    df.select("State", "Color", "Count").groupBy("State", "Color").agg(count("Color").alias("Total")).orderBy(
        "Total", ascending=True).show(n=600, truncate=False)



def spark_job_02():
    # Create a New Spark Session.
    spark = sps.builder.appName("DATABRICKS_MMCOUNT_SPARK_JOB").getOrCreate()

    # Read the File as a CSV into Spark Dataframe by inferring Schema.
    # And specifying that the file has Header.
    #
    # Example: Run spark_job in DataBricks LakeHouse.
    # Upload the mnm_dataset.csv in the Databricks DBFS.
    # Copy the URL of the File in the DBFS in the Read Function.
    df = spark.read.format("csv").option("header", "true").option("mergeSchema", "true").option("inferSchema", "true").load(
        "dbfs:/FileStore/mnm_dataset.csv")

    # SQL Query:
    # SELECT Columns:   -->> "State", "Color", "Count"
    # WHERE:            -->> "State" = "WA"
    # GROUP By Columns: -->> "State", "Color"
    df.select("State", "Color", "Count").where(df.State == "WA").groupBy("State", "Color").agg(
        count("Count").alias("Total")).orderBy("Total", ascending=True).show(n=600, truncate=True)


if __name__ == "__main__":
    spark_job()
    spark_job_02()
