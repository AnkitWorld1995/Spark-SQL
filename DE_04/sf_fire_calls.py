import s3fs
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from s3fs import S3FileSystem
import fastparquet as fp

S3_DATA_SOURCE_PATH = "s3://emr-dump/data/sf-fire-calls.csv"
S3_DATA_OUTPUT_PATH = "s3://emr-dump/output/"
S3_PARQUET_FILE_PATH = "s3://authsvcbucket-01/*.parquet"


# S3_DATA_SOURCE_PATH = 'data/sf-fire-calls.csv'
# S3_DATA_OUTPUT_PATH = 'output/'


def start_spark_session():
    spark = (SparkSession
             .builder
             .appName('SF-Fire-Calls')
             .getOrCreate())
    return spark


def sf_schema():
    fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                              StructField('UnitID', StringType(), True),
                              StructField('IncidentNumber', IntegerType(), True),
                              StructField('CallType', StringType(), True),
                              StructField('CallDate', StringType(), True),
                              StructField('WatchDate', StringType(), True),
                              StructField('CallFinalDisposition', StringType(), True),
                              StructField('AvailableDtTm', StringType(), True),
                              StructField('Address', StringType(), True),
                              StructField('City', StringType(), True),
                              StructField('Zipcode', IntegerType(), True),
                              StructField('Battalion', StringType(), True),
                              StructField('StationArea', StringType(), True),
                              StructField('Box', StringType(), True),
                              StructField('OriginalPriority', StringType(), True),
                              StructField('Priority', StringType(), True),
                              StructField('FinalPriority', IntegerType(), True),
                              StructField('ALSUnit', BooleanType(), True),
                              StructField('CallTypeGroup', StringType(), True),
                              StructField('NumAlarms', IntegerType(), True),
                              StructField('UnitType', StringType(), True),
                              StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                              StructField('FirePreventionDistrict', StringType(), True),
                              StructField('SupervisorDistrict', StringType(), True),
                              StructField('Neighborhood', StringType(), True),
                              StructField('Location', StringType(), True),
                              StructField('RowID', StringType(), True),
                              StructField('Delay', FloatType(), True)])
    return fire_schema


def dataframe_init():
    sp = start_spark_session()
    df_schema = sf_schema()
    dataframe = sp.read.csv(S3_DATA_SOURCE_PATH, header=True, schema=df_schema)

    fire_dfn = (
        dataframe
        .withColumn("IncidentDate", F.to_timestamp(F.col("CallDate"), "MM/dd/yyyy")).drop('CallDate')
        .withColumn("OnWatchDate", F.to_timestamp(F.col("WatchDate"), "MM/dd/yyyy")).drop('WatchDate')
        .withColumn("AvailableDtTS", F.to_timestamp(F.col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")).drop(
            'AvailableDtTm')
    )

    spark_query = (fire_dfn
                   .filter((F.col('CallType').like('%Medical%')) & (F.year('IncidentDate').isin(2017, 2018)) &
                           (F.month('IncidentDate').between(5, 6)))
                   .orderBy(F.year('IncidentDate'), ascending=True))

    # print(writePath)
    spark_query.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH)
    print('Total number of records: %s' % spark_query.count())


def read_aws_s3():
    s3 = S3FileSystem()
    fs = s3fs.core.S3FileSystem()
    s3_open = s3.open(path=S3_PARQUET_FILE_PATH)
    all_paths_from_s3 = fs.glob(path=S3_PARQUET_FILE_PATH)
    fp_obj = fp.ParquetFile(all_paths_from_s3, open_with=s3_open)
    # convert to pandas dataframe
    df = fp_obj.to_pandas()
    print(df)


if __name__ == '__main__':
    # start_spark_session()
    # sf_schema()
    # dataframe_init()
    read_aws_s3()
