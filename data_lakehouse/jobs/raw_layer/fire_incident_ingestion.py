import requests
from pyspark.sql.functions import year, month, day, col


class FireIncidentIngestion:
    def __init__(self, spark, reference_date):
        self.spark = spark
        self.__URL = f"https://data.sfgov.org/resource/wr8u-xric.json?$where=incident_date>=%27{reference_date}%27"
        self.sc = self.spark.sparkContext
        
        self.__DATABASE = "fire_incidents_raw"
        self.__TABLE_NAME = "fire_incidents"
        self.__PARTITION_DATE_COLUMN = "data_as_of"
        self.__TIMESTAMP_STRING = "timestamp"
        self.__YEAR_COLUMN = "year"
        self.__MONTH_COLUMN = "month"
        self.__DAY_COLUMN = "day"

    def __get_data(self):
        response = requests.get(self.__URL)
        return response.json()


    def __add_partition_column(self, dataframe):
        return dataframe \
            .withColumn(self.__YEAR_COLUMN, year(col(self.__PARTITION_DATE_COLUMN).cast(self.__TIMESTAMP_STRING))) \
            .withColumn(self.__MONTH_COLUMN, month(col(self.__PARTITION_DATE_COLUMN).cast(self.__TIMESTAMP_STRING))) \
            .withColumn(self.__DAY_COLUMN, day(col(self.__PARTITION_DATE_COLUMN).cast(self.__TIMESTAMP_STRING)))


    def ingest_data(self):
        str_json_data = self.__get_data()
        dataframe = self.spark.read.json(self.sc.parallelize([str_json_data]))
        dataframe = self.__add_partition_column(dataframe)
        dataframe \
            .write \
            .format("delta") \
            .mode("append") \
            .partitionBy(self.__YEAR_COLUMN, self.__MONTH_COLUMN, self.__DAY_COLUMN) \
            .saveAsTable(f"{self.__DATABASE}.{self.__TABLE_NAME}")
