from pyspark.sql.functions import explode, col

class FireIncidentsDataHandling:
    def __init__(self, spark, days_to_fetch = -1):
        self.spark = spark
        self.days_to_fetch = days_to_fetch

        self.__DATABASE = "fire_incidents"
        self.__TABLE_NAME = "fire_incidents"
        self.__PRIMARY_KEY = "id"
        self.__YEAR_COLUMN = "year"
        self.__MONTH_COLUMN = "month"
        self.__DAY_COLUMN = "day"
        
    
    def __read_raw_data(self):
        if self.days_to_fetch > -1:
            return self.spark.sql(f"""
                                    select *
                                    from {self.__DATABASE}_raw.{self.__TABLE_NAME}
                                    where
                                        to_date(concat(year, '-', month, '-', day))
                                        between date_sub(now(), {self.days_to_fetch}) and date_sub(now(), 0)""")
        else:
            return self.spark.sql(f"select * from {self.__DATABASE}_raw.{self.__TABLE_NAME}")


    def __read_silver_data(self):
        return self.spark.sql("select * from {self.__DATABASE}.{self.__TABLE_NAME}")


    def __merge_upsert(self, source_dataframe, target_dataframe):
        target_dataframe.alias("old").merge(
            source_dataframe.alias("new"),
            f"old.{self.__PRIMARY_KEY} = new.{self.__PRIMARY_KEY}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


    def __save_as_table(self, dataframe):
        dataframe.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy(self.__YEAR_COLUMN, self.__MONTH_COLUMN, self.__DAY_COLUMN) \
            .saveAsTable(f"{self.__DATABASE}.{self.__TABLE_NAME}")


    def handle_data(self):
        raw_layer_dataframe = self.__read_raw_data().select("*", "point.*").drop("point")
        if self.spark._jsparkSession.catalog().tableExists(self.__DATABASE, self.__TABLE_NAME):
            silver_layer_dataframe = self.__read_silver_data()
            self.__merge_upsert(raw_layer_dataframe, silver_layer_dataframe)
            return
        self.__save_as_table(raw_layer_dataframe)
