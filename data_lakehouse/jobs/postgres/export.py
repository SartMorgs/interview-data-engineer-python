class ExportDataToPostgres:
    def __init__(self, spark, host, port, database, user, password):
        self.connection_string = f"jdbc:postgresql://{host}:{port}/{database}"
        self.spark = spark

        self.__USER = user
        self.__PASSWORD = password

        self.__SOURCE_DATABASE = "fire_incidents"
        self.__TARGET_SCHEMA = "public"
        self.__TABLE_NAME = "fire_incidents"


    def __read_data(self):
        return self.spark.sql(f"select * from {self.__SOURCE_DATABASE}.{self.__TABLE_NAME}")


    def export_data(self):
        dataframe = self.__read_data()
        dataframe.write \
            .format("jdbc") \
            .option("url", self.connection_string) \
            .option("driver", "org.postgresql.Driver").option("dbtable", f"{self.__TARGET_SCHEMA}.{self.__TABLE_NAME}") \
            .option("user", self.__USER).option("password", self.__PASSWORD) \
            .mode("overwrite") \
            .save()
