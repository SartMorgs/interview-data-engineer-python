import shutil
import os

from pathlib import Path
from dotenv import load_dotenv
from jobs.raw_layer.fire_incident_ingestion import FireIncidentIngestion
from jobs.silver_layer.fire_incident_data_handling import FireIncidentsDataHandling
from jobs.postgres.export import ExportDataToPostgres
from jobs.utils.spark import init_spark


class FireIncidentDataFlow:
    def __init__(self):
        load_dotenv()
        self.spark = init_spark("Fire Incident Ingestion Job")

        self.__POSTGRES_HOST = os.getenv("POSTGRES_HOST")
        self.__POSTGRES_PORT = os.getenv("POSTGRES_PORT")
        self.__POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
        self.__POSTGRES_USER = os.getenv("POSTGRES_USER")
        self.__POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

    
    def main(self):
        # DATABASE CREATION
        self.spark.sql("create database if not exists fire_incidents_raw")
        self.spark.sql("create database if not exists fire_incidents")
        
        # CLEAN TABLES (TEST)
        if Path(f"{Path(__file__).parents[1]}/share").is_dir():
            shutil.rmtree(f"{Path(__file__).parents[1]}/share")
        if Path(f"{Path(__file__).parents[1]}/spark-warehouse").is_dir():
            shutil.rmtree(f"{Path(__file__).parents[1]}/spark-warehouse")
        self.spark.sql("drop table if exists fire_incidents_raw.fire_incidents")
        
        # INGEST DATA
        fire_incident_ingestion = FireIncidentIngestion(self.spark, '2019-09-12')
        fire_incident_ingestion.ingest_data()
        
        # HANDLING DATA
        fire_incident_data_handling = FireIncidentsDataHandling(self.spark)
        fire_incident_data_handling.handle_data()

        
        # READ TABLE
        #raw_dataframe = self.spark.sql("select * from fire_incidents_raw.fire_incidents limit 15")
        #raw_dataframe.show(truncate=False)

        silver_dataframe = self.spark.sql("select * from fire_incidents.fire_incidents limit 15")
        silver_dataframe.printSchema()

        # EXPORT TABLE
        export_data_to_postgres = ExportDataToPostgres(
            self.spark,
            self.__POSTGRES_HOST,
            self.__POSTGRES_PORT,
            self.__POSTGRES_DB_NAME,
            self.__POSTGRES_USER,
            self.__POSTGRES_PASSWORD
        )
        export_data_to_postgres.export_data()
        
        self.spark.stop()
        

if __name__ == "__main__":
    FireIncidentDataFlow().main()