# data-engineer-challenge
This repository has been made for the Data Engineering challenge of an IT company. This is providing jobs to ingest and process the fire incidents data in the data lakehouse and also generate a data model. The data model will be used by the business inteligence team, whom will run queries that aggregate these incidents along the following dimensions: time period, district, and battalion

# Assumptions
For this project I am assumpting:
- The data has an API as source and this API accepts SoQL queries.
- The daily workflow would be orchestrated by some tool that could provide the parameters we need for raw and silver layer job
- The raw layer job is extracting by day, once it would be a daily ingestion and we coudl have the reference day passed by the orchestrator
- Once we had no definition about the tool or method that the reports would be generated, I used queries running in postgres warehouse

# Remarks
The main file I used to test this is just for sake of this exercise, in real life we would have the proper environment and an orchestration tool for this. The postgres write step is just for sake of dbt usage in this exercise, with the proper warehouse we wouldn't need this, postgres is not the best option.
Python version used was 3.12

# Windows config
## Install and set all environment variables
As I've been using windows for first part of this exercise, I had to set all environment variables:
```
    [System.Environment]::SetEnvironmentVariable('JAVA_HOME', <java_path>)
    [System.Environment]::SetEnvironmentVariable('HADOOP_HOME', 'C:\hadoop')
    [System.Environment]::SetEnvironmentVariable('PYSPARK_PYTHON', 'C:\python')
    [System.Environment]::SetEnvironmentVariable('SPARK_HOME', 'C:\Spark')
    [System.Environment]::SetEnvironmentVariable('SPARK_LOCAL_IP', '127.0.0.1:4040')
    $env:PATH = $env:PATH + ";" + $env:JAVA_HOME + "\bin;" + $env:SPARK_HOME + "\bin;" + $env:HADOOP_HOME + "\bin;"
```

## Install all packages
``` pip install -r requirements.txt ```

## Up Postgres Database
``` docker compose -f docker/postgres_docker_compose up -d ```

## Run main
``` python data_lakehouse/main/fire_incident_data_flow.py ```

# DBT
 To install direnv and use the variables in .envrc file 
``` 
    curl -sfL https://direnv.net/install.sh | bash 
    direnv allow
```

To run the dbt we have to go to ```dbt/fire_incidents``` directory and execute
``` dbt run ```