FROM godatadriven/pyspark

ADD data_lakehouse/. /data_lakehouse/src
ADD requirements.txt /data_lakehouse
WORKDIR /data_lakehouse
RUN pip install -r requirements

ENTRYPOINT [ "python3" ]
CMD [ "data_lakehouse/src/main/fire_incident_data_flow.py" ]