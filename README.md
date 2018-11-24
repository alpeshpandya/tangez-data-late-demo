# Tangez Datalake Demo

## Usage
* `chmod +x run_demo.sh`
* `./run_demo.sh`

## Airflow
* Airflow admin: http://localhost:8080
* Demo workflow: http://localhost:8080/admin/airflow/graph?execution_date=&dag_id=A-tangenz-demo-dag

## Jupyter
* Check the last line of run_demo.sh command to get url for Jupyter
* Open kafkaSendDataPy.ipynb and run all cells.
* Open kafkaReceiveAndSaveToCassandraPy.ipynb and run cells up to start streaming. Check in subsequent cells that Cassandra collects data properly.
* Connect to Spark UI. It is available in your browser at localhost:4040