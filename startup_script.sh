#!/bin/bash

service sshd start
chmod -R 777 /var/lib/cassandra
chmod -R 777 /var/log/cassandra
su guest cassandra &

su guest $HOME/kafka/bin/zookeeper-server-start.sh $HOME/kafka/config/zookeeper.properties  > /home/guest/zookeeper.log 2>&1 &
su guest $HOME/kafka/bin/kafka-server-start.sh $HOME/kafka/config/server.properties > /home/guest/kafka.log 2>&1 &

sleep 15
cqlsh -f init_cassandra.cql

#start spark cluster
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-slave.sh spark://localhost:7077

airflow scheduler -D
airflow webserver -D

rm -rf /home/guest/.local
su - guest -c "export PYSPARK_DRIVER_PYTHON=/home/guest/anaconda2/bin/jupyter && export PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip='\''*'\''' && pyspark --master spark://localhost:7077"