#!/bin/bash

service sshd start
service cassandra start

su guest $HOME/kafka/bin/zookeeper-server-start.sh $HOME/kafka/config/zookeeper.properties  > /home/guest/zookeeper.log 2>&1 &
su guest $HOME/kafka/bin/kafka-server-start.sh $HOME/kafka/config/server.properties > /home/guest/kafka.log 2>&1 &

sleep 15
cqlsh -f init_cassandra.cql

airflow scheduler -D
airflow webserver -D

rm -rf /home/guest/.local
su - guest -c '/home/guest/anaconda2/bin/jupyter notebook --ip='\''*'\'''