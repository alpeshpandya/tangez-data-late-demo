#!/usr/bin/env bash

export AIRFLOW_GPL_UNIDECODE=yes
yum install gcc -y
pip install apache-airflow
pip uninstall psutil -y
pip install psutil
airflow initdb