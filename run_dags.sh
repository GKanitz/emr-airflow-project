#!/bin/bash

airflow trigger_dag dag_cluster
airflow trigger_dag dag_normalize
airflow trigger_dag dag_analytics
