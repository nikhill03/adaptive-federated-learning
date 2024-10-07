SHELL=/bin/bash

all:
	export COMPONENT_IMAGE_VERSION=0.3

	export DAG_IMAGE_VERSION=0.4

	cd /home/system/workspace/vmware/afl_oran_traffic_prediction/

	docker compose build ingester fl_client fl_server dagger

	docker push hoangtuansu/ingester:0.1

	docker push hoangtuansu/fl_client:0.1

	docker push hoangtuansu/fl_server:0.1

	docker push hoangtuansu/ad_dag:0.1
  
  helm upgrade airflow apache-airflow/airflow --namespace taisp-ml-infra --set images.airflow.repository=hoangtuansu/ad_dag --set images.airflow.tag=0.1 --set images.airflow.pullPolicy=Always