from airflow import DAG
import os
import kubernetes
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

COMPONENT_IMAGE_VERSION = os.getenv('COMPONENT_IMAGE_VERSION')
NAMESPACE = 'taisp-ml-infra'


default_args = {
    'owner': 'taisp',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 7,
    'retry_delay': timedelta(minutes=5),
    'namespace': NAMESPACE,
    'in_cluster': True,  # if set to true, will look in the cluster, if false, looks for file
    'get_logs': True,
    'is_delete_operator_pod': True
}

dag = DAG('FL5G',
          default_args=default_args,
          description='Federated Learning 5G Dag',
          schedule=None,
          start_date=datetime(2017, 3, 20),
          catchup=False)


fl_server_service = k8s.V1Service(
        metadata=k8s.V1ObjectMeta(
            name="aggregator"
        ),
        spec=k8s.V1ServiceSpec(
            selector={"task_id": "fl_server"},
            ports=[k8s.V1ServicePort(
                port=51000
            )]
        )
    )

core_v1_api = kubernetes.client.CoreV1Api()

try:
    core_v1_api.create_namespaced_service(body=fl_server_service, namespace=NAMESPACE)
except Exception as e:
    pass

volume0 = k8s.V1Volume(
    name="pm-data-client0",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="pm5g-client0-pvc"),
)

volume1 = k8s.V1Volume(
    name="pm-data-client1",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="pm5g-client1-pvc"),
)


ingest_data = KubernetesPodOperator(
            image=f"hoangtuansu/fl5g_ingester:0.1",
            env_vars=[
                k8s.V1EnvVar(name='DATA_PATH', value='/opt/oran/shared')],
            image_pull_policy="Always",
            name=f"ingest_data",
            task_id=f"ingest_data",
            cmds=["bash", "-c"],
            arguments=["cp /opt/oran/*.csv /mnt/client0 && cp /opt/oran/*.csv /mnt/client1 && sleep 10"],
            on_finish_action='keep_pod',
            volumes=[volume0, volume1],
            volume_mounts=[
                k8s.V1VolumeMount(mount_path="/mnt/client0", name="pm-data-client0", sub_path=None, read_only=False),
                k8s.V1VolumeMount(mount_path="/mnt/client1", name="pm-data-client1", sub_path=None, read_only=False)
                ],
            dag=dag,
        )


fl_server = KubernetesPodOperator(
            image=f"hoangtuansu/fl_server:0.1",
            env_vars=[
                k8s.V1EnvVar(name='DATA_PATH', value='/opt/oran/shared')],
            image_pull_policy="Always",
            name=f"fl_server",
            task_id=f"fl_server",
            cmds=["bash", "-c"],
            arguments=["python main.py"],
            on_finish_action='keep_pod',
            dag=dag,
        )

fl_client1 = KubernetesPodOperator(
            image=f"hoangtuansu/fl_client:0.1",
            env_vars=[
                k8s.V1EnvVar(name='DATA_PATH', value='/opt/oran/shared')],
            image_pull_policy="Always",
            name=f"fl_client1",
            task_id=f"fl_client1",
            cmds=["bash", "-c"],
            arguments=["python main.py -cid 1 -sa aggregator.taisp-ml-infra.svc:51000"],
            on_finish_action='keep_pod',
            volumes=[volume0],
            volume_mounts=[
                k8s.V1VolumeMount(mount_path="/opt/oran/shared", name="pm-data-client0", sub_path=None, read_only=False),
            ],
            dag=dag,
        )

fl_client0 = KubernetesPodOperator(
            image=f"hoangtuansu/fl_client:0.1",
            env_vars=[
                k8s.V1EnvVar(name='DATA_PATH', value='/opt/oran/shared')],
            image_pull_policy="Always",
            name=f"fl_client0",
            task_id=f"fl_client0",
            cmds=["bash", "-c"],
            arguments=["python main.py -cid 0 -sa aggregator.taisp-ml-infra.svc:51000"],
            on_finish_action='keep_pod',
            volumes=[volume1],
            volume_mounts=[
                k8s.V1VolumeMount(mount_path="/opt/oran/shared", name="pm-data-client1", sub_path=None, read_only=False),
            ],
            dag=dag,
        )

'''

clean_data = KubernetesPodOperator(
            image=f"hoangtuansu/cleaner:{COMPONENT_IMAGE_VERSION}",
            env_vars=[
                    k8s.V1EnvVar(name='COLLECTED_TABLE', value='RawPM'),
                    k8s.V1EnvVar(name='CLEAN_TRAIN_DATA_TABLE', value='TrainPM'),
                    k8s.V1EnvVar(name='CLEAN_TEST_DATA_TABLE', value='TestPM'),
                    k8s.V1EnvVar(name='CLEAN_ACTUAL_DATA_TABLE', value='ActualPM'), 
                    k8s.V1EnvVar(name='SHARED_PATH', value='/opt/oran/shared')],
            name=f"clean_data",
            image_pull_policy="Always",
            task_id=f"clean_data",
            cmds=["bash", "-c"],
            arguments=["python clean_data.py"],
            on_finish_action='keep_pod',
            dag=dag,
        )

train_data = KubernetesPodOperator(
            image=f"hoangtuansu/trainer:{COMPONENT_IMAGE_VERSION}",
            env_vars=[
                    k8s.V1EnvVar(name='CLEAN_TRAIN_DATA_TABLE', value='TrainPM'),
                    k8s.V1EnvVar(name='CLEAN_TEST_DATA_TABLE', value='TestPM'),
                    k8s.V1EnvVar(name='CLEAN_ACTUAL_DATA_TABLE', value='ActualPM'),
                    k8s.V1EnvVar(name='MLFLOW_TRACKING_URI', value='http://10.180.113.115:32256'),
                    k8s.V1EnvVar(name='MLFLOW_TRACKING_USERNAME', value='user'),
                    k8s.V1EnvVar(name='MLFLOW_TRACKING_PASSWORD', value='sr9TvkIjaj'), 
                    k8s.V1EnvVar(name='SHARED_PATH', value='/opt/oran/shared')],
            name=f"train_data",
            task_id=f"train_data",
            image_pull_policy="Always",
            cmds=["bash", "-c"],
            arguments=["python model_train.py"],
            on_finish_action='keep_pod',
            dag=dag,
        )

'''

ingest_data >> [fl_server, fl_client0, fl_client1]