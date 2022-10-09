import yaml
from airflow import DAG
from tasks import create_task_collector, create_task_prep
import os
import pendulum


def create_dag(dag_id, config, scheduler):
    dag = DAG(
        dag_id,
        start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
        schedule=scheduler,
        catchup=False,
    )

    with dag:
        (
            create_task_prep(config=config, output_dir=dag_id.upper())
            >> create_task_collector(config=config, output_dir=dag_id.upper())
        )

    return dag


my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, "config.yaml")

with open(configuration_file_path) as file:
    configs = yaml.safe_load(file)

    for robot in configs["robots"]:
        globals()[robot] = create_dag(
            dag_id=robot, config=configs["robots"][robot], scheduler=configs["robots"][robot]["schedule"]
        )
