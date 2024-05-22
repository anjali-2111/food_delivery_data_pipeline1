from airflow import DAG
# from airflow.operators.sensors import S3KeySensor
# from airflow.sensors.s3_sensor import S3KeySensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime, timedelta

default_args = {
                    'owner':'airflow',
                    'depends_on_past': False,
                    'start_date': datetime(2024, 4, 1),
                    'email_on_failure': False,
                    'email_on_retry': False,
                    'retries': 1,
                    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_food_delivery_data',
    default_args=default_args,
    description='A DAG to process food delivery data using Spark on AWS EMR',
    schedule_interval=timedelta(days=1),
)

s3_sensor = S3KeySensor(
    task_id='s3_sensor',
    bucket_key='food_delivery_data/',
    wildcard_match=True,
    bucket_name='food-delivery12',
    timeout=60*60*24,  # Timeout after 24 hours
    poke_interval=600,  # Check every 10 minutes
    mode='poke',
    dag=dag,
)

step_adder = EmrAddStepsOperator(
    task_id='add_step',
    job_flow_id='j-309U67H96GDLA',
    aws_conn_id='aws_default',
    steps=[{
        'Name': 'Run PySpark Script',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                's3://food-delivery12/spark_script/',
                's3://food-delivery12/food_delivery_data/',
                's3://food-delivery12/output/'
            ],
        },
    }],
    dag=dag,
)

step_sensor = EmrStepSensor(
    task_id='step_sensor',
    job_flow_id='j-309U67H96GDLA',
    step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    poke_interval=120,  # Check every 2 minutes
    timeout=86400,  # Fail if not completed in 1 day
    mode='poke',
    dag=dag,
)

s3_sensor >> step_adder >> step_sensor

