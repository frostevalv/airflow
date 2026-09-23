"""
Test DAG for Kubernetes Executor Quota Configuration

This DAG creates 10 tasks that should run on the KubernetesExecutor.
With the quota set to 5 pods, you should observe:
- Maximum of 5 pods running concurrently
- Remaining 5 tasks queued until pods complete
- No more than 5 pods at any given time

Usage:
1. Upload this DAG to your Airflow dags folder
2. Trigger it manually from the Airflow UI
3. Watch the pods: kubectl get pods -n <namespace> -l component=worker -w
4. Check quota usage: kubectl describe resourcequota <release>-k8s-executor-quota -n <namespace>
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import time
import socket


def test_task(task_number):
    """
    Simple test function that prints information about the pod it's running in.
    Sleeps for 30 seconds to allow observation of the quota in action.
    """
    hostname = socket.gethostname()
    print(f"=" * 80)
    print(f"Task {task_number} starting")
    print(f"Running on pod: {hostname}")
    print(f"Timestamp: {datetime.now()}")
    print(f"=" * 80)

    # Sleep to allow observation of concurrent pod limits
    print(f"Task {task_number} will sleep for 30 seconds...")
    time.sleep(30)

    print(f"Task {task_number} completed successfully!")
    print(f"=" * 80)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
        'test_k8s_executor_quota',
        default_args=default_args,
        description='Test DAG to verify Kubernetes Executor quota configuration',
        schedule="@once",
        catchup=False,
        tags=['test', 'kubernetes', 'quota'],
) as dag:
    # Create 10 tasks - should see max 5 running concurrently
    tasks = []
    for i in range(15):
        task = PythonOperator(
            task_id=f'test_task_{i:02d}',
            python_callable=test_task,
            op_kwargs={'task_number': i + 1},
        )
        tasks.append(task)

    # No dependencies - all tasks should try to run in parallel
    # The quota will limit concurrent execution to 5 pods

