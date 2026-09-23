"""
Example DAG demonstrating Secret() patterns for KubernetesPodOperator,
based on the official Airflow KPO example:
https://github.com/apache/airflow/blob/main/providers/cncf/kubernetes/tests/system/cncf/kubernetes/example_kubernetes.py
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.configuration import conf

NAMESPACE = conf.get("kubernetes_executor", "namespace")

DAGS_SECRET_NAME = "dags-secret"
DAGS_SECRET_MOUNT_PATH = "/var/airflow/secrets"
IMAGE = "paas.registry.smk.sbercloud.dev/airflow:3.0.3-Certs-dp-products"

# Pattern 1 — mount all keys as files under DAGS_SECRET_MOUNT_PATH/ in one declaration.
# File paths: /var/airflow/secrets/bool_type, /var/airflow/secrets/int_type, etc.
# ! Do NOT pass individual Secret("volume", same_path, ..., key=X) objects for each key —
# Kubernetes rejects pods where multiple volumeMounts share the same mountPath.
secret_all_files = Secret("volume", DAGS_SECRET_MOUNT_PATH, DAGS_SECRET_NAME)

# Pattern 2 — inject each key as a named environment variable.
secret_bool_env = Secret("env", "BOOL_TYPE", DAGS_SECRET_NAME, "bool_type")
secret_int_env = Secret("env", "INT_TYPE", DAGS_SECRET_NAME, "int_type")
secret_key_env = Secret("env", "SECRET_KEY", DAGS_SECRET_NAME, "secret_key")

# Pattern 3 — inject all keys as environment variables in one declaration.
# Each JSON key becomes an env var with the same name.
secret_all_env = Secret("env", None, DAGS_SECRET_NAME)

_common_kpo = dict(
    namespace=NAMESPACE,
    in_cluster=True,
    image=IMAGE,
    image_pull_secrets=[{"name": "cloud-registry-secret"}],
    get_logs=True,
    is_delete_operator_pod=True,
    service_account_name="airflow-worker",
    security_context={"runAsUser": 50000, "runAsGroup": 0, "fsGroup": 0},
    container_security_context={
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": False,
        "runAsNonRoot": True,
    },
)

with DAG(
    dag_id="dags_secret_kpo_example_1",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["secrets", "kpo", "example"],
) as dag:

    # Pattern 1: read keys from mounted files, print value and its type.
    kpo_file = KubernetesPodOperator(
        task_id="read_secrets_as_files",
        name="secret-file-pod",
        cmds=["bash", "-cx"],
        arguments=[
            f"""
            for key in bool_type int_type secret_key; do
                val=$(cat {DAGS_SECRET_MOUNT_PATH}/$key)
                echo "$key: value=$val"
            done
            """
        ],
        secrets=[secret_all_files],
        **_common_kpo,
    )

    # Pattern 2: read keys from named environment variables, print name and type.
    kpo_env = KubernetesPodOperator(
        task_id="read_secrets_as_named_env",
        name="secret-named-env-pod",
        cmds=["bash", "-cx"],
        # * Print only that vars are set, not their values.
        arguments=[
            """
            for var in BOOL_TYPE INT_TYPE SECRET_KEY; do
                echo "$var is set: ${!var:+yes}"
            done
            """
        ],
        secrets=[secret_bool_env, secret_int_env, secret_key_env],
        **_common_kpo,
    )

    # Pattern 3: all keys injected at once — env var names match JSON key names.
    kpo_all_env = KubernetesPodOperator(
        task_id="read_all_secrets_as_env",
        name="secret-all-env-pod",
        cmds=["bash", "-cx"],
        arguments=[
            """
            for var in bool_type int_type secret_key; do
                echo "$var is set: ${!var:+yes}"
            done
            """
        ],
        secrets=[secret_all_env],
        **_common_kpo,
    )

    kpo_file >> kpo_env >> kpo_all_env
