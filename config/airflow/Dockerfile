FROM apache/airflow:2.9.3-python3.11

USER root

# docker.io is the Compose submission path: Airflow shells into the dp-spark container because
# there is no cluster to schedule against there.
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# The Kubernetes submission path: KubernetesPodOperator creates a pod per Spark job. Installed in
# both runtimes so the DAG parses identically either way -- Compose simply never takes that
# branch. Pinned to 9.0.1: it requires apache-airflow>=2.8.0, whereas the 10.x line needs 2.11+
# and would not install against the 2.9.3 base image.
RUN pip install --no-cache-dir "apache-airflow-providers-cncf-kubernetes==9.0.1"
