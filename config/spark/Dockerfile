# Spark with the medallion's dependency tree already resolved.
#
# The three Spark jobs submit with --packages, which resolves Iceberg, the AWS bundle,
# hadoop-aws and the Kafka connector from Maven Central into spark.jars.ivy=/tmp/.ivy2.
# That directory is pod-local, so under Kubernetes each of the three Jobs re-downloads
# roughly 100MB on every run -- inside the cluster, where actions/cache cannot reach.
#
# Mounting a hostPath there does not work: kubelet creates a DirectoryOrCreate mount as
# root:root 0755 while the Job runs as the image's UID 185, and Ivy dies with
# "FileNotFoundException: /tmp/.ivy2/cache/resolved-...xml". Compose only gets away with
# the equivalent because its spark service sets user: "0:0".
#
# So the cache is baked in instead. Resolution happens once at build time, into the exact
# path the jobs already use, owned by the runtime user. The submit arguments are unchanged
# -- --packages still appears, and still resolves the same tree; it simply finds every
# artifact locally. That matters: the arguments are asserted byte-for-byte against
# airflow/dags/spark_jobs.py and k8s/base/spark.yaml by tests/test_spark_jobs.py, and
# dropping --packages in favour of dumping jars into $SPARK_HOME/jars would also lose the
# transitive dependency resolution that --packages performs.
FROM spark:3.5.8-python3

USER root

# Must match ICEBERG_PACKAGE + KAFKA_PACKAGE in airflow/dags/spark_jobs.py. Asserted by
# tests/test_spark_jobs.py, because warming the wrong coordinates would silently fall back
# to downloading at runtime -- correct, but slow in exactly the way this image exists to fix.
ARG SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4"

# spark-submit resolves --packages before it runs the application, so a no-op script is
# enough to populate the cache. local[1] keeps the warm-up JVM small.
RUN printf 'pass\n' > /tmp/warmup.py \
    && /opt/spark/bin/spark-submit \
         --master "local[1]" \
         --conf spark.jars.ivy=/tmp/.ivy2 \
         --packages "${SPARK_PACKAGES}" \
         /tmp/warmup.py \
    && rm /tmp/warmup.py \
    && chown -R 185:185 /tmp/.ivy2

USER 185
