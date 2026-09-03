# Spark with the medallion's dependency tree already resolved.
#
# Not primarily for speed. Measured: baking the jars took the medallion step from 2.6 to
# 2.3 minutes and added ~0.3 minutes to a cached image build, so on runner time it is
# roughly a wash. The earlier 15-20 minute medallion runs that prompted this were
# crash-loops, not downloads.
#
# The reasons that hold up:
#
# * Acceptance stops depending on Maven Central. Without this, every deployment gate
#   re-resolves four coordinates and their transitive tree from the public internet, and a
#   slow or unavailable repository fails the deploy rather than the code.
# * The Spark runtime was the last component acceptance deployed that was not pinned by
#   digest -- it pulled a mutable upstream tag while the other three images were promoted
#   by sha256. Building it closes that hole in "build once, promote the same artifact".
#
# On the mechanism: a hostPath at spark.jars.ivy does not work. kubelet creates a
# DirectoryOrCreate mount as root:root 0755 while the Job runs as the image's UID 185, so
# Ivy fails with "FileNotFoundException: /tmp/.ivy2/cache/resolved-...xml" and the Job
# crash-loops. Compose only gets away with the equivalent because its spark service sets
# user: "0:0". Baking into the image sidesteps ownership entirely.
#
# The submit arguments are deliberately unchanged -- --packages still appears and still
# resolves the same tree, it simply finds every artifact locally. Those arguments are
# asserted byte-for-byte across three runtimes by tests/test_spark_jobs.py, and dumping
# jars into $SPARK_HOME/jars instead would lose the transitive resolution --packages does.
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
