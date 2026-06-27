# Spark 4.0.1 / Scala 2.13 / Java 17 image with Delta + s3a jars BAKED IN, so
# containers never resolve packages from Maven Central at runtime (works offline).
#
# If this exact base tag is unavailable for your platform, override with:
#   docker build --build-arg SPARK_IMAGE=apache/spark:4.0.1 ...
# (verify tags at https://hub.docker.com/r/apache/spark/tags)
ARG SPARK_IMAGE=apache/spark:4.0.1-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root

# Exact, version-locked jars (from the adversarially-validated matrix).
ARG DELTA_VERSION=4.0.0
ARG HADOOP_AWS_VERSION=3.4.1
ARG AWS_SDK_VERSION=2.24.6
ARG MAVEN=https://repo1.maven.org/maven2

# Download jars with the image's python (no apt-get layer — faster, smaller build).
RUN set -eux; cd "${SPARK_HOME}/jars"; \
    for url in \
      "${MAVEN}/io/delta/delta-spark_2.13/${DELTA_VERSION}/delta-spark_2.13-${DELTA_VERSION}.jar" \
      "${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
      "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "${MAVEN}/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar"; do \
        python3 -c "import sys,urllib.request; u=sys.argv[1]; urllib.request.urlretrieve(u, u.rsplit('/',1)[1])" "$url"; \
      done

# Minimal Python deps for ingestion (Spark/PySpark/Delta come from the base image).
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Make pyspark + py4j importable from a PLAIN python process. The base image exposes them
# only to the spark-submit/pyspark launchers, NOT on PYTHONPATH — but `make analyze`, the
# Dagster assets and the containerized pytest import pyspark without spark-submit. A .pth in
# site-packages adds both (py4j zip version resolved by glob, no hardcoding).
RUN SITE="$(python3 -c 'import site; print(site.getsitepackages()[0])')" \
    && printf '%s\n' "${SPARK_HOME}/python" > "${SITE}/pyspark.pth" \
    && ls "${SPARK_HOME}"/python/lib/py4j-*-src.zip >> "${SITE}/pyspark.pth"

# Pre-install DuckDB extensions (httpfs + delta) into a shared dir so the default DuckDB SQL
# consumption (`make analyze`) works OFFLINE — no extension CDN at runtime, consistent with the
# baked Spark/Delta jars. run_questions.py LOADs from DUCKDB_EXTENSION_DIRECTORY.
ENV DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb-ext
RUN mkdir -p /opt/duckdb-ext \
    && python3 -c "import duckdb; duckdb.connect(config={'extension_directory':'/opt/duckdb-ext'}).execute('INSTALL httpfs; INSTALL delta;')" \
    && chmod -R a+rX /opt/duckdb-ext

# Default Spark configs (s3a -> MinIO + Delta extensions).
COPY infra/spark/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# /opt/app on PYTHONPATH so the src/analysis/orchestration packages import.
ENV PYTHONPATH=/opt/app:${PYTHONPATH}
# `docker compose exec` bypasses the login shell, so it does NOT inherit the base image's
# entrypoint PATH — only the Docker ENV PATH (java + system dirs). Add Spark's bin so
# `spark-submit` (bronze/silver/gold/eda) resolves under `make` exec calls.
ENV PATH=${SPARK_HOME}/bin:${PATH}
WORKDIR /opt/app
USER spark
