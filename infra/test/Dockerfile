# Fully-containerized test runner — the host needs ONLY Docker.
# Reuses the Spark image (Java 17 + Spark 4.0.1 + baked Delta/s3a jars) and adds the test
# tooling. Spark tests build a local SparkSession against the BAKED jars (DELTA_VIA_PACKAGES=0),
# so no network is required at test time.
FROM ifood-case/spark:4.0.1

USER root
RUN pip install --no-cache-dir "pytest>=8,<9" "duckdb>=1.0" "pandas>=2,<3"

ENV PYTHONPATH=/opt/app:${PYTHONPATH}
ENV DELTA_VIA_PACKAGES=0
WORKDIR /opt/app
USER spark
