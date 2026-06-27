# Dagster UI image: reuses the Spark image (so assets run PySpark in-process against the
# baked Delta/s3a jars) and adds Dagster. Build after the spark image exists (make up).
FROM ifood-case/spark:4.0.1

USER root
COPY orchestration/requirements.txt /tmp/dagster-req.txt
RUN pip install --no-cache-dir -r /tmp/dagster-req.txt
RUN mkdir -p /opt/dagster/home && chown -R spark /opt/dagster

ENV DAGSTER_HOME=/opt/dagster/home
ENV PYTHONPATH=/opt/app:${PYTHONPATH}
WORKDIR /opt/app
USER spark
