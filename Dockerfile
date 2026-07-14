FROM apache/spark:3.5.1

USER root

RUN pip3 install --no-cache-dir \
    kafka-python \
    psycopg2-binary

WORKDIR /opt/spark

USER root