FROM bitnami/spark:3.5

USER root
ENV PIP_ROOT_USER_ACTION=ignore

# Python + pip + curl
RUN apt-get update \
 && apt-get install -y python3 python3-pip curl \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && rm -rf /var/lib/apt/lists/*

# Python packages for Spark jobs
COPY requirements.txt requirements.txt
RUN python3 -m pip install --upgrade pip setuptools wheel \
 && python3 -m pip install --no-cache-dir -r requirements.txt

# Bundle Iceberg / Nessie / S3 JARs
RUN set -eux; \
    mkdir -p /opt/iceberg; \
    curl -fSL -o /opt/iceberg/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar; \
    curl -fSL -o /opt/iceberg/iceberg-nessie-1.6.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-nessie/1.6.1/iceberg-nessie-1.6.1.jar; \
    curl -fSL -o /opt/iceberg/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar; \
    curl -fSL -o /opt/iceberg/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar; \
    chmod 0644 /opt/iceberg/*.jar; \
    cp -f /opt/iceberg/*.jar /opt/bitnami/spark/jars/

# Create spark user home and caches
RUN echo "spark:x:1001:1001::/home/spark:/bin/bash" >> /etc/passwd \
 && echo "spark:x:1001:" >> /etc/group \
 && mkdir -p /home/spark/.ivy2 /tmp/.ivy2 \
 && chown -R 1001:0 /home/spark /tmp/.ivy2 /opt/iceberg /opt/bitnami/spark/jars

USER 1001
RUN python3 -m pip install --user --no-cache-dir -r requirements.txt

ENV HOME=/home/spark \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/spark_jobs:/opt

USER 1001

