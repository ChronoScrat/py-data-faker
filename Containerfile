# py-datafaker Containerfile
# This is a simple utility to use py-datafaker directly through a
# Container.

# py-datafaker is an utility to create fake datasets in Apache Spark
# and store them in a Hive database. It was originally developed by
# dunnhumby. This is a port of the original code to python (and pyspark)
# from Scala.

ARG registry=quay.io
ARG image=centos/centos
ARG image_tag=stream10

FROM ${registry}/${image}:${image_tag}

# General DNF config
# CentOS 10s still uses DNF4, which is quite slow to download and install
# packages. This increases some limits and enables CRB, EPEL and the plugins.
RUN dnf config-manager --save --setopt=fastestmirror=True \
    --setopt=max_parallel_downloads=10 \
    --setopt=installonly_limit=10

RUN dnf config-manager --set-enabled crb
RUN dnf install -y epel-release python3-dnf-plugins-core

# Parameters
# These control what version of OpenJDK and Apache Spark we will use.
ARG OPENJDK_VERSION="21"
ARG SPARK_VERSION="4.0.0"
ARG HADOOP_VERSION="3"
ARG PY_VERSION="3.12"

# Necessary dependencies for Spark
# We will use Java 21 (OpenJDK 21). Spark also needs py4j to work properly, so
# we must also install pip for ease of management.
# We also install pandas in the image because our utility allows the user to
# download the generated dataset into their computer. This is achieved by passing
# the data into pandas first.
RUN dnf install -y \
    java-${OPENJDK_VERSION}-openjdk-headless \
    pip

RUN pip install py4j pandas pyyaml

# Install Spark
# First, we set up a few env variables that should also be available inside of the
# container.
ENV JAVA_HOME="/etc/alternatives/jre"
ENV SPARK_HOME="/opt/spark"
ENV PATH="${PATH}:${SPARK_HOME}/bin"
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"

# Then, we properly install Apache Spark.
RUN mkdir --parents "/opt"
RUN curl --location "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o "/tmp/spark.tar.gz"
RUN tar xzf /tmp/spark.tar.gz -C /opt --owner root --group root --no-same-owner
RUN rm -f /tmp/spark.tar.gz
RUN ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}"

# Make PySpark available
# Modifying PYTHONPATH is the correct way to make a library available to
# Python, but this will also do.
RUN ln -s ${SPARK_HOME}/python/pyspark /usr/lib/python${PY_VERSION}/site-packages/pyspark

# Clean Image
RUN dnf clean all -y
RUN dnf autoremove -y
RUN rm -fr /var/cache/dnf/*
RUN rm -fr /usr/share/doc/*

# Create workspace
# This is where we will add the actual program
RUN mkdir /workspace
WORKDIR /workspace

# Add necessary files
COPY datafaker/ /workspace/datafaker/
ADD main.py LICENSE /workspace/

# Entrypoint
ENTRYPOINT [ "python3", "main.py" ]