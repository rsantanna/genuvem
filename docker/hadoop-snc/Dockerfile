FROM ubuntu:20.04
LABEL maintainer="gomes.rsantanna@gmail.com"

# Build Arguments
ARG HADOOP_VERSION
ARG SPARK_VERSION
ARG ZEPPELIN_VERSION
ARG SCALA_VERSION
ARG GOOGLE_APPLICATION_CREDENTIALS

# Environment Setup
ENV HOME             /root
ENV GENOOGLE_HOME    /app/genoogle
ENV JAVA_HOME        /usr/lib/jvm/java-8-openjdk-amd64
ENV SCALA_HOME       /usr/local/scala
ENV HADOOP_HOME      /usr/local/hadoop
ENV SPARK_HOME       /usr/local/spark
ENV ZEPPELIN_HOME    /usr/local/zeppelin
ENV DEBIAN_FRONTEND  noninteractive

# Path
ENV PATH "${SCALA_HOME}/bin:${SPARK_HOME}/bin:${HADOOP_HOME}/bin:${ZEPPELIN_HOME}/bin:${PATH}"

# Install necessary packages
RUN apt-get update -y && \
    apt-get install -y ssh wget curl git nano openjdk-8-jdk-headless openjdk-8-jre-headless ant r-base python3-pip python-is-python3 && \
    apt-get clean autoclean && \
    rm -rf /var/lib/apt/lists/*

# Download and Extract Hadoop
WORKDIR ${HADOOP_HOME}
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar --strip-components=1 -zxvf ./hadoop-${HADOOP_VERSION}.tar.gz && \
    rm ./hadoop-${HADOOP_VERSION}.tar.gz && \
    cd ./share/hadoop/common/lib && \
    wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar 

# Download and Extract Spark
WORKDIR ${SPARK_HOME}
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz && \
    tar --strip-components=1 -zxvf ./spark-${SPARK_VERSION}-bin-without-hadoop.tgz && \
    rm ./spark-${SPARK_VERSION}-bin-without-hadoop.tgz

# Download and Extract Zeppelin
WORKDIR ${ZEPPELIN_HOME}
RUN wget https://dlcdn.apache.org/zeppelin/zeppelin-${ZEPPELIN_VERSION}/zeppelin-${ZEPPELIN_VERSION}-bin-netinst.tgz && \
    tar --strip-components=1 -zxvf ./zeppelin-${ZEPPELIN_VERSION}-bin-netinst.tgz && \
    rm ./zeppelin-${ZEPPELIN_VERSION}-bin-netinst.tgz && \
    rm -r notebook

# Download and Extract Scala
WORKDIR ${SCALA_HOME}
RUN wget https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz && \
    tar --strip-components=1 -zxvf ./scala-${SCALA_VERSION}.tgz && \
    rm ./scala-${SCALA_VERSION}.tgz

# Build Genoogle and remove default configuration and example files
WORKDIR ${GENOOGLE_HOME}
RUN git clone https://github.com/felipealbrecht/Genoogle.git . && \
    ant jar && \
    mv ./ant-build/genoogle.jar . && \
    rm -r ./conf && \
    rm -r ./files

# Install Python packages
RUN pip install --no-cache-dir jupyter pandas grpcio protobuf matplotlib bokeh holoviews hvplot altair plotly \
      seaborn plotnine

# Generate SSH Keys
RUN ssh-keygen -q -t rsa -N "" -f ${HOME}/.ssh/id_rsa && cat ${HOME}/.ssh/id_rsa.pub > ${HOME}/.ssh/authorized_keys2

# Copy Hadoop Configurations
COPY ./conf/core-site.xml ${HADOOP_HOME}/etc/hadoop/core-site.xml
COPY ./conf/hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
COPY ./conf/yarn-site.xml ${HADOOP_HOME}/etc/hadoop/yarn-site.xml
COPY ./conf/mapred-site.xml ${HADOOP_HOME}/etc/hadoop/mapred-site.xml

# hadoop-env.sh
RUN echo "export JAVA_HOME=\"${JAVA_HOME}\"" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_NAMENODE_USER=root" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_NAMENODE_GROUP=root" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_DATANODE_USER=root" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_SECONDARYNAMENODE_USER=root" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "export GOOGLE_APPLICATION_CREDENTIALS=\"${GOOGLE_APPLICATION_CREDENTIALS}\"" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "export YARN_NODEMANAGER_USER=root" >> ${HADOOP_HOME}/etc/hadoop/yarn-env.sh && \
    echo "export YARN_RESOURCEMANAGER_USER=root" >> ${HADOOP_HOME}/etc/hadoop/yarn-env.sh

# spark-env.sh
RUN echo "export HADOOP_HOME=\"${HADOOP_HOME}/etc/hadoop\"" >> ${SPARK_HOME}/conf/spark-env.sh && \
    echo "export HADOOP_CONF_DIR=\"${HADOOP_HOME}/etc/hadoop\"" >> ${SPARK_HOME}/conf/spark-env.sh && \
    echo "export SPARK_DIST_CLASSPATH=\"$(hadoop classpath)\"" >> ${SPARK_HOME}/conf/spark-env.sh && \
    echo "export GOOGLE_APPLICATION_CREDENTIALS=\"${GOOGLE_APPLICATION_CREDENTIALS}\"" >> ${SPARK_HOME}/conf/spark-env.sh

# spark-defaults.sh
RUN echo "spark.master=yarn" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.submit.deployMode=client" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.gs.project.id=" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.hadoop.google.cloud.auth.service.account.enable=true" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.hadoop.google.cloud.auth.service.account.json.keyfile=${GOOGLE_APPLICATION_CREDENTIALS}" >> ${SPARK_HOME}/conf/spark-defaults.conf

# zeppelin-env.sh
RUN echo "export JAVA_HOME=\"${JAVA_HOME}\"" >> ${ZEPPELIN_HOME}/conf/zeppelin-env.sh && \
    echo "export ZEPPELIN_ADDR=0.0.0.0" >> ${ZEPPELIN_HOME}/conf/zeppelin-env.sh && \
    echo "export HADOOP_CONF_DIR=\"${HADOOP_HOME}/etc/hadoop\"" >> ${ZEPPELIN_HOME}/conf/zeppelin-env.sh && \
    echo "export USE_HADOOP=true" >> ${ZEPPELIN_HOME}/conf/zeppelin-env.sh && \
    echo "export SPARK_MASTER=yarn" >> ${ZEPPELIN_HOME}/conf/zeppelin-env.sh && \
    echo "export SPARK_HOME=\"${SPARK_HOME}\""  >> ${ZEPPELIN_HOME}/conf/zeppelin-env.sh && \
    echo "export GOOGLE_APPLICATION_CREDENTIALS=\"${GOOGLE_APPLICATION_CREDENTIALS}\"" >> ${ZEPPELIN_HOME}/conf/zeppelin-env.sh
    
# Format Namenode
RUN ${HADOOP_HOME}/bin/hdfs namenode -format

# Create Init Scripts
WORKDIR ${HOME}

RUN echo "#!/bin/sh" >> start_cluster.sh && \
    echo "/etc/init.d/ssh start"  >> start_cluster.sh && \
    echo "${HADOOP_HOME}/sbin/start-dfs.sh"  >> start_cluster.sh && \
    echo "${HADOOP_HOME}/sbin/start-yarn.sh"  >> start_cluster.sh && \
    echo "${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /user/root" >> start_cluster.sh && \
    echo "${ZEPPELIN_HOME}/bin/zeppelin-daemon.sh start"  >> start_cluster.sh && \
    echo "/bin/bash" >> start_cluster.sh && \
    chmod +x ./start_cluster.sh

# Run
EXPOSE 4040 8042 8080 8088 50070 
ENTRYPOINT ["./start_cluster.sh"]
