FROM ubuntu:20.04

# Install required dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip openjdk-11-jdk && \
    pip3 install jupyterlab pandas pyspark

# Install Hadoop dependencies
RUN apt-get install -y wget tar ssh rsync

# Set environment variables for Hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Copy jars and datasets
COPY ./jars /hadoop/jars
COPY ./data /hadoop/data
COPY ./hive /hadoop/hive
COPY ./spark /hadoop/spark

# Expose ports for Jupyter and Hadoop services
EXPOSE 8888 9870 8088

# Start JupyterLab
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--NotebookApp.token=''"]
