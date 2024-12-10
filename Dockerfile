FROM ubuntu:20.04

# Set non-interactive mode to avoid prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary dependencies
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget ssh rsync python3 python3-pip && \
    pip3 install jupyterlab pandas pyspark

# Install Hadoop (updated to version 3.3.6)
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzvf hadoop-3.3.6.tar.gz && \
    mv hadoop-3.3.6 /usr/local/hadoop && \
    rm hadoop-3.3.6.tar.gz

# Set environment variables for Hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Configure SSH (Hadoop requires SSH to work)
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    service ssh start

# Expose necessary ports
EXPOSE 9870 8088 8888

# Start Hadoop services and JupyterLab
CMD ["/bin/bash"]
