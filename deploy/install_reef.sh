#!/bin/sh
# Copyright (C) 2016 Seoul National University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# SSH, Rsync
sudo apt-get install -y ssh 
sudo apt-get install -y rsync

# SSH key
ssh-keygen -t dsa -P '' -f ~/.ssh/id_rsa 
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# Install Git
sudo apt-get install -y git

# Install Java
sudo apt-get install -y openjdk-7-jdk
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/
echo "export JAVA_HOME=$JAVA_HOME" >> ~/.profile

# (Java 8 HotSpot)
# sudo add-apt-repository ppa:webupd8team/java
# sudo apt-get update
# sudo apt-get install oracle-java8-installer

# Install Hadoop
cd /tmp
wget http://apache.mirror.cdnetworks.com/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz
tar -xzvf hadoop-2.7.2.tar.gz
mv hadoop-2.7.2 ~/hadoop

export HADOOP_HOME=/home/azureuser/hadoop
export YARN_HOME=$HADOOP_HOME
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
echo "export HADOOP_HOME=$HADOOP_HOME" >> ~/.profile
echo "export YARN_HOME=\$HADOOP_HOME" >> ~/.profile
echo "export YARN_CONF_DIR=\$HADOOP_HOME/etc/hadoop" >> ~/.profile
echo "export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH" >> ~/.profile

# Install Spark
#cd /tmp
#wget http://apache.mirror.cdnetworks.com/spark/spark-1.6.0/spark-1.6.0-bin-hadoop2.6.tgz
#tar -xzvf spark-1.6.0-bin-hadoop2.6.tgz
#mv spark-1.6.0-bin-hadoop2.6 ~/spark
#export SPARK_HOME=/home/azureuser/spark

# Install Maven
sudo apt-get install -y maven

#Install protobuf
sudo apt-get install -y automake autoconf g++ make

cd /tmp
wget http://protobuf.googlecode.com/files/protobuf-2.5.0.tar.bz2
tar xjf protobuf-2.5.0.tar.bz2
rm protobuf-2.5.0.tar.bz2
cd /tmp/protobuf-2.5.0
./configure 
sudo make
sudo make install
sudo ldconfig
cd ~

# Install REEF
git clone https://github.com/apache/reef
cd ~/reef
mvn clean -TC1 install -DskipTests -Dcheckstyle.skip=true -Dfindbugs.skip=true
