#!/bin/bash
# Install packages and leave a log in /tmp/install_reef_log file to track the progress.
LOG_FILE=/tmp/install_reef.log

function install_essential {
  # SSH, rsync, Git, Maven
  sudo apt-get install -y ssh rsync git maven

  #Install prerequisite for protobuf
  sudo apt-get install -y automake autoconf g++ make

  # SSH key
  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
}

function install_protobuf {
  cd /tmp
  wget http://protobuf.googlecode.com/files/protobuf-2.5.0.tar.bz2
  tar xjf protobuf-2.5.0.tar.bz2
  rm protobuf-2.5.0.tar.bz2
  cd /tmp/protobuf-2.5.0
  ./configure
  sudo make
  sudo make install
  sudo ldconfig
}

# Let's go with HotSpot, as OpenJDK has an issue (http://stackoverflow.com/questions/8375423/missing-artifact-com-suntoolsjar). Note that keyboard interrupt is required while installation (Choose <OK>, then <YES>).
function install_java {
  sudo add-apt-repository ppa:webupd8team/java
  sudo apt-get update
  sudo apt-get install -y oracle-java8-installer
  export JAVA_HOME=/usr/lib/jvm/java-8-oracle/
  echo "export JAVA_HOME=$JAVA_HOME" >> ~/.profile
}

# Install hadoop on $HOME/hadoop
function install_hadoop {
  cd /tmp
  wget http://mirror.navercorp.com/apache/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz
  tar -xzvf hadoop-2.7.2.tar.gz
  mv hadoop-2.7.2 ~/hadoop

  export HADOOP_HOME=$HOME/hadoop
  export YARN_HOME=$HADOOP_HOME
  export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
  export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
  echo "export HADOOP_HOME=$HADOOP_HOME" >> ~/.profile
  echo "export YARN_HOME=\$HADOOP_HOME" >> ~/.profile
  echo "export YARN_CONF_DIR=\$HADOOP_HOME/etc/hadoop" >> ~/.profile
  echo "export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH" >> ~/.profile
}

function install_reef {
  cd ~
  git clone https://github.com/apache/reef
  cd reef
  mvn clean -TC1 install -DskipTests -Dcheckstyle.skip=true -Dfindbugs.skip=true
}

# Normally we don't have to build cay in all machines.
function install_cay {
  # Install Fortran (which cay depends on)
  sudo apt-get install -y libgfortran3

  cd ~
  git clone https://github.com/cmssnu/cay # Username/password is required
  cd cay
  mvn clean -TC1 install -DskipTests -Dcheckstyle.skip=true -Dfindbugs.skip=true
}

# Start installation
export LC_ALL="en_US.UTF-8"
echo "Install start: $(date)" > $LOG_FILE

install_essential
echo "Essential packages installed" >> $LOG_FILE

install_protobuf
echo "Protobuf installed" >> $LOG_FILE

install_java
echo "Java installed" >> $LOG_FILE

install_hadoop
echo "Hadoop installed" >> $LOG_FILE

install_reef
echo "REEF installed" >> $LOG_FILE

# install_cay #  Comment out this line only when you want to build cay on your machine
# echo "Cay installed" >> $LOG_FILE

echo "Done: $(date)" >> $LOG_FILE
