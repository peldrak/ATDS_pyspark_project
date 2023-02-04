# ATDS_pyspark_project

## System Configurations

---
/etc/hosts
---

192.168.0.1 master    #private ipv4 of master <br>
192.168.0.2 slave     #private ipv4 of slave

---
passwordless ssh setup 
---

```
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
scp -r .ssh/ user@slave:~/
```


## Hadoop (HDFS) Configurations:

---
~/.bashrc
---

```
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_INSTALL=/home/user/hadoop-3.3.4
export PATH=$PATH:$HADOOP_INSTALL/bin
export PATH=$PATH:$HADOOP_INSTALL/sbin
export HADOOP_HOME=$HADOOP_INSTALL
export HADOOP_COMMON_HOME=$HADOOP_INSTALL
export HADOOP_HDFS_HOME=$HADOOP_INSTALL
export HADOOP_CONF_DIR=$HADOOP_INSTALL/etc/hadoop
```

---
/home/user/hadoop-3.3.4/etc/hadoop/core-site.xml
---

```
<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://master:9000</value>
</property>
</configuration>
```

---
/home/user/hadoop-3.3.4/etc/hadoop/hdfs-site.xml
---

```
<configuration>
<property>
<name>dfs.replication</name>
<value>2</value>
<description>Default block replication.</description>
</property>
<property>
<name>dfs.namenode.name.dir</name>
<value>/home/user/hdfsname</value>
</property>
<property>
<name>dfs.datanode.data.dir</name>
<value>/home/user/hdfsdata</value>
</property>
<property>
<name>dfs.blocksize</name>
<value>64m</value>
<description>Block size</description>
</property>
<property>
<name>dfs.webhdfs.enabled</name>
<value>true</value>
</property>
<property>
<name>dfs.support.append</name>
<value>true</value>
</property>
</configuration>
```

---
/home/user/hadoop-3.3.4/etc/hadoop/workers
---

```
master 
slave
```

## Spark Configurations:

---
~/.bashrc
---

```
export SPARK_HOME=/home/user/spark-3.3.1-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
alias start-all.sh='$SPARK_HOME/sbin/start-all.sh'
alias stop-all.sh='$SPARK_HOME/sbin/stop-all.sh'
```

---
/home/user/spark-3.3.1-bin-hadoop3/conf/spark-env.sh
---

```
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=1g
```

---
/home/user/spark-3.3.1-bin-hadoop3/conf/spark-defaults.conf
---

```
spark.master  spark://master:7077
spark.submit.deployMode  client
spark.executor.instances  2
spark.executor.cores  1
spark.executor.memory  512m
spark.driver.memory  512m
```

---
/home/user/spark-3.3.1-bin-hadoop3/conf/workers
---

```
master 
slave
```
