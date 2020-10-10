# HW1

## Major features
1.seperate dictionary and posting function
## Major difficulties encountered
1.RDD structure understanding
2.Environment setup
3.RDD function understanding
## Instructions for installation/compilation/ configuration/execution environment 

### 安裝java

1.Download Java JDK 8 from Java’s official website
2.Set the following environment variables
* JAVA_HOME = C:\Progra~1\Java\jdk1.8.0_161
* PATH += C:\Progra~1\Java\jdk1.8.0_161\bin

### 安裝spark
1.Download Spark from Spark’s official website(Download the .tgz file)
2.Extract the .tgz file into D:\Spark
3.Set the environment variables:
* SPARK_HOME = D:\Spark\spark-2.3.0-bin-hadoop2.7
* PATH += D:\Spark\spark-2.3.0-bin-hadoop2.7\bin

### 安裝winutils
1.Download winutils.exe( https://github.com/steveloughran/winutils)
2.Choose the same version as the package type you choose for the Spark .tgz file you chose in section 2 “Spark: Download and Install”
3.navigate inside the hadoop-X.X.X folder, and inside the bin folder you will find winutils.exe
4.Move the winutils.exe file to the bin folder inside SPARK_HOME(D:\Spark\spark-2.3.0-bin-hadoop2.7\bin
)
5.Set the folowing environment variable to be the same as SPARK_HOME(HADOOP_HOME = D:\Spark\spark-2.3.0-bin-hadoop2.7)

#

### Instructions
start master
under spark directory
./sbin/start-master.sh

start worker
./sbin/start-slave.sh 10.0.2.15:7077

complie
2.cd to workspace
3.run 
spark-submit --master spark://10.0.2.15:7077 dictionary.py
spark-submit --master spark://10.0.2.15:7077 posting.py
## Team member
109598091 