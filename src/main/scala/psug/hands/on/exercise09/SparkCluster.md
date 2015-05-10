# Exercise 09 : Initialize a cluster, submit it a task

## Statement

We want to run the script created in the first exercise (psug.hands.on.exercise01.SumOfSquareOfNonPrimeNumbers) on a 
Spark cluster

### Notions

* Create a Spark cluster (master and workers)
* Submit it a job

## How To

### Setup Spark Environment

* Download Spark archive : 
```bash
wget http://apache.crihan.fr/dist/spark/spark-1.3.1/spark-1.3.1-bin-hadoop2.6.tgz
```
* Uncompress it and go to the uncompressed directory
```bash
tar -xvf spark-1.3.1-bin-hadoop2.6.tgz
cd spark-1.3.1-bin-hadoop2.6
```
* save spark path as variable SPARK_HOME
```bash
SPARK_HOME="$(pwd)" 
```

### Launch Master

* Start master
```bash
$SPARK_HOME/sbin/start-master.sh
```
* Go to http://localhost:8080 to verify it is launched
* In the webpage, and note the field URL at the left-top corner, for instance spark://blue:7077

### Launch Worker

* Start a worker
```bash
./$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker -c 1 -m 512M spark://blue:7077
```
* Go to http://localhost:8080 to verify that worker is well launched

### Run exercise 01 on your Spark cluster

* Go to root of this project
* Build this project
```bash
sbt clean package
```
* Launch the exercise 01 on Spark Cluster (don't forget to replace spark://blue:7077 by the URL of your master)
```bash
$SPARK_HOME/bin/spark-submit --master spark://blue:7077 --class psug.hands.on.solutions.exercise01.SumOfSquaresOfNonPrimeNumbers --deploy-mode cluster target/scala-2.10/psug-hands-on-spark_2.10-0.0.1.jar
```
* You can go on page http://localhost:8080, click on the worker and then click on its logs (stdout) to see the result of your job


