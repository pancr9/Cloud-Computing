# Information Retrieval on basis of Term Frequency - Inverse Document Frequency

### The assignment has been completed on Cloudera Single Node cluster on Ubuntu Machine (Ubuntu 16.04.3 LTS on wiki-mirco.txt)

* Author: Rekhansh Panchal
* Email: rekhanshpanchal@gmail.com
* ITCS 6190 - Cloud Computing


#### General Information

* The objective of the program is to get familiar with PageRank algorithm implementation in Hadoop with context of Information Retrieval.
* This is based on http://infolab.stanford.edu/~backrub/google.html
* Here, we’ll be calculating rank of each page depending on its incoming links and damping factor.

#### Requirements:
* Java
* Hadoop using Cloudera single node cluster
* micro-wiki.txt
* Ubuntu 16.04.3 

>  rekhansh should be replaced by your Ubuntu username.


#### Assumptions

* Convergence of values in matrix will occur after 10 iterations.


##### Below mentioned are the commands followed for execution of program.

```
pwd
```
/home/rekhansh/PageRank
---
```
echo $JAVA_HOME
```
/usr/java/jdk1.8.0_144
---

##### Create input folder.
```
hadoop fs -mkdir /user/rekhansh/pageRank /user/rekhansh/pageRank/input
```
---

##### Get micro-wiki into input.
```
hadoop fs -put /home/rekhansh/PageRank/Documents/*  /user/rekhansh/pageRank/input
```
---

##### Create folder build.
```
mkdir -p build
```
---


##### Compile PageRank.java and put class files in build folder.
```
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Code/PageRank.java -d build -Xlint
```
---

##### Create pagerank.jar
```
jar -cvf pagerank.jar -C build/ .
```
---

##### Run pagerank.jar and store output in output folder
```
hadoop jar pagerank.jar org.myorg.PageRank /user/rekhansh/pageRank/input /user/rekhansh/pageRank/output
```
---

##### Generating output in .out file
```
hadoop fs -cat /user/rekhansh/pageRank/output/* > wiki-micro.out
```
---

##### Generating output in .out file for initial 100 values.
```
head -100 wiki-micro.out > wiki-micro-100.out
```
---

Thank you!
