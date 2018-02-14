### The Linear Regression has been completed on Cloudera Single Node cluster on Ubuntu Machine (Ubuntu 16.04.3 LTS).
* Author: Rekhansh Panchal
* Email: rekhanshpanchal@gmail.com
* ITCS 6190 - Cloud Computing.

#### General Information
* The objective of the program is to get familiar with Spark with context of Linear Regression.

#### Requirements:
* Python (Used 2.7.12)
* Spark (Used 2.2.0)
* numpy package.
* Ubuntu 16.04.3 

>  rekhansh should be replaced by your Ubuntu username.

#### Below mentioned are the commands followed for execution of program.
```
pwd
```
###### /home/rekhansh/linearRegression

---

##### Check Spark version
```
spark-submit --version
```
---

##### Check files existing
```
ls
```
linreg.py  yxlin2.csv  yxlin.csv

---

##### Run program with input yxlin
```
spark-submit linreg.py yxlin.csv yxlin.out
```
---

##### Run program with input yxlin2
```
spark-submit linreg.py yxlin2.csv yxlin2.out
```

Thank you!
