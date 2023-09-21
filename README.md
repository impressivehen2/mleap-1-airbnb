# Mleap Demo Airbnb Price Prediction
## Links
#### 
Spark Processing, ML
<br>
https://notebook.community/TrueCar/mleap-demo/notebooks/MLeap%20-%20Train%20Airbnb%20Price%20Prediction%20Model%20-%20Spark%20Saturday

MLeap Pipeline Serialization
<br>
https://github.com/combust/mleap

Sbt Multi Project
<br>
https://www.scala-sbt.org/1.x/docs/Multi-Project.html

Sample Pipeline with MLeap
<br>
http://www.binbinfang.com/2019/04/24/MLeap%E5%AE%9E%E4%BE%8B%E7%AF%87/

MLeap, Scala, Java version
<br>
https://github.com/combust/mleap
#### 

# Summary
#### model
Train linear regression and random forest model\
Convert the Spark Model to Mleap Model\

#### server
Start MLeap Server with model

## Steps
#### 1. com/impressivehen/data/CheckDataTest.scala
Load airbnb.csv with Spark, understand the data

#### 2. com/impressivehen/pipeline/TrainPipeline.scala
Load airbnb.csv with Spark and divide to training data & validation data
<br>
Create Pipeline to load define feature, target, apply ML estimator
<br>
Serialize Pipeline with Mleap, save to .zip 

#### 3. com/impressivehen/pipeline/TrainPipeline.scala
Load Mleap .zip bundle, apply on data

## Knowledge
#### (a) Spark Pipeline 
https://www.cnblogs.com/kongchung/p/5776727.html
1. https://spark.apache.org/docs/3.2.0/api/scala/org/apache/spark/ml/Transformer.html
<br>
Transformer: transform() a DataFrame to another DataFrame
<br>

2. https://spark.apache.org/docs/3.2.0/api/scala/org/apache/spark/ml/Estimator.html
<br>
Estimator: fit() a DataFrame into a Transformer
<br>

3. Pipeline: Connect multiple Transformer, Estimator into a ML Workflow

#### (b) Spark Pipeline 2.
https://spark.apache.org/docs/1.6.0/ml-guide.html
![img_1.png](img_1.png)
Top row represents a Pipeline with three stages. The first two (Tokenizer and HashingTF) are Transformers (blue), and the third (LogisticRegression) is an Estimator (red)

## Debug
#### (a) ClassNotFoundException: breeze.storage.Zero$DoubleZero$
import "org.scalanlp" %% "breeze-natives" % "2.1.0"

#### (b) Java 17 not compatible
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
