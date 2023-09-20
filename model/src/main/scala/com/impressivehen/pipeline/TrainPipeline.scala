package com.impressivehen.pipeline

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.spark.SparkSupport.SparkTransformerOps
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.ml.bundle.SparkBundleContext

import scala.util.Using


object TrainPipeline {
  case class House(id: String, name: String, price: Double, bedrooms: Double, bathrooms: Double, room_type: String, square_feet: Double, host_is_superhost: Double, state: String, cancellation_policy: String, security_deposit: Double, cleaning_fee: Double, extra_people: Double, number_of_reviews: Int, price_per_bedroom: Double, review_scores_rating: Double, instant_bookable: Double)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Airbnb")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    /*
    Step 1: Load dataset
     */
    val inputDs = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/airbnb.csv")
      .as[House]

    /*
    Step 2: Define features(columns to be used), filter, divide dataset to be used for training, validation
     */
    // Scala Array is fixed-size
    val continuousFeatures = Array(
      "bathrooms",
      "bedrooms",
      "security_deposit",
      "cleaning_fee",
      "extra_people",
      "number_of_reviews",
      "review_scores_rating"
    )

    val categoricalFeatures = Array(
      "room_type",
      "host_is_superhost",
      "cancellation_policy",
      "instant_bookable"
    )

    val allFeatures = continuousFeatures ++ categoricalFeatures

    // "price" is our target, features ++ target are all columns we need
    val allCols: Array[Column] = (allFeatures ++ Seq("price")).map(inputDs.col)

    // Creates a filter condition to remove rows from Dataset where any of the column have null values
    // returns true only if a Row has no null Column
    val nullFilter = allCols.map(_.isNotNull).reduce(_ && _)

    val filteredDs = inputDs.select(allCols: _*).filter(nullFilter).persist()

    val Array(trainingDs, validationDs) = filteredDs.randomSplit(Array(0.7, 0.3))

    /*
    Step 3: Define stages for transform, standardize, group features into vector in Pipeline
     */

    /*
    VectorAssembler is a Transformer that combines a given list of columns into a single vector column.
    ex:
    bathrooms | bedrooms | extra_people
    1         | 3        | 5
    va = VectorAssembler(inputCols = ["bathrooms", "bedrooms", "extra_people"], outputCol = "features")
    df = va.transform(df)
    ->
    features
    [1, 3, 5]
     */
    val continuousFeatureAssembler = new VectorAssembler().
      setInputCols(continuousFeatures).
      setOutputCol("unscaled_continuous_features")

    /*
    StandardScaler, is a Estimator to standardizes a set of features to have zero mean and a standard deviation of 1.
    ex:
    [2, 8, 5]
    (x-x.mean())/x.std(ddof=1)
    -> [-1, 1, 0]
     */
    val continuousFeatureScaler = new StandardScaler().
      setInputCol("unscaled_continuous_features").
      setOutputCol("scaled_continuous_features")

    /*
    StringIndexer, is a Estimator to convert categorical string columns into numerical indices.
    The StringIndexer processes the input column’s string values based on their frequency in the dataset.
    By default, the most frequent label receives the index 0, the second most frequent label receives index 1,
    and so on. By Default, same frequency index value will be assigned based on the alphabetical order
    ex:
    indexer = StringIndexer(inputCol="Categories", outputCol="Categories_Indexed")
    indexerModel = indexer.fit(df)
    indexed_df = indexerModel.transform(df)

    Categories |  ->      Categories_Indexed |
    A                     1
    A                     1
    B                     0
    B                     0
    B                     0
    C                     2
    C                     2
    D                     3
     */
    val categoricalFeatureIndexers = categoricalFeatures.map {
      feature =>
        new StringIndexer().
          setInputCol(feature).
          setOutputCol(s"${feature}_index")
    }

    // all feature cols (String)(categorical+continuous) after processing
    val featureCols = categoricalFeatureIndexers.map(_.getOutputCol) ++ Seq("scaled_continuous_features")

    val featureAssembler = new VectorAssembler().
      setInputCols(featureCols).
      setOutputCol("features")

    /*
    Step 4: Run Pipeline, build RandomForestRegressor, LinearRegression Model
     */
    // spark.ml.PipelineStage allows and construct and combine Transformer, Estimator into series of stages
    val dataProcessStages = Array(continuousFeatureAssembler, continuousFeatureScaler) ++ categoricalFeatureIndexers ++ Array(featureAssembler)

    // RandomForestRegressor Estimator
    val randomForest = new RandomForestRegressor().
      setFeaturesCol("features").
      // target variable col
      setLabelCol("price").
      // after training, creates prediction output col
      setPredictionCol("price_prediction")

    val randomForestStages = dataProcessStages ++ Array(randomForest)

    val randomForestPipeline = new Pipeline().
      setStages(randomForestStages)

    /*
     When Pipeline.fit() is called, the stages are executed in order.
     If a stage is an Estimator, its Estimator.fit() method will be called on the input dataset to fit a model.
     Then the model, which is a transformer, will be used to transform the dataset as the input to the next stage.
     If a stage is a Transformer, its Transformer.transform() method will be called to produce the dataset for the next stage.
     */
    val randomForestPipelineFit = randomForestPipeline.fit(trainingDs)

    println("Complete: Training Random Forest")

    val scoredRandomForest = randomForestPipelineFit.transform(validationDs)

    scoredRandomForest
      //      .select("bathrooms", "bedrooms", "security_deposit", "number_of_reviews", "price", "price_prediction")
//      .select("features", "price", "price_prediction")
      .where("bedrooms>0 and bathrooms>0")
      .limit(10)
      .show(false)

    /*
    Step 5: Save MLeap Pipeline
     */
    /*
    SparkBundleContext provides a context for managing MLeap bundles and transformations within Spark pipelines.
    withDataset() method allows you to specify how a dataset should be associated with a bundle
     */
    val sparkBundleContext = SparkBundleContext().withDataset(randomForestPipelineFit.transform(validationDs))

    // Creates zip at /tmp, default format is binary Avro
    Using(BundleFile("jar:file:/tmp/uber-spark-randomForest-pipeline.zip")) { bf =>
      randomForestPipelineFit.writeBundle
//        .format(SerializationFormat.Json)
        .save(bf)(sparkBundleContext).get
    }

    spark.stop()
  }
}
