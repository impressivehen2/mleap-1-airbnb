package com.impressivehen

import ml.combust.bundle.BundleFile
import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}
import ml.combust.mleap.runtime.MleapSupport.MleapBundleFileOps
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, Transformer}
import resource.managed

object PipelineDeserializerTest {
  def main(args: Array[String]): Unit = {
    val mleapPipeline = loadModel("/tmp/uber-spark-randomForest-pipeline.zip")

    // input schema must match SparkBundleContext().withDataset(ds)
    val inputSchema = StructType(Seq(
      StructField("bedrooms", ScalarType.Double),
      StructField("bathrooms", ScalarType.Double),
      StructField("room_type", ScalarType.String),
      StructField("host_is_superhost", ScalarType.Double),
      StructField("cancellation_policy", ScalarType.String),
      StructField("security_deposit", ScalarType.Double),
      StructField("cleaning_fee", ScalarType.Double),
      StructField("extra_people", ScalarType.Double),
      StructField("number_of_reviews", ScalarType.Double),
      StructField("review_scores_rating", ScalarType.Double),
      StructField("instant_bookable", ScalarType.Double),
    )).get

    val data = Seq(
      Row(1.0,1.0,"Private room",0.0,"flexible",0.0,30.0,100.0,70.0,94.0,0.0),
      Row(0.0,1.0,"Entire home/apt",0.0,"flexible",0.0,0.0,7.0,60.0,97.0,0.0)
    )

    val frame = DefaultLeapFrame(inputSchema, data)

    val transformedFrame = mleapPipeline.transform(frame).get

    val ds = transformedFrame.dataset

    val outputSchemaSize = ds(0).size
    println("Output schema:")
    transformedFrame.schema.fields.foreach { field =>
      println(s"$field")
    }
    println(s"Transformed dataframe: $ds")
    println("Row(0) price_prediction: %s".format(ds(0).get(outputSchemaSize-1)))
    println("Row(1) price_prediction: %s".format(ds(1).get(outputSchemaSize-1)))
  }

  def loadModel(path: String): Transformer = {
    val modelPath = "jar:file://" + path
    val bundle = (for (bundleFile <- managed(BundleFile(modelPath))) yield {
      bundleFile.loadMleapBundle().get
    }).opt

    bundle match {
      case Some(bt) => bt.root
      case None => throw new RuntimeException("Unable to load Mleap bundle " + modelPath)
    }
  }
}
