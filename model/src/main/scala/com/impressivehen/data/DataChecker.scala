package com.impressivehen.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.BooleanType


object DataChecker {
  case class House(id: String, name: String, price: Double, bedrooms: Double, bathrooms: Double, room_type: String, square_feet: Double, host_is_superhost: Boolean, state: String, cancellation_policy: String, security_deposit: Double, cleaning_fee: Double, extra_people: Double, number_of_reviews: Int, price_per_bedroom: Double, review_scores_rating: Double, instant_bookable: Boolean)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Airbnb")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val inputDs = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/airbnb.csv")
      .withColumn("host_is_superhost", col("host_is_superhost").cast(BooleanType))
      .withColumn("instant_bookable", col("instant_bookable").cast(BooleanType))
      .as[House]

    println("inputDs rows count: " + inputDs.count())

    //
    val ds1 = inputDs.filter("price >= 50 AND price <= 750 AND bathrooms > 0")
    println("ds1 rows count: " + ds1.count())
    ds1.show()
    ds1.select("price", "bedrooms", "bathrooms", "number_of_reviews", "cleaning_fee").describe().show()

    //
    // createOrReplaceTempView: Creates a lazily evaluated "view" that you can then use like a hive table in Spark SQL
    inputDs.createOrReplaceTempView("ds")
    spark.sql(f"""
        select state, count(*) as n, cast(avg(price) as decimal(12, 2)) as avg_price, max(price) as max_price
        from ds group by state
        order by count(*) desc
    """).show()
  }
}
