package com.esgi

import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode, Column, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.{Try, Success, Failure}

object FetchAndTransform {

  case class PackagingInfo(
    material: String,
    shape: String,
    recycling: String,
    quantity: Option[String]
  )

  val API_BASE_URL = "https://datasets-server.huggingface.co/rows?dataset=openfoodfacts%2Fproduct-database&config=default&split=food"
  val DB_URL = "jdbc:postgresql://localhost:5433/food_analysis"
  val DB_USER = "postgres"
  val DB_PASSWORD = "postgres"

  def main(args: Array[String]): Unit = {
    val spark = Try {
      SparkSession.builder()
        .appName("FetchAndTransformSpark")
        .master("local[*]")
        .getOrCreate()
    } match {
      case Success(session) => session
      case Failure(e) =>
        println(s"Failed to create Spark session: ${e.getMessage}")
        sys.exit(1)
    }

    import spark.implicits._

    Try {
      val offset = 500
      val length = 100
      val url = s"$API_BASE_URL&offset=$offset&length=$length"

      val rawJsonStr = Try(scala.io.Source.fromURL(url).mkString) match {
        case Success(json) => json
        case Failure(e) =>
          println(s"Failed to fetch data from API: ${e.getMessage}")
          spark.stop()
          sys.exit(1)
      }

      val jsonDF = spark.read
        .option("multiLine", true)
        .json(Seq(rawJsonStr).toDS)

      val rowsDF = jsonDF.select(explode($"rows").alias("row"))
      val productsDF = rowsDF.select($"row.row".alias("product"))

      val packagingSchema = ArrayType(StructType(Seq(
        StructField("material", StringType, nullable = true),
        StructField("number_of_units", StringType, nullable = true),
        StructField("quantity_per_unit", StringType, nullable = true),
        StructField("quantity_per_unit_unit", StringType, nullable = true),
        StructField("quantity_per_unit_value", StringType, nullable = true),
        StructField("recycling", StringType, nullable = true),
        StructField("shape", StringType, nullable = true),
        StructField("weight_measured", StringType, nullable = true)
      )))

      // Function to extract nutrition value from nutriments array
      def getNutrimentValue(nutriments: Column, name: String): Column = {
        filter(nutriments, n => n.getField("name") === name)
          .getItem(0)
          .getField("100g")
          .cast("double")
      }

      val parsedDF = productsDF.select(
        $"product.code".alias("code"),
        coalesce($"product.product_name".getItem(0).getField("text"), lit("")).alias("product_name"),
        coalesce($"product.brands", lit("")).alias("brands"),
        coalesce($"product.nutriscore_grade", lit("")).alias("nutriscore_grade"),
        coalesce($"product.nutriscore_score".cast("int"), lit(0)).alias("nutriscore_score"),
        getNutrimentValue($"product.nutriments", "energy-kcal").alias("energy_kcal"),
        getNutrimentValue($"product.nutriments", "fat").alias("fat"),
        getNutrimentValue($"product.nutriments", "saturated-fat").alias("saturated_fat"),
        getNutrimentValue($"product.nutriments", "carbohydrates").alias("carbohydrates"),
        getNutrimentValue($"product.nutriments", "sugars").alias("sugars"),
        getNutrimentValue($"product.nutriments", "proteins").alias("proteins"),
        getNutrimentValue($"product.nutriments", "salt").alias("salt"),
        getNutrimentValue($"product.nutriments", "cocoa").alias("cocoa_percentage"),
        coalesce($"product.allergens_tags", array()).cast(ArrayType(StringType)).alias("allergens"),
        coalesce($"product.labels_tags", array()).cast(ArrayType(StringType)).alias("labels"),
        coalesce($"product.categories_tags", array()).cast(ArrayType(StringType)).alias("categories"),
        coalesce($"product.packagings", array()).cast(packagingSchema).alias("packaging")
      )

      val cleanedDF = parsedDF.withColumn("packaging",
        expr("""
          transform(packaging, p -> struct(
            p.material as material,
            p.shape as shape,
            p.recycling as recycling,
            p.quantity_per_unit as quantity
          ))
        """)
      )

      // Prepare data for database insertion
      val productsToSave = cleanedDF.select(
        $"code", $"product_name", $"brands", $"nutriscore_grade", $"nutriscore_score",
        $"energy_kcal", $"fat", $"saturated_fat", $"carbohydrates", 
        $"sugars", $"proteins", $"salt", $"cocoa_percentage"
      ).na.fill(0, Seq("nutriscore_score"))
       .na.fill(0.0, Seq(
         "energy_kcal", "fat", "saturated_fat", "carbohydrates", 
         "sugars", "proteins", "salt"
       ))

      val allergensToSave = cleanedDF.select(
        $"code".alias("product_code"), 
        explode_outer($"allergens").alias("allergen")
      ).filter($"allergen".isNotNull && $"allergen" =!= "")

      val labelsToSave = cleanedDF.select(
        $"code".alias("product_code"), 
        explode_outer($"labels").alias("label")
      ).filter($"label".isNotNull && $"label" =!= "")

      val categoriesToSave = cleanedDF.select(
        $"code".alias("product_code"), 
        explode_outer($"categories").alias("category")
      ).filter($"category".isNotNull && $"category" =!= "")

      val packagingsToSave = cleanedDF.select(
        $"code".alias("product_code"),
        explode_outer($"packaging").alias("pack")
      ).select(
        $"product_code",
        $"pack.material",
        $"pack.shape",
        $"pack.recycling",
        $"pack.quantity"
      ).filter($"material".isNotNull && $"material" =!= "")

      def writeToDB(df: DataFrame, table: String, mode: SaveMode = SaveMode.Append): Unit = {
  Try {
    // Read existing records based on table's unique constraints
    val existingDF = table match {
      case "products" =>
        spark.read
          .format("jdbc")
          .option("url", DB_URL)
          .option("dbtable", "products")
          .option("user", DB_USER)
          .option("password", DB_PASSWORD)
          .load()
          .select("code")
      
      case "allergens" =>
        spark.read
          .format("jdbc")
          .option("url", DB_URL)
          .option("dbtable", "allergens")
          .option("user", DB_USER)
          .option("password", DB_PASSWORD)
          .load()
          .select("product_code", "allergen")
      
      case "labels" =>
        spark.read
          .format("jdbc")
          .option("url", DB_URL)
          .option("dbtable", "labels")
          .option("user", DB_USER)
          .option("password", DB_PASSWORD)
          .load()
          .select("product_code", "label")
      
      case "categories" =>
        spark.read
          .format("jdbc")
          .option("url", DB_URL)
          .option("dbtable", "categories")
          .option("user", DB_USER)
          .option("password", DB_PASSWORD)
          .load()
          .select("product_code", "category")
      
      case "packagings" =>
        // Create empty DataFrame with same schema as input
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df.schema)
      
      case _ => throw new IllegalArgumentException(s"Unknown table: $table")
    }

    // Filter out records that already exist
    val newRecords = table match {
      case "products" =>
        val existingCodes = existingDF.collect().map(_.getString(0)).toSet
        df.filter(!col("code").isin(existingCodes.toSeq:_*))
      
      case "allergens" =>
        val existingPairs = existingDF.collect()
          .map(r => (r.getString(0), r.getString(1))).toSet
        df.filter(row => {
          val productCode = row.getAs[String]("product_code")
          val allergen = row.getAs[String]("allergen")
          !existingPairs.contains((productCode, allergen))
        })
      
      case "labels" =>
        val existingPairs = existingDF.collect()
          .map(r => (r.getString(0), r.getString(1))).toSet
        df.filter(row => {
          val productCode = row.getAs[String]("product_code")
          val label = row.getAs[String]("label")
          !existingPairs.contains((productCode, label))
        })
      
      case "categories" =>
        val existingPairs = existingDF.collect()
          .map(r => (r.getString(0), r.getString(1))).toSet
        df.filter(row => {
          val productCode = row.getAs[String]("product_code")
          val category = row.getAs[String]("category")
          !existingPairs.contains((productCode, category))
        })
      
      case "packagings" =>
        // No duplicate check for packagings
        df
      
      case _ => throw new IllegalArgumentException(s"Unknown table: $table")
    }

    // Write only new records
    if (newRecords.count() > 0) {
      newRecords.write
        .format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", table)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .mode(mode)
        .save()
      println(s"Wrote ${newRecords.count()} new records to $table")
    } else {
      println(s"No new records to write to $table")
    }
  } match {
    case Success(_) => println(s"Successfully processed $table")
    case Failure(e) => 
      println(s"Failed to process $table: ${e.getMessage}")
      e.printStackTrace()
  }
}

      // Write data to database
      writeToDB(productsToSave, "products", SaveMode.Append)
      writeToDB(allergensToSave, "allergens")
      writeToDB(labelsToSave, "labels")
      writeToDB(categoriesToSave, "categories")
      writeToDB(packagingsToSave, "packagings")

      println("Processing completed successfully")
    } match {
      case Success(_) => spark.stop()
      case Failure(e) =>
        println(s"Error during processing: ${e.getMessage}")
        spark.stop()
        sys.exit(1)
    }
  }
}