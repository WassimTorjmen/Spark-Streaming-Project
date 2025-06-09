package com.esgi

import scala.io.Source
import java.io.{File, PrintWriter}
import ujson._

object FetchAndTransform {
  case class PackagingInfo(
    material: String,
    shape: String,
    recycling: String,
    quantity: Option[String]
  )

  case class NutritionalInfo(
    code: String,
    product_name: String,
    brands: String,
    nutriscore_grade: String,
    nutriscore_score: Int,
    energy_kcal: Double,
    fat: Double,
    saturated_fat: Double,
    carbohydrates: Double,
    sugars: Double,
    proteins: Double,
    salt: Double,
    cocoa_percentage: Option[Double],
    allergens: List[String],
    labels: List[String],
    categories: List[String],
    packaging: List[PackagingInfo]
  )

  def main(args: Array[String]): Unit = {
    try {
      val baseUrl = "https://datasets-server.huggingface.co/rows?dataset=openfoodfacts%2Fproduct-database&config=default&split=food"
      val offset = 0
      val length = 5
      val url = s"$baseUrl&offset=$offset&length=$length"

      println(s"Fetching data from URL: $url")
      val jsonString = Source.fromURL(url).mkString
      val json = ujson.read(jsonString)
      val rows = json("rows").arr

      val cleanedData = rows.map { row =>
        val rowData = row("row").obj
        
        // Helper functions with safer access
        def getStr(data: Obj, key: String): String = 
          data.value.get(key).map {
            case s: Str => s.str
            case _ => ""
          }.getOrElse("")
          
        def getInt(data: Obj, key: String): Int = 
          data.value.get(key).map {
            case n: Num => n.num.toInt
            case _ => 0
          }.getOrElse(0)
          
        def getList(data: Obj, key: String): List[String] = 
          data.value.get(key).map {
            case a: Arr => a.arr.map {
              case s: Str => s.str.replaceAll("(en:|fr:)", "")
              case _ => ""
            }.toList
            case _ => List.empty
          }.getOrElse(List.empty)
        
        // Product info
        val code = getStr(rowData, "code")
        val productName = rowData.value.get("product_name").map {
          case names: Arr => names.arr.collectFirst {
            case obj: Obj if obj.value.get("lang").exists {
              case s: Str => s.str == "fr"
              case _ => false
            } => obj.value.get("text").collect {
              case s: Str => s.str
            }.getOrElse("")
          }.getOrElse(names.arr.headOption.flatMap {
            case obj: Obj => obj.value.get("text").collect {
              case s: Str => s.str
            }
            case _ => None
          }.getOrElse(""))
          case _ => ""
        }.getOrElse("")
        
        val brands = getStr(rowData, "brands")
        val nutriscoreGrade = getStr(rowData, "nutriscore_grade")
        val nutriscoreScore = getInt(rowData, "nutriscore_score")
        
        // Nutrition data - fixed extraction
        val nutriments = rowData.value.get("nutriments").collect {
          case a: Arr => a.arr.flatMap {
            case o: Obj => Some(o)
            case _ => None
          }
          case _ => List.empty[Obj]
        }.getOrElse(List.empty)
        
        def getNutrientValue(name: String): Double = {
          nutriments.find(_.value.get("name").exists {
            case s: Str => s.str == name
            case _ => false
          }).flatMap(_.value.get("100g").collect {
            case n: Num => n.num
            case s: Str => try { s.str.toDouble } catch { case _: Exception => 0.0 }
            case _ => 0.0
          }).getOrElse(0.0)
        }
        
        def getOptNutrientValue(name: String): Option[Double] = {
          nutriments.find(_.value.get("name").exists {
            case s: Str => s.str == name
            case _ => false
          }).flatMap(_.value.get("100g").collect {
            case n: Num => Some(n.num)
            case s: Str => try { Some(s.str.toDouble) } catch { case _: Exception => None }
            case _ => None
          }).flatten
        }
        
        // Packaging info
        val packagings = rowData.value.get("packagings").map {
          case a: Arr => a.arr.collect {
            case o: Obj => 
              PackagingInfo(
                material = o.value.get("material").collect { case s: Str => s.str }.getOrElse(""),
                shape = o.value.get("shape").collect { case s: Str => s.str }.getOrElse(""),
                recycling = o.value.get("recycling").collect { case s: Str => s.str }.getOrElse(""),
                quantity = o.value.get("quantity_per_unit").collect { case s: Str => Some(s.str) }.getOrElse(None)
              )
          }.toList
          case _ => List.empty
        }.getOrElse(List.empty)
        
        NutritionalInfo(
          code = code,
          product_name = productName,
          brands = brands,
          nutriscore_grade = nutriscoreGrade,
          nutriscore_score = nutriscoreScore,
          energy_kcal = getNutrientValue("energy-kcal"),
          fat = getNutrientValue("fat"),
          saturated_fat = getNutrientValue("saturated-fat"),
          carbohydrates = getNutrientValue("carbohydrates"),
          sugars = getNutrientValue("sugars"),
          proteins = getNutrientValue("proteins"),
          salt = getNutrientValue("salt"),
          cocoa_percentage = getOptNutrientValue("cocoa"),
          allergens = getList(rowData, "allergens_tags"),
          labels = getList(rowData, "labels_tags"),
          categories = getList(rowData, "categories_tags"),
          packaging = packagings
        )
      }

      // Convert to JSON
      val jsonOutput = Arr.from(cleanedData.map { product =>
        Obj(
          "code" -> Str(product.code),
          "product_name" -> Str(product.product_name),
          "brands" -> Str(product.brands),
          "nutriscore_grade" -> Str(product.nutriscore_grade),
          "nutriscore_score" -> Num(product.nutriscore_score),
          "energy_kcal" -> Num(product.energy_kcal),
          "fat" -> Num(product.fat),
          "saturated_fat" -> Num(product.saturated_fat),
          "carbohydrates" -> Num(product.carbohydrates),
          "sugars" -> Num(product.sugars),
          "proteins" -> Num(product.proteins),
          "salt" -> Num(product.salt),
          "cocoa_percentage" -> product.cocoa_percentage.map(Num(_)).getOrElse(Null),
          "allergens" -> Arr.from(product.allergens.map(Str(_))),
          "labels" -> Arr.from(product.labels.map(Str(_))),
          "categories" -> Arr.from(product.categories.map(Str(_))),
          "packaging" -> Arr.from(product.packaging.map { p =>
            Obj(
              "material" -> Str(p.material),
              "shape" -> Str(p.shape),
              "recycling" -> Str(p.recycling),
              "quantity" -> p.quantity.map(Str(_)).getOrElse(Null)
            )
          })
        )
      })

      // Write to file
      val outputFile = new File("nutritional_data.json")
      val writer = new PrintWriter(outputFile)
      try {
        writer.write(jsonOutput.toString())
        println(s"Successfully wrote data to ${outputFile.getAbsolutePath}")
        println("Sample data:")
        println(jsonOutput.arr.head.toString())
      } finally {
        writer.close()
      }

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}