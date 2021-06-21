package SparkPack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkObj {

	def main(args: Array[String]): Unit ={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val json_df = spark.read.format("json").option("multiLine","true").load("file:///D:/Study/Bigdata/Zeyobron/Works/Data_Files/Files_3/MultiArrays.json")

					println("======================= Raw DF ========================")
					json_df.show()
					json_df.printSchema()

					val flat_df = json_df.select(
							col("Students"),
							col("address.Permanent_address").alias("Ins_Permanent_address"),
							col("address.temporary_address").alias("Ins_Temporary_address"),
							col("first_name"),
							col("second_name")
							)

					val explode_df = flat_df.withColumn("Students",explode(col("Students")))
					val explode_df2 = explode_df.withColumn("components",explode(col("Students.user.components")))
					val final_df = explode_df2.select(
							col("Students.user.address.Permanent_address").alias("C_Permanent_address"),
							col("Students.user.address.temporary_address").alias("C_Temporary_address"),
							col("Students.user.gender"),
							col("Students.user.name.*"),
							col("Ins_Permanent_address"),
							col("Ins_Temporary_address"),
							col("first_name"),
							col("second_name"),
							col("components")
							)

					println("======================= Final DF ========================")
					final_df.show()
					final_df.printSchema()

					val complex_df = final_df.groupBy("C_Permanent_address","C_Temporary_address","gender","first","last","title","Ins_Permanent_address",
							"Ins_Temporary_address","first_name","second_name").agg(collect_list("components").alias("components"))

					println("======================= Complex DF ========================")
					complex_df.show()
					complex_df.printSchema()

					val complex_df2 = complex_df.select(
							struct(
									struct(
											struct(
													col("C_Permanent_address").alias("Permanent_address"),
													col("C_Temporary_address").alias("temporary_address")
													).alias("address"),
											col("components"),
											col("gender"),
											struct(
													col("first"),
													col("last"),
													col("title")
													).alias("name")
											).alias("user")
									).alias("Students"), 
							struct(
									col("Ins_Permanent_address").alias("Permanent_address"),
									col("Ins_Temporary_address").alias("temporary_address")
									).alias("address"),
							col("first_name"),
							col("second_name")
							)

					println("======================= Complex DF2 ========================")
					complex_df2.show(false)
					complex_df2.printSchema()

					val complex_df3 = complex_df2.groupBy("address","first_name","second_name").agg(collect_list("Students").alias("Students"))
					.select(col("Students"),col("address"),col("first_name"),col("second_name"))

					println("=======================Final DF========================")
					complex_df3.show(false)
					complex_df3.printSchema()
	}

}