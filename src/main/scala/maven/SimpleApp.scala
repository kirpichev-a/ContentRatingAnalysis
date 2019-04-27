package maven

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._  

object SimpleApp {
  
  def main(args: Array[String]) {
    val spark = org.apache.spark.sql.SparkSession.builder // create session
        .master("local")
        .appName("Analysis of content rating")
        .getOrCreate;
    spark.sparkContext.setLogLevel("ERROR") 
    val df = spark.read // load TMNDB 5000 movie dataset
         .format("csv")
         .option("header", "true") //first line in file has headers
         .option("mode", "DROPMALFORMED")
         .load("src/main/scala/maven/movie_metadata.csv")
    
    df.sort(desc("title_year"))
    df.show()
    
    //2010s
    val df2010s = df.filter("title_year > 2010") // obtain relevant data
    .select("content_rating", "title_year")
    .groupBy("content_rating")
    .count()
    
    val dfDesc2010s = df2010s.orderBy(desc("count"))
    
    val sum2010s = df.filter("title_year >= 2010").count() // total amount of produced films above 2010
    
    dfDesc2010s.withColumn("count", col("count").cast("float")) // cast int to float in column "count" to enable division 
    val df2010complete = dfDesc2010s.withColumn("percentage", format_number((col("count") / (sum2010s.asInstanceOf[Float]) * 100), 2)) // counting and appending column of percentages of content rating
    println("Content rating after 2010:")
    df2010complete.show
    
    //2000s
    val df2000s = df.filter(col("title_year") <= 2010 && col("title_year") > 2000) // obtain relevant data
    .select("content_rating", "title_year")
    .groupBy("content_rating")
    .count()
    
    val dfDesc2000s = df2000s.orderBy(desc("count"))
    
    val sum2000s = df.filter(col("title_year") <= 2010 && col("title_year") > 2000).count() // total amount of produced films in 2000s
    
    dfDesc2000s.withColumn("count", col("count").cast("float")) // cast int to float
    println("Content rating in 2001-2010:")
    val df2000complete = dfDesc2000s.withColumn("percentage", format_number((col("count") / (sum2000s.asInstanceOf[Float]) * 100), 2))
    df2000complete.show
    //1980-2000
    val df8090s = df.filter(col("title_year") <= 2000 && col("title_year") > 1980) // obtain relevant data
    .select("content_rating", "title_year")
    .groupBy("content_rating")
    .count()
    
    val dfDesc8090s = df8090s.orderBy(desc("count"))
    
    val sum8090s = df.filter(col("title_year") <= 2000 && col("title_year") > 1980).count() // total amount of produced films in 2000s
    
    dfDesc8090s.withColumn("count", col("count").cast("float"))
    val df8090complete = dfDesc8090s.withColumn("percentage", format_number((col("count") / (sum8090s.asInstanceOf[Float]) * 100), 2))
    println("Content rating in 1981-2000:")
    df8090complete.show
    //1950-1980
    val df5070s = df.filter(col("title_year") <= 1980 && col("title_year") > 1950) // obtain relevant data
    .select("content_rating", "title_year")
    .groupBy("content_rating")
    .count()
    
    val dfDesc5070s = df5070s.orderBy(desc("count"))
    
    val sum5070s = df.filter(col("title_year") <= 1980 && col("title_year") > 1950).count() // total amount of produced films in 2000s
    
    dfDesc5070s.withColumn("count", col("count").cast("float"))
    val df5070complete = dfDesc5070s.withColumn("percentage", format_number((col("count") / (sum5070s.asInstanceOf[Float]) * 100), 2))
    println("Content rating in 1951-1980:")
    df5070complete.show
    //Before 1950
    val dfBefore50s = df.filter(col("title_year") <= 1950) // obtain relevant data
    .select("content_rating", "title_year")
    .groupBy("content_rating")
    .count()
    
    val dfDescBefore50s = dfBefore50s.orderBy(desc("count"))
    
    val sumBefore50s = df.filter(col("title_year") <= 1950).count() // total amount of produced films before 1950
    
    dfDescBefore50s.withColumn("count", col("count").cast("float"))
    val dfBefore50scomplete = dfDescBefore50s.withColumn("percentage", format_number((col("count") / (sumBefore50s.asInstanceOf[Float]) * 100), 2))
    println("Content rating in 1900-1950:")
    dfBefore50scomplete.show
    
    
    
    spark.stop() // end session
  }
}