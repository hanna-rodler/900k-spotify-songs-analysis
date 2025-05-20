package com.bigdata.process;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.col;

public class DanceabilityPopularityProcessor {

    private static final String PARQUET_ROOT_FOLDER = "data/processed/";


    public static void main(String[] args) {
        System.setProperty ("hadoop.home.dir", "C:/hadoop/" );
        System.load ("C:/hadoop/bin/hadoop.dll");

        SparkSession spark = SparkSession.builder()
                .appName("Danceability vs Popularity Reader")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();

        // Spark appearenntly sets "snappy" compression automatically
        //read csv
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("multiLine", true)
                .option("escape", "\"")
                .csv("data/spotify_dataset.csv");

        Dataset<Row> cleanedDf = df.select("Artist(s)", "song", "Popularity", "Danceability", "Tempo").na().drop();

        cleanedDf.printSchema();
        cleanedDf.show(10, false);

        System.out.println("Initiale Partitionen: " + cleanedDf.rdd().getNumPartitions());

        Dataset<Row> withBuckets = cleanedDf.withColumn("popularity_bucket", when(col("popularity").$less(26), "0-25")
                .when(col("popularity").$less(51), "26-50")
                .when(col("popularity").$less(76), "51-75")
                .otherwise("75-100"));

        withBuckets.write().partitionBy("popularity_bucket").mode(SaveMode.Overwrite).parquet(PARQUET_ROOT_FOLDER+"popularity.parquet");


        Dataset<Row> withSmallerBuckets = cleanedDf.withColumn("popularity_bucket", when(col("popularity").$less(11), "0-10")
                .when(col("popularity").$less(21), "11-20")
                .when(col("popularity").$less(31), "21-30")
                .when(col("popularity").$less(41), "31-40")
                .when(col("popularity").$less(51), "41-50")
                .when(col("popularity").$less(61), "51-60")
                .when(col("popularity").$less(71), "61-70")
                .when(col("popularity").$less(81), "71-80")
                .when(col("popularity").$less(91), "81-90")
                .otherwise("91-100"));

        withSmallerBuckets.write().partitionBy("popularity_bucket").mode(SaveMode.Overwrite).parquet(PARQUET_ROOT_FOLDER+"popularity_bucket_smaller.parquet");


        spark.stop();
    }
}
