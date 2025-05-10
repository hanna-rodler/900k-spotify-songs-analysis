package com.bigdata.analysis;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DanceabilityPopularityAnalysis {

    private static final String PARQUET_ROOT_FOLDER = "data/processed/";


    public static void main(String[] args) {
        System.setProperty ("hadoop.home.dir", "C:/hadoop/" );
        System.load ("C:/hadoop/bin/hadoop.dll");

        SparkSession spark = SparkSession.builder()
                .appName("")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        //read csv
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("multiLine", true)
                .option("escape", "\"")
                .csv("data/spotify_dataset.csv");


        System.out.println("Initiale Partitionen: " + df.rdd().getNumPartitions());


        Dataset<Row> withBuckets = df.withColumn("popularity_bucket", when(col("popularity").$less(26), "0-25")
                .when(col("popularity").$less(51), "26-50")
                .when(col("popularity").$less(76), "51-75")
                .otherwise("75-100"));

        withBuckets.write().partitionBy("popularity_bucket").mode(SaveMode.Overwrite).parquet(PARQUET_ROOT_FOLDER+"popularity.parquet");

        spark.stop();
    }
}
