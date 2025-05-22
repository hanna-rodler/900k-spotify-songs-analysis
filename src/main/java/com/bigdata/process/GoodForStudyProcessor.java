package com.bigdata.process;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class GoodForStudyProcessor {

    private static final String PARQUET_ROOT_FOLDER = "data/processed/";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/hadoop/");
        System.load("C:/hadoop/bin/hadoop.dll");

        SparkSession spark = SparkSession.builder()
                .appName("Good For Study Analysis")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();


        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("multiLine", true)
                .option("escape", "\"")
                .csv("data/spotify_dataset.csv");


        // just compare per "Good for Work/Study 0 or 1
        Dataset<Row> cleanedDf = df.select("Good for Work/Study", "Tempo", "Loudness (db)", "Energy",
                "Positiveness", "Speechiness", "Instrumentalness", "Danceability", "Acousticness", "Popularity",
                        "Time signature", "emotion", "Key")
                .na().drop();

        cleanedDf.printSchema();
        cleanedDf.show(5, false);

        System.out.println("Initiale Partitionen: " + cleanedDf.rdd().getNumPartitions());

        Dataset<Row> withBuckets = cleanedDf.withColumn("good_for_study", when(col("Good for Work/Study")
                        .equalTo(1), lit(1))
                        .otherwise(lit(0))
                ).withColumn("emotion", lower(col("emotion")));

        withBuckets.write().partitionBy("good_for_study").mode(SaveMode.Overwrite).parquet(PARQUET_ROOT_FOLDER+"good_for_study.parquet");



        // COMPARE SONGS WITH DIFFERENT TAGS
        Dataset<Row> cleanedTagComparisonDf = df.select(
                "Good for Work/Study", "Good for Relaxation/Meditation", "Good for Yoga/Stretching",
                "Good For Party", "Good for Exercise", "Good for Running",
                "Tempo", "Loudness (db)", "Energy", "Positiveness", "Speechiness", "Instrumentalness",
                "Danceability", "Acousticness", "Popularity").na().drop();

        Dataset<Row> cleanedParquetTagComparisonDf = cleanedTagComparisonDf.withColumn("good_for_study",
                when(col("Good for Work/Study").equalTo(1), lit(1))
                        .otherwise(lit(0))
        ).withColumn("good_for_party",
                when(col("Good for Party").equalTo(1), lit(1))
                        .otherwise(lit(0))
        ).withColumn("good_for_relaxation_meditation",
                when(col("Good for Relaxation/Meditation").equalTo(1), lit(1))
                        .otherwise(lit(0))
        ).withColumn("good_for_yoga_stretching",
                when(col("Good for Yoga/Stretching").equalTo(1), lit(1))
                        .otherwise(lit(0))
        ).withColumn("good_for_exercise",
                when(col("Good for Exercise").equalTo(1), lit(1))
                        .otherwise(lit(0))
        ).withColumn("good_for_running",
                when(col("Good for Running").equalTo(1), lit(1))
                        .otherwise(lit(0))
        )
        .drop("Good for Work/Study", "Good for Party", "Good for Yoga/Stretching", "Good for Relaxation/Meditation",
                "Good for Exercise", "Good for Running");

        cleanedParquetTagComparisonDf.printSchema();
        cleanedParquetTagComparisonDf.show(5, false);

        cleanedParquetTagComparisonDf.write().partitionBy("good_for_study", "good_for_party").mode(SaveMode.Overwrite).parquet(PARQUET_ROOT_FOLDER+"good_for_study_tag_comp.parquet");

        spark.stop();
    }
}
