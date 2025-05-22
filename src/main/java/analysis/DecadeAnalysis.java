package analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Computes decade-level averages for positiveness, tempo, and energy.
 */
public class DecadeAnalysis {

    private static final String PARQUET_OUTPUT_PATH = "data/processed/decade_genre_analysis.parquet";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/Users/43650/hadoop/");
        System.load("C:/Users/43650/hadoop/bin/hadoop.dll");

        SparkSession spark = SparkSession.builder()
                .appName("Song Decade Analysis")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("multiLine", true)
                .option("escape", "\"")
                .csv("data/raw/spotify_dataset.csv");

        // Clean column for easier access
        df = df.withColumnRenamed("Release Date", "ReleaseDate");

        // Add Decade column
        Dataset<Row> transformedDF = df.withColumn("decade",
                expr("floor(year(ReleaseDate) / 10) * 10")
        );

        // Write to Parquet, partinioned by decade
        transformedDF.write()
                .partitionBy("decade")
                .mode(SaveMode.Overwrite)
                .parquet(PARQUET_OUTPUT_PATH);

        // Read back the parquet with automatic partition discovery
        Dataset<Row> parquetDF = spark.read()
                .parquet(PARQUET_OUTPUT_PATH);

        // DataFrame API
        Dataset<Row> analysisResult = parquetDF.groupBy("decade")
                .agg(
                        avg("Positiveness").alias("avg_positiveness"),
                        avg("Tempo").alias("avg_tempo"),
                        avg("Energy").alias("avg_energy"),
                        count("*").alias("n_songs")
                )
                .orderBy("decade");

        analysisResult.show(20, false);

        /*
+------+------------------+-------------------+------------------+-------+
|decade|avg_positiveness  |avg_tempo          |avg_energy        |n_songs|
+------+------------------+-------------------+------------------+-------+
|NULL  |48.34549677349458 |0.5307057211577463 |62.08626585321262 |147683 |
|1900  |62.733333333333334|0.5214990138133333 |20.533333333333335|15     |
|1910  |65.5              |0.32840236685      |20.5              |2      |
|1920  |58.11538461538461 |0.5179790623576923 |23.153846153846153|26     |
|1930  |69.8409090909091  |0.530930607856818  |35.86363636363637 |44     |
|1940  |53.5              |0.4487179487208334 |26.541666666666668|24     |
|1950  |56.51605995717345 |0.46741760956702294|36.06423982869379 |467    |
|1960  |56.357489155396784|0.5096019845673139 |45.03342689461597 |3919   |
|1970  |58.07440507118362 |0.5352136492514595 |55.53465655201081 |9623   |
|1980  |57.41900237529691 |0.5393090556428004 |65.0068091844814  |12630  |
|1990  |51.152628298358614|0.516642426760858  |63.59339289424475 |24065  |
|2000  |49.48236317621006 |0.5264194350901185 |66.82916798481493 |37932  |
|2010  |43.82326616829028 |0.5341862804514738 |64.05925289090567 |175637 |
|2020  |46.043472698726525|0.5321091851375882 |60.61810780950166 |85985  |
+------+------------------+-------------------+------------------+-------+
         */

        // Save the result for later -> tableau?
        analysisResult.write()
                .mode(SaveMode.Overwrite)
                .parquet("data/processed/decade_analysis_result.parquet");

        spark.stop();
    }
}
