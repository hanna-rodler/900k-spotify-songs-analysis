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

        // Filter out Null Values -> 147683 songs do not have a Release Date
        Dataset<Row> filteredDF = parquetDF.filter(col("decade").isNotNull());

        // DataFrame API
        Dataset<Row> analysisResult = filteredDF.groupBy("decade")
                .agg(
                        avg("Positiveness").alias("avg_positiveness"),
                        avg(col("Tempo").multiply(100)).alias("avg_tempo"),
                        avg("Energy").alias("avg_energy"),
                        count("*").alias("n_songs")
                )
                .orderBy("decade");

        analysisResult.show(20, false);

        // Save as parquet file
        analysisResult.write()
                .mode(SaveMode.Overwrite)
                .parquet("data/processed/decade_analysis_result.parquet");

        // Save as CSV
        analysisResult.write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv("data/processed/decade_analysis_result.csv");

        spark.stop();
    }
}
