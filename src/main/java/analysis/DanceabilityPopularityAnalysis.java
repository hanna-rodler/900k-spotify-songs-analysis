package analysis;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;

public class DanceabilityPopularityAnalysis {
    private static final String PARQUET_ROOT_FOLDER = "data/processed/";

    public static void main(String[] args) {
        System.setProperty ("hadoop.home.dir", "C:/hadoop/" );
        System.load ("C:/hadoop/bin/hadoop.dll");

        SparkSession spark = SparkSession.builder()
                .appName("Danceability vs Popularity Reader")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();

        Dataset<Row> df = spark.read().parquet(PARQUET_ROOT_FOLDER + "popularity.parquet");

        // CORRELATION
        double correlation = df.stat().corr("popularity", "danceability");
        System.out.println("Correlation: " + correlation);
        /* Correlation: 0.06850174197717052 <-- basically none*/

        // Tempo - just slight negative correlation
        double correlationTempo = df.stat().corr("tempo", "danceability");
        System.out.println("Tempo Correlation: " + correlationTempo);
        /*Tempo Correlation: -0.11193402575843812*/

        // OUTPUT with SQL Spark
        df.createOrReplaceTempView("songs");
        Dataset<Row> result = spark.sql(
                "SELECT popularity_bucket, AVG(danceability) AS avg_danceability " +
                        "FROM songs " +
                        "GROUP BY popularity_bucket " +
                        "ORDER BY popularity_bucket"
        );

        result.show(false);
        /*+-----------------+------------------+
        |popularity_bucket|avg_danceability  |
        +-----------------+------------------+
        |0-25             |57.513114536148066|
        |26-50            |58.04001088731628 |
        |51-75            |61.4400484384037  |
        |75-100           |63.66382978723404 |
        +-----------------+------------------+*/

        Dataset<Row> smallerPartitionedDf = spark.read().parquet(PARQUET_ROOT_FOLDER + "popularity_bucket_smaller.parquet");

        // OUTPUT with SQL Spark
        smallerPartitionedDf.createOrReplaceTempView("songs");
        Dataset<Row> resultSmallerPartitioned = spark.sql(
                "SELECT popularity_bucket, AVG(danceability) AS avg_danceability " +
                        "FROM songs " +
                        "GROUP BY popularity_bucket " +
                        "ORDER BY popularity_bucket"
        );

        /*+-----------------+------------------+
        |popularity_bucket|avg_danceability  |
        +-----------------+------------------+
        |0-10             |58.480713218820014|
        |11-20            |57.44631822180766 |
        |21-30            |56.87233808483553 |
        |31-40            |57.895461342682   |
        |41-50            |59.484671059369774|
        |51-60            |60.91297422992387 |
        |61-70            |62.14932802389249 |
        |71-80            |62.28346456692913 |
        |81-90            |64.86210892236384 |
        |91-100           |70.97142857142858 |
        +-----------------+------------------+
        */

        resultSmallerPartitioned.show(false);
    }

}
