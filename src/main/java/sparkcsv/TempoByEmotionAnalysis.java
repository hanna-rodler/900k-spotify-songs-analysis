package sparkcsv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

import static org.apache.spark.sql.functions.*;

public class TempoByEmotionAnalysis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Tempo by Emotion Analysis")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();


        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("multiLine", true)
                .option("escape", "\"")
                .csv("data/raw/spotify_dataset.csv");

        //only necessary columns
        Dataset<Row> filteredDf = df.select("emotion", "Tempo").na().drop();


        Dataset<Row> groupedTempo = filteredDf.groupBy("emotion")
                .agg(
                        avg("Tempo").alias("avg_tempo"),
                        min("Tempo").alias("min_tempo"),
                        max("Tempo").alias("max_tempo"),
                        stddev("Tempo").alias("stddev_tempo"),
                        count("*").alias("song_count")
                )
                .filter("song_count >= 20")
                .orderBy("avg_tempo");

        //show results
        groupedTempo.show(50, false);

        //save as parquet file
        groupedTempo.write()
                .mode(SaveMode.Overwrite)
                .option("compression", "snappy")
                .parquet("data/processed/tempo_by_emotion/");

        //only emotions with more than 20 songs
        Dataset<Row> emotionCounts = filteredDf.groupBy("emotion")
                .agg(count("*").alias("count"))
                .filter("count >= 20");


        Dataset<Row> filteredEmotions = filteredDf
                .join(emotionCounts.select("emotion"), "emotion");

        //create index column for Grafana
        Dataset<Row> distinctEmotions = filteredEmotions
                .select("emotion").distinct()
                .withColumn("emotion_index", row_number().over(Window.orderBy("emotion")).minus(1));

        Dataset<Row> indexedDf = filteredEmotions.join(distinctEmotions, "emotion");

        //save CSV for Grafana
        indexedDf
                .withColumn("emotion_index", col("emotion_index").cast("int"))
                .withColumn("Tempo", col("Tempo").cast("double"))
                .select( "emotion_index", "Tempo")
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("data/processed/for_grafana/");


        spark.stop();
    }
}
