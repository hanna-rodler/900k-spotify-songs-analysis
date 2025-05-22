package analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.spark.sql.functions.*;

public class GoodForStudySongsAnalysis {
    private static final String PARQUET_ROOT_FOLDER = "data/processed/";
    private static final String OUTPUT_PATH = "data/analysis_results/";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/hadoop/");
        System.load("C:/hadoop/bin/hadoop.dll");

        SparkSession spark = SparkSession.builder()
                .appName("Danceability vs Popularity Reader")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();

        Dataset<Row> df = spark.read().parquet(PARQUET_ROOT_FOLDER + "good_for_study.parquet");

        /* -------------------------------------------------------
         * compare good for study with not good for study songs
         ------------------------------------------------------- */
        Dataset<Row> results = df.groupBy("good_for_study")
                .agg(
                        avg("Tempo").alias("avg_tempo"),
                        avg("Loudness (db)").alias("avg_loudness"),
                        avg("Energy").alias("avg_energy"),
                        avg("Positiveness").alias("avg_positiveness"),
                        avg("Speechiness").alias("avg_speechiness"),
                        avg("Instrumentalness").alias("avg_instrumentalness"),
                        avg("Danceability").alias("avg_danceability"),
                        avg("Acousticness").alias("avg_acousticness"),
                        avg("Popularity").alias("avg_popularity")
                );

        results.show();

        System.out.println("\n--- Häufigkeit der Taktarten für 'Good for Study' Songs ---");
        Dataset<Row> timeSignatureCountsByStudyStatus = df
                .groupBy("good_for_study", "Time signature")
                .count()
                .orderBy(asc("good_for_study"), desc("count"));

        System.out.println("Taktart-Verteilung für 'Good for Study':");
        timeSignatureCountsByStudyStatus.show(false);

/*         Dataset<Row> pivotedCounts = timeSignatureCountsByStudyStatus
             .groupBy("Time signature")
             .pivot("good_for_study", Arrays.asList(0, 1))
             .agg(first("count").alias("count"));
         pivotedCounts.show(false);*/
        Dataset<Row> goodForStudyTimeSignatureCounts = df
                .groupBy("Time signature")
                .count()
                .orderBy(desc("count"));
        goodForStudyTimeSignatureCounts.show(false);

        System.out.println("\n--- Häufigkeit und Prozent der Taktarten nach 'Good for Study' Status ---");
        // sum per good_for_study group
        Dataset<Row> totalCountsByStudyStatus = df
                .groupBy("good_for_study")
                .count()
                .withColumnRenamed("count", "total_songs_in_category");

        // join df with total number
        Dataset<Row> timeSignatureWithPercentageRes = timeSignatureCountsByStudyStatus.join(
                totalCountsByStudyStatus,
                "good_for_study"
        );

        // calc percentage
        Dataset<Row> timeSignaturePercRes = timeSignatureWithPercentageRes.withColumn(
                        "percentage",
                        round((col("count").cast("double").divide(col("total_songs_in_category"))).multiply(100), 2)
                )
                .select("good_for_study", "Time signature", "count", "percentage")
                .orderBy(asc("good_for_study"), desc("percentage"));

        System.out.println("Taktart-Verteilung (Anzahl und Prozent) für 'Good for Study':");
        timeSignaturePercRes.show(false);
        /*
        +--------------+--------------+------+----------+
        |good_for_study|Time signature|count |percentage|
        +--------------+--------------+------+----------+
        |0             |4/4           |421235|91.67     |
        |0             |3/4           |28163 |6.13      |
        |0             |5/4           |7488  |1.63      |
        |0             |1/4           |2630  |0.57      |
        |1             |4/4           |30276 |78.58     |
        |1             |3/4           |6446  |16.73     |
        |1             |5/4           |1058  |2.75      |
        |1             |1/4           |748   |1.94      |
        +--------------+--------------+------+----------+
         */

        System.out.println("\nHäufigkeit der Emotionen für 'Good for Study' Songs");
        Dataset<Row> emotionRes = df
                .groupBy("good_for_study", "emotion")
                .count()
                .orderBy(asc("good_for_study"), desc("count"));

        emotionRes.show();


        // KEYS
        System.out.println("\n--- Häufigkeit der Keys für 'Good for Study' Songs ---");
        Dataset<Row> keyRes = df
                .groupBy("good_for_study", "key")
                .count();
        // calc key percentags
        Dataset<Row> keyResWithTotalCounts = keyRes.join(
                totalCountsByStudyStatus,
                "good_for_study"
        );
        Dataset<Row> keysPercRes = keyResWithTotalCounts.withColumn(
                        "percentage",
                        round((col("count").cast("double").divide(col("total_songs_in_category"))).multiply(100), 2)
                )
                .select("good_for_study", "key", "count", "percentage")
                .orderBy(asc("good_for_study"), desc("percentage"));

        System.out.println("Key-Verteilung für 'Good for Study':");
        keysPercRes.show(false);
        
        results.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(OUTPUT_PATH +"good_for_study.csv");
        results.write().mode(SaveMode.Overwrite).parquet(OUTPUT_PATH +"good_for_study.parquet");

        // time Signature
//        timeSignaturePercRes.write().mode(SaveMode.Overwrite).parquet(OUTPUT_PATH +"good_for_study_time_signature.parquet");
        timeSignaturePercRes.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(OUTPUT_PATH +"good_for_study_with_time_signature.csv");

        // emotion
//        emotionRes.write().mode(SaveMode.Overwrite).parquet(OUTPUT_PATH +"good_for_study_emotion.parquet");
        emotionRes.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(OUTPUT_PATH +"good_for_study_emotion.csv");

        // key
//        keysPercRes.write().mode(SaveMode.Overwrite).parquet(OUTPUT_PATH +"good_for_study_keys.parquet");
        keysPercRes.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(OUTPUT_PATH +"good_for_study_keys.csv");



        /* -------------------
            TAG COMPARISON
         -------------------------- */

        Dataset<Row> tagCompDf = spark.read().parquet(PARQUET_ROOT_FOLDER + "good_for_study_tag_comp.parquet");

        // just good for study
        Dataset<Row> goodForStudySongs = tagCompDf.filter(
                col("good_for_study").equalTo(1))
                .withColumn("tagged_as", lit("good for study"))
                .drop("good_for_study");
        System.out.println("Study/Work songs " + goodForStudySongs.count());
        goodForStudySongs.show(5, false);
        Dataset<Row> goodForStudySongsRes = calcAverages(goodForStudySongs, "tagged_as");
        goodForStudySongsRes.show();
        goodForStudySongsRes.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(OUTPUT_PATH +"good_for_study_songs.csv");


        // CALM songs = good for study/work, relaxation/meditation, yoga/stretching
        Dataset<Row> goodForCalmSongs = tagCompDf.filter(
                        col("good_for_study").equalTo(1)
                                .and(col("good_for_relaxation_meditation").equalTo(1))
                                .and(col("good_for_yoga_stretching").equalTo(1))
                )
                .withColumn("tagged_as", lit("good for calm"))
                .drop("good_for_study")
                .drop("good_for_relaxation_meditation")
                .drop("good_for_yoga_stretching");
        System.out.println("Calm songs " + goodForCalmSongs.count());
        Dataset<Row> goodForCalmSongsRes = calcAverages(goodForCalmSongs, "tagged_as");
        goodForCalmSongsRes.show(5, false);

        // just PARTY songs
        Dataset<Row> goodForPartySongs = tagCompDf.filter(
                col("good_for_party").equalTo(1))
                .withColumn("tagged_as", lit("good for party"))
                .drop("good_for_party");
        Dataset<Row> goodPartySongsRes = calcAverages(goodForPartySongs, "tagged_as");
        System.out.println("Good for partying songs" + goodForPartySongs.count());
        goodPartySongsRes.show();

        // ACTIVE songs = good for party, running, exercise
        Dataset<Row> goodForActiveSongs = tagCompDf.filter(
                col("good_for_party").equalTo(1)
                .and(col("good_for_running").equalTo(1))
                .and(col("good_for_exercise").equalTo(1))
        ).withColumn("tagged_as", lit("good for activities"))
                .drop("good_for_party")
                .drop("good_for_running")
                .drop("good_for_exercise");
        System.out.println("Active songs" + goodForActiveSongs.count());
        goodForActiveSongs.show(5, false);
        Dataset<Row> goodForActiveSongsRes = calcAverages(goodForActiveSongs, "tagged_as");
        goodForActiveSongsRes.show();

        Dataset<Row> tagComparison = goodForStudySongsRes
                .unionByName(goodForCalmSongsRes)
                        .unionByName(goodForActiveSongsRes)
                                .unionByName(goodPartySongsRes);

        tagComparison.show(5, false);
        tagComparison
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(OUTPUT_PATH +"tag_comparison.csv");


        spark.stop();
    }

    private static Dataset<Row> calcAverages(Dataset<Row> df, String groupedBy) {
        return df
                .groupBy(groupedBy)
                .agg(
                        avg("Tempo").alias("avg_tempo"),
                        avg("Loudness (db)").alias("avg_loudness"),
                        avg("Energy").alias("avg_energy"),
                        avg("Positiveness").alias("avg_positiveness"),
                        avg("Speechiness").alias("avg_speechiness"),
                        avg("Instrumentalness").alias("avg_instrumentalness"),
                        avg("Danceability").alias("avg_danceability"),
                        avg("Acousticness").alias("avg_acousticness"),
                        avg("Popularity").alias("avg_popularity")
                );
    }
}
