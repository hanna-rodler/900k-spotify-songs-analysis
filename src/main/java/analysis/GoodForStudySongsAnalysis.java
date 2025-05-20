package analysis;

// good for Work/Study

// 1) vs. all others.
// 2) vs. [Good For Party, Good for Exercise, Running, Good for Driving, Good for Social Gathering]
// 3) similar to [Good for Relaxation/Meditation?, Good for Yoga / Stretching?]

// I have songs with different tags that are each 0 for false and 1 for true. I want to compare the songs with the tags true fo "Good for Work/Study", "Good for Relaxation/Meditation" and "Good for Yoga / Stretching" with the songs with the tags true for "Good For Party", "Good for Exercise, Running", "Good for Driving" and "Good for Social Gathering".

/*
 start:
 - tempo
 - loudness (-5.45db)
 - energy <- less
 - speechiness <- less
 - positiveness <- more
 - instrumentalness <- more
 - danceability?
 - acousticness?
 */

/* further:
 - emotion (joy, surprise, sadness
 - time signature
 - key?
 */

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.Arrays;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.spark.sql.functions.*;

public class GoodForStudySongsAnalysis {
    private static final String PARQUET_ROOT_FOLDER = "data/processed/";
    private static final String CSV_OUTPUT_PATH = "data/analysis_results/good_for_study_csv.csv";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/hadoop/");
        System.load("C:/hadoop/bin/hadoop.dll");

        SparkSession spark = SparkSession.builder()
                .appName("Danceability vs Popularity Reader")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();

        Dataset<Row> df = spark.read().parquet(PARQUET_ROOT_FOLDER + "good_for_study.parquet");

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
        /* Good for study: slower, less loud, less energetic, more positive, less speechy, more instrumental. less danceable. a lot more acousticness. little less popular.

             +--------------+------------------+------------------+-----------------+----------------+------------------+--------------------+------------------+-----------------+-----------------+
            |good_for_study|         avg_tempo|      avg_loudness|       avg_energy|avg_positiveness|   avg_speechiness|avg_instrumentalness|  avg_danceability| avg_acousticness|   avg_popularity|
            +--------------+------------------+------------------+-----------------+----------------+------------------+--------------------+------------------+-----------------+-----------------+
            |             0|0.5475057388356092|0.7699728065855758|65.68058260667085|48.2482780982806|11.971988222518666|   6.942614156915571|59.078325038463966|21.77385581942936|30.62009353217807|
            |             1|0.3373019096733573|0.6692051661966613| 27.7226389141211|32.9851029041551| 4.541719654304327|  12.360722535101607| 48.82626975681919|77.13557914406582|28.89265785990501|
            +--------------+------------------+------------------+-----------------+----------------+------------------+--------------------+------------------+-----------------+-----------------+
         */

        // TODO: check for emotions to exclude "True", "pink" and "thirst"

        System.out.println("\n--- Häufigkeit der Taktarten für 'Good for Study' Songs ---");
        Dataset<Row> timeSignatureCountsByStudyStatus = df
                .groupBy("good_for_study", "Time signature")
                .count()
                .orderBy(asc("good_for_study"), desc("count"));
        /* --- Häufigkeit der Taktarten für 'Good for Study' Songs ---
            Taktart-Verteilung für 'Good for Study' (1 = Ja, 0 = Nein):
            +--------------+--------------+------+
            |good_for_study|Time signature|count |
            +--------------+--------------+------+
            |0             |4/4           |421235|
            |0             |3/4           |28163 |
            |0             |5/4           |7488  |
            |0             |1/4           |2630  |
            |1             |4/4           |30276 |
            |1             |3/4           |6446  |
            |1             |5/4           |1058  |
            |1             |1/4           |748   |
            +--------------+--------------+------+
         */

        System.out.println("Taktart-Verteilung für 'Good for Study' (1 = Ja, 0 = Nein):");
        timeSignatureCountsByStudyStatus.show(false);
        // Gruppieren nach "Time signature" und zählen
         Dataset<Row> pivotedCounts = timeSignatureCountsByStudyStatus
             .groupBy("Time signature")
             .pivot("good_for_study", Arrays.asList(0, 1)) // Pivotieren nach good_for_study mit Werten 0 und 1
             .agg(first("count").alias("count")); // Aggregiere die Zählungen
         pivotedCounts.show(false);
        Dataset<Row> goodForStudyTimeSignatureCounts = df
                .groupBy("Time signature") // Gruppiere nach der Taktart
                .count() // Zähle die Vorkommen jeder Taktart
                .orderBy(desc("count")); // Ordne absteigend nach Häufigkeit
        goodForStudyTimeSignatureCounts.show(false);
        /*
            +--------------+------+-----+
            |Time signature|0     |1    |
            +--------------+------+-----+
            |3/4           |28163 |6446 |
            |4/4           |421235|30276|
            |5/4           |7488  |1058 |
            |1/4           |2630  |748  |
            +--------------+------+-----+
         */

        System.out.println("\n--- Häufigkeit und Prozent der Taktarten nach 'Good for Study' Status ---");

        // 1. Gesamtzahl der Songs pro 'good_for_study' Gruppe ermitteln
        Dataset<Row> totalCountsByStudyStatus = df
                .groupBy("good_for_study")
                .count()
                .withColumnRenamed("count", "total_songs_in_category"); // Benenne die Spalte um, um Konflikte zu vermeiden

        // 3. Joine die beiden DataFrames, um die Gesamtzahlen für die Prozentberechnung hinzuzufügen
        Dataset<Row> timeSignatureWithPercentageRes = timeSignatureCountsByStudyStatus.join(
                totalCountsByStudyStatus,
                "good_for_study" // Join-Spalte
        );

        // 4. Prozentsatz berechnen
        // Wichtig: Cast zu Double, um Fließkomma-Division zu erhalten
        Dataset<Row> timeSignaturePercRes = timeSignatureWithPercentageRes.withColumn(
                        "percentage",
                        round((col("count").cast("double").divide(col("total_songs_in_category"))).multiply(100), 2)
                )
                .select("good_for_study", "Time signature", "count", "percentage")
                .orderBy(asc("good_for_study"), desc("percentage"));

        System.out.println("Taktart-Verteilung (Anzahl und Prozent) für 'Good for Study' (1 = Ja, 0 = Nein):");
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

        System.out.println("\n--- Häufigkeit der Taktarten für 'Good for Study' Songs ---");
        Dataset<Row> emotionRes = df
                .groupBy("good_for_study", "emotion") // Gruppiere nach beiden Spalten
                .count() // Zähle die Vorkommen jeder Kombination
                .orderBy(asc("good_for_study"), desc("count"));

        emotionRes.show();
        /*
            +--------------+---------+------+
            |good_for_study|  emotion| count|
            +--------------+---------+------+
            |             0|      joy|174092|
            |             0|  sadness|142959|
            |             0|    anger| 91586|
            |             0|     fear| 23925|
            |             0|     love| 22403|
            |             0| surprise|  4529|
            |             0|     True|    14|
            |             0|     pink|     2|
            |             0|     Love|     2|
            |             0|   thirst|     1|
            |             0| interest|     1|
            |             0|confusion|     1|
            |             0|    angry|     1|
            |             1|      joy| 15276|
            |             1|  sadness| 13849|
            |             1|    anger|  3903|
            |             1|     love|  2976|
            |             1|     fear|  2076|
            |             1| surprise|   445|
            |             1|     True|     3|
            +--------------+---------+------+
         */


        // KEYS
        System.out.println("\n--- Häufigkeit der Taktarten für 'Good for Study' Songs ---");
        Dataset<Row> keyRes = df
                .groupBy("good_for_study", "key") // Gruppiere nach beiden Spalten
                .count();
//                .orderBy(asc("good_for_study"), desc("count"));


        // 2. Joine mit den bereits berechneten Gesamtzahlen pro good_for_study-Kategorie
        // Wir können totalCountsByStudyStatus hier wiederverwenden!
        Dataset<Row> keyResWithTotalCounts = keyRes.join(
                totalCountsByStudyStatus,
                "good_for_study"
        );

        // 3. Prozentsatz berechnen und Spalten auswählen/ordnen
        Dataset<Row> keysPercRes = keyResWithTotalCounts.withColumn(
                        "percentage",
                        round((col("count").cast("double").divide(col("total_songs_in_category"))).multiply(100), 2)
                )
                .select("good_for_study", "key", "count", "percentage")
                .orderBy(asc("good_for_study"), desc("percentage"));

        System.out.println("Tonart-Verteilung (Anzahl und Prozent) für 'Good for Study' (1 = Ja, 0 = Nein):");
        keysPercRes.show(false);


        results.write().mode(SaveMode.Overwrite).parquet("data/analysis_results/good_for_study.parquet");
        System.out.println("\nSpeichere Ergebnisse als CSV-Datei unter: " + CSV_OUTPUT_PATH);

        // Optional: Vorhandenes Verzeichnis löschen, um Fehler zu vermeiden
        try {
            File outputDir = new File(CSV_OUTPUT_PATH);
            if (outputDir.exists()) {
                deleteDirectory(outputDir);
            }
        } catch (Exception e) {
            System.err.println("Fehler beim Löschen alter CSV-Ausgabeverzeichnisse: " + e.getMessage());
        }
        results.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(CSV_OUTPUT_PATH);

        // time Sig
        timeSignaturePercRes.write().mode(SaveMode.Overwrite).parquet("data/analysis_results/good_for_study_time_signature.parquet");
        timeSignaturePercRes.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("data/analysis_results/good_for_study_with_time_signature.csv");

        // emotion save
        emotionRes.write().mode(SaveMode.Overwrite).parquet("data/analysis_results/good_for_study_emotion.parquet");
        emotionRes.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("data/analysis_results/good_for_study_emotion.csv");

        // key sav
        keysPercRes.write().mode(SaveMode.Overwrite).parquet("data/analysis_results/good_for_study_keys.parquet");
        keysPercRes.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("data/analysis_results/good_for_study_keys.csv");



        /* -------------------
            TAG COMPARISON
         -------------------------- */

        Dataset<Row> tagCompDf = spark.read().parquet(PARQUET_ROOT_FOLDER + "good_for_study_tag_comp.parquet");

        Dataset<Row> goodForStudySongs = tagCompDf.filter(
                col("good_for_study").equalTo(1));
        System.out.println("Study/Work songs " + goodForStudySongs.count());
        goodForStudySongs.show(5, false);
        Dataset<Row> goodForStudySongsRes = calcAverages(goodForStudySongs);
        goodForStudySongsRes.show();
        goodForStudySongs.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("data/analysis_results/good_for_study_songs.csv");
/*
         just for good for study = 38531 songs
            +--------------+------------------+------------------+----------------+----------------+-----------------+--------------------+-----------------+-----------------+-----------------+
            |good_for_study|         avg_tempo|      avg_loudness|      avg_energy|avg_positiveness|  avg_speechiness|avg_instrumentalness| avg_danceability| avg_acousticness|   avg_popularity|
            +--------------+------------------+------------------+----------------+----------------+-----------------+--------------------+-----------------+-----------------+-----------------+
            |             1|0.3373019096733573|0.6692051661966613|27.7226389141211|32.9851029041551|4.541719654304327|  12.360722535101607|48.82626975681919|77.13557914406582|28.89265785990501|
            +--------------+------------------+------------------+----------------+----------------+-----------------+--------------------+-----------------+-----------------+-----------------+
*/


        // calm songs = good for study/work, relaxation/meditation, yoga/stretching
        Dataset<Row> goodForCalmSongs = tagCompDf.filter(
                col("good_for_study").equalTo(1)
                .and(col("good_for_relaxation_meditation").equalTo(1))
                .and(col("good_for_yoga_stretching").equalTo(1))
        );
        System.out.println("Calm songs " + goodForCalmSongs.count());
        goodForCalmSongs.show(5, false);
        Dataset<Row> goodForCalmSongsRes = calcAverages(goodForCalmSongs);
        goodForCalmSongsRes.show();
        goodForCalmSongs.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("data/analysis_results/good_for_calmness_songs.csv");
/*
         good for study/work, relaxation/meditation, yoga/stretching =  10876 songs
            +------------------+------------------+------------------+-----------------+-----------------+--------------------+-----------------+----------------+------------------+
            |         avg_tempo|      avg_loudness|        avg_energy| avg_positiveness|  avg_speechiness|avg_instrumentalness| avg_danceability|avg_acousticness|    avg_popularity|
            +------------------+------------------+------------------+-----------------+-----------------+--------------------+-----------------+----------------+------------------+
            |0.2752545640897837|0.6121784544538358|16.268205222508275|24.97122103714601|4.408789996322177|   19.28714600956234|40.82603898492093|87.8843324751747|28.730323648400148|
            +------------------+------------------+------------------+-----------------+-----------------+--------------------+-----------------+----------------+------------------+

*/


        // party songs
        Dataset<Row> goodForPartySongs = tagCompDf.filter(
                col("good_for_party").equalTo(1));
        Dataset<Row> goodPartySongsAvgs = calcAverages(goodForPartySongs);
        System.out.println("Good for partying songs" + goodForPartySongs.count());
        goodPartySongsAvgs.show();
        goodForPartySongs.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("data/analysis_results/good_for_party_songs.csv");
/*
         just for good for party = 25719 songs
            +--------------+------------------+------------------+-----------------+-----------------+------------------+--------------------+-----------------+------------------+-----------------+
            |good_for_party|         avg_tempo|      avg_loudness|       avg_energy| avg_positiveness|   avg_speechiness|avg_instrumentalness| avg_danceability|  avg_acousticness|   avg_popularity|
            +--------------+------------------+------------------+-----------------+-----------------+------------------+--------------------+-----------------+------------------+-----------------+
            |             1|0.6588638565527887|0.8014300858473167|77.74124188343248|60.13491970916443|11.433609393833352|   4.988024417745636|66.67697033321669|12.488860375597808|55.23360161748124|
            +--------------+------------------+------------------+-----------------+-----------------+------------------+--------------------+-----------------+------------------+-----------------+

*/

        // active songs = good for party, running, exercise
        Dataset<Row> goodForActiveSongs = tagCompDf.filter(
                col("good_for_party").equalTo(1)
                .and(col("good_for_running").equalTo(1))
                .and(col("good_for_exercise").equalTo(1))
        ).cache();
        System.out.println("Active songs" + goodForActiveSongs.count());
        goodForActiveSongs.show(5, false);
        Dataset<Row> goodForActiveSongsRes = calcAverages(goodForActiveSongs);
        goodForActiveSongsRes.show();
        goodForActiveSongs.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("data/analysis_results/good_for_activities_songs.csv");
        /* good for party, running, exercise = 7683 songs
            +------------------+------------------+-----------------+-----------------+------------------+--------------------+-----------------+------------------+------------------+
            |         avg_tempo|      avg_loudness|       avg_energy| avg_positiveness|   avg_speechiness|avg_instrumentalness| avg_danceability|  avg_acousticness|    avg_popularity|
            +------------------+------------------+-----------------+-----------------+------------------+--------------------+-----------------+------------------+------------------+
            |0.7533361521301009|0.8112882867359859|82.64675257061045|62.94598464141611|14.053624886112196|  4.1918521410907195|62.81205258362619|11.553689964857478|54.758557855004554|
            +------------------+------------------+-----------------+-----------------+------------------+--------------------+-----------------+------------------+------------------+
         */

        spark.stop();
    }

    private static Dataset<Row> calcAverages(Dataset<Row> df) {
        return df
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
