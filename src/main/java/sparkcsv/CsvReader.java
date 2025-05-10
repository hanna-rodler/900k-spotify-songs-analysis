package sparkcsv;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.Arrays;

public class CsvReader {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CSV with ID")
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


        JavaRDD<Row> rddWithIndex = df.javaRDD().zipWithIndex().map(tuple -> {
            Row originalRow = tuple._1;
            Long index = tuple._2 + 1;
            Object[] newRow = new Object[originalRow.size() + 1];
            newRow[0] = index;
            for (int i = 0; i < originalRow.size(); i++) {
                newRow[i + 1] = originalRow.get(i);
            }
            return RowFactory.create(newRow);
        });


        StructType oldSchema = df.schema();
        StructField[] oldFields = oldSchema.fields();
        StructField[] newFields = new StructField[oldFields.length + 1];
        newFields[0] = new StructField("id", DataTypes.LongType, false, Metadata.empty());
        System.arraycopy(oldFields, 0, newFields, 1, oldFields.length);
        StructType newSchema = new StructType(newFields);

        Dataset<Row> finalDf = spark.createDataFrame(rddWithIndex, newSchema);

        //output
        finalDf.printSchema();
        finalDf.show(20, false);

        spark.stop();
    }
}
