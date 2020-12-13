package net.jgp.books.spark.ch02.lab100_csv_to_db;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * CSV to a relational database.
 * 
 * @author jgp
 */
public class CsvJoinApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    CsvJoinApp app = new CsvJoinApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
	
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("CSV Join")
        .master("local")
        .getOrCreate();

    // Step 1: Ingestion
    // ---------
   
    // Reads a local CSV file with header, called data/authors.csv, stores it in a
    // dataframe
    Dataset<Row> dfAuthors = spark.read()
        .format("csv")
        .option("header", "true")
        .load("data/authors.csv");
    
    // Reads a local CSV file with header, called data/origins.csv, stores it in a
    // dataframe
    Dataset<Row> dfOrigins = spark.read()
        .format("csv")
        .option("header", "true")
        .load("data/origins.csv");
    
    dfAuthors = dfAuthors.join(
				            dfOrigins,
				            dfAuthors.col("fname").equalTo(dfOrigins.col("fname")),
				            "left");    
    
    dfAuthors = dfAuthors.drop(dfOrigins.col("fname"));

    dfAuthors.show();
  }
}
