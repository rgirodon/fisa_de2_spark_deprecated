package net.jgp.books.spark.ch02.lab100_csv_to_db;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Transforming records.
 * 
 * @author jgp
 */
public class TP1_UsCensusTransformationApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    TP1_UsCensusTransformationApp app = new TP1_UsCensusTransformationApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creation of the session
    SparkSession spark = SparkSession.builder()
        .appName("Record transformations")
        .master("local")
        .getOrCreate();

    // Ingestion of the census data
    Dataset<Row> intermediateDf = spark
        .read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/census/PEP_2017_PEPANNRES.csv");

    // Renaming and dropping the columns we do not need
    
       // drop columns GEO.id, resbase42010, respop72011, respop72012, respop72013, respop72014, respop72015, respop72016 (tip : dataset drop method)
    
       // rename column GEO.id2 -> id, GEO.display-label -> label, rescen42010 -> real2010, respop72010 -> est2010, respop72017 -> est2017 (tip : dataset withColumnRenamed method)
    
    
    // Creates the additional columns
    
       // create column countyState as an array from column label splitted around the "," (tip : dataset withColumn and col methods, and split function)
    
          // create column state as 2nd part of column countyState (tip : column getItem method)
    
          // create column county as 2nd part of column countyState (tip : column getItem method)
    
       // create column stateId from column id (tip : expr function)
    
       // create column countyId from column id (tip : expr function)
    
    
    // Compute new columns

       // diff is est2010 - real2010 (tip : dataset withColumn method, and expr function)
    
       // growth is est2017-est2010 (tip : dataset withColumn method, and expr function)

  }
}
