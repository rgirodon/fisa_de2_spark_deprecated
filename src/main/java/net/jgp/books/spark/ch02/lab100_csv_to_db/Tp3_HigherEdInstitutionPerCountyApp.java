package net.jgp.books.spark.ch02.lab100_csv_to_db;

import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.element_at;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Tp3_HigherEdInstitutionPerCountyApp {

	/**
	   * main() is your entry point to the application.
	   * 
	   * @param args
	   */
	  public static void main(String[] args) {
		  
		  Tp3_HigherEdInstitutionPerCountyApp app = new Tp3_HigherEdInstitutionPerCountyApp();
	    
		  app.start();
	  }

	  /**
	   * The processing code.
	   */
	  private void start() {
	    // Creation of the session
	    SparkSession spark = SparkSession
	    						.builder()
						        .appName("Tp3_HigherEdInstitutionPerCountyApp")
						        .master("local")
						        .getOrCreate();

	    // Ingestion of the census data
	    Dataset<Row> censusDf = spark
	    							.read()
							        .format("csv")
							        .option("header", "true")
							        .option("inferSchema", "true")
							        .option("encoding", "cp1252")
							        .load("data/census/PEP_2017_PEPANNRES.csv");
	     	    
	    // Renaming and dropping the columns we do not need
	    
	       // drop columns GEO.id, resbase42010, respop72010, respop72011, respop72012, respop72013, respop72014, respop72015, respop72016 (tip : dataset drop method)
	    
	       // rename column GEO.id2 -> countyId, GEO.display-label -> county, respop72017 -> pop2017 (tip : dataset withColumnRenamed method)

	    
	    // Higher education institution
	    Dataset<Row> higherEdDf = spark
							        .read()
							        .format("csv")
							        .option("header", "true")
							        .option("inferSchema", "true")
							        .load("data/dapip/InstitutionCampus.csv");	    
	    
	    // Keep only lines with column LocationType == 'Institution' (tip : dataset filter method) 
	    
	    // Create column "addressElements" as the split of column "Address" around space character (tip : dataset split method)
	    
	    // Create column "addressElementCount" as the size of column "addressElements" (tip : dataset size method)
	    
	    // Create column "zip9" as the last element of column "addressElements" (tip : dataset element_at method)
	    
	    // Create column "splitZipCode" as the split of column "zip9" around "-" character (tip : dataset split method)
	    
	    higherEdDf = higherEdDf
				        .withColumn("zip", higherEdDf.col("splitZipCode").getItem(0))
				        .withColumnRenamed("LocationName", "location")
				        .drop("DapipId")
				        .drop("OpeId")
				        .drop("ParentName")
				        .drop("ParentDapipId")
				        .drop("LocationType")
				        .drop("Address")
				        .drop("GeneralPhone")
				        .drop("AdminName")
				        .drop("AdminPhone")
				        .drop("AdminEmail")
				        .drop("Fax")
				        .drop("UpdateDate")
				        .drop("zip9")
				        .drop("addressElements")
				        .drop("addressElementCount")
				        .drop("splitZipCode");
	    
	    // Add column zip as 1st element of column "splitZipCode"
	    
	    // Renaming and dropping the columns we do not need
	    
	       // drop columns DapipId, OpeId, ParentName, ParentDapipId, LocationType, Address, GeneralPhone, AdminName, AdminPhone, AdminEmail, Fax, UpdateDate, zip9, addressElements, addressElementCount, splitZipCode (tip : dataset drop method)
	    
	       // rename column LocationName -> location (tip : dataset withColumnRenamed method)	    

	    // Zip to county
	    Dataset<Row> countyZipDf = spark
							        .read()
							        .format("csv")
							        .option("header", "true")
							        .option("inferSchema", "true")
							        .load("data/hud/COUNTY_ZIP_092018.csv");

	    // drop columns res_ratio, bus_ratio, oth_ratio, tot_ratio
	    
	    // Create dataset institPerCountyDf as the Join of datasets higherEdDf and countyZipDf on column zip (inner join)
	    
	    // Join dataset institPerCountyDf and censusDf on column county (and countyId) (left join)

	    // Final clean up
	    
	    	// drop columns zip, county, countyId
	    
	    	// drop duplicate lines (with distinct method)
	  }
}
