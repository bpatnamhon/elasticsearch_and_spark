package com.spark.elasticsearch.crawler;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CsvCrawler
{

  public static final String ELASTIC_USERNAME = "elastic";
  public static final String ELASTIC_PASSWORD = "LD6qOy8lwbsyEtVBk8D0HQjr";
  public static final String ELASTIC_HOST = "https://e5c04e747de0484b8900746f2fde90c1.eastus.azure.elastic-cloud.com";
//  public static final String ELASTIC_HOST = "https://31c511244c91434bb86ddd0ee6955bd2.westus2.azure.elastic-cloud.com";
//  public static final String ELASTIC_PASSWORD = "huCOxzoq4gPVR4sVAjFyrjDm";
  public static final String ELASTIC_PORT = "9243";
  public static final String BASE64ENCODED_SAS_TOKEN = "c3A9ciZzdD0yMDIyLTExLTA5VDA5OjQwOjM3WiZzZT0yMDIyLTExLTA5VDE3OjQwOjM3WiZzcHI9aHR0cHMmc3Y9MjAyMS0wNi0wOCZzcj1jJnNpZz1kTzhrUlU5YlFCMEFUQTM3MDlmQng2RGUlMkZ2ODlBJTJGOHZrNlRQMXFiZTR1YyUzRA==";
//  public static final String BASE64ENCODED_SAS_TOKEN =           "c3A9ciZzdD0yMDIyLTEwLTE4VDA5OjQ4OjQ4WiZzZT0yMDIyLTEwLTE4VDE3OjQ4OjQ4WiZzdj0yMDIxLTA2LTA4JnNyPWMmc2lnPU5leWxsQlAxRjk4bURwZzhoa1N3RERnbG9QdVU4OFB2cVRCYmdzenl0TjQlM0Q=";
  public static final String BLOB_STORAGE_SAS_LINK = "fs.azure.sas.fchcdnonprodmodelsync.fcmodelsyncinputfiles.privatelink.blob.core.windows.net";

  private static Logger logger = Logger.getLogger("CsvCrawler");

  @GetMapping(value = "/crawl/csv")
  public boolean save()
  {
//    String path = "C:\\niagara\\elk\\inputFiles\\exportedNodeset2.csv";
//    String path = "https://fchcdnonprodmodelsync.blob.core.windows.net/fcmodelsyncinputfiles/exportedNodeset.csv";
    String indexName = "nodeset8";

    SparkSession sparkSession = null;
    try
    {
      sparkSession = SparkSession.builder()
        .appName("studentData")
        .master("local[*]")
        .config(
          BLOB_STORAGE_SAS_LINK,
          BASE64ENCODED_SAS_TOKEN
        )
        .config("spark.es.nodes", ELASTIC_HOST)
        .config("spark.es.port", ELASTIC_PORT)
        .config("spark.es.net.http.auth.user", ELASTIC_USERNAME)
        .config("spark.es.net.http.auth.pass", ELASTIC_PASSWORD)
        .config("spark.es.nodes.wan.only","true")
        .getOrCreate();

      logger.info("========================SparkSession established successfully...");

//      sparkSession.conf().set("fs.azure.sas.fchcdnonprodmodelsync.blob.core.windows.net", "c3A9ciZzdD0yMDIyLTEwLTE4VDA5OjQ4OjQ4WiZzZT0yMDIyLTEwLTE4VDE3OjQ4OjQ4WiZzdj0yMDIxLTA2LTA4JnNyPWMmc2lnPU5leWxsQlAxRjk4bURwZzhoa1N3RERnbG9QdVU4OFB2cVRCYmdzenl0TjQlM0Q=");

      logger.info("========================SparkSession Blob storage trying to access it with PRIVATELINK...");
//          "fs.azure.account.key.fchcdnonprodmodelsync.blob.core.windows.net",
//          "sp=r&st=2022-10-18T09:48:48Z&se=2022-10-18T17:48:48Z&sv=2021-06-08&sr=c&sig=NeyllBP1F98mDpg8hkSwDDgloPuU88PvqTBbgszytN4%3D"
//      sparkSession.conf().set(
//        "fs.azure.sas.fcmodelsyncinputfiles.blob.core.windows.net",
//        "sp=r&st=2022-10-18T09:48:48Z&se=2022-10-18T17:48:48Z&sv=2021-06-08&sr=c&sig=NeyllBP1F98mDpg8hkSwDDgloPuU88PvqTBbgszytN4%3D"
//      );


      Dataset<Row> csv = sparkSession.read()
        .option("header", true)
        .option("inferSchema", "true")
//        .option(
//          "fs.azure.sas.fchcdnonprodmodelsync.fcmodelsyncinputfiles.blob.core.windows.net",
//          "c3A9ciZzdD0yMDIyLTEwLTE4VDA5OjQ4OjQ4WiZzZT0yMDIyLTEwLTE4VDE3OjQ4OjQ4WiZzdj0yMDIxLTA2LTA4JnNyPWMmc2lnPU5leWxsQlAxRjk4bURwZzhoa1N3RERnbG9QdVU4OFB2cVRCYmdzenl0TjQlM0Q="
//        )
        .csv("wasbs://fcmodelsyncinputfiles@fchcdnonprodmodelsync.privatelink.blob.core.windows.net/exportedNodesetSmall.csv");
//      csv.show();
      logger.info("Dataset form read created....");

      JavaEsSparkSQL.saveToEs(csv, indexName);
      logger.info("Saved successfully to Elasticsearch");
//      Dataset<Row> rowDataset = JavaEsSparkSQL.esDF(sparkSession, indexName);

    }
    catch (Exception e)
    {
      System.out.println("===================================");
      logger.log(Level.INFO, "Exception occurred... ", e);
      e.printStackTrace();
      System.out.println("===================================");
      throw e;
    }
    finally
    {
      if (sparkSession != null)
        sparkSession.close();
    }

    return true;
  }

  @GetMapping(value = "/crawl/csv/read")
  public boolean readFromES()
  {
    String path = "C:\\niagara\\elk\\outputFiles\\fromAzure\\nodeset1";

    SparkSession sparkSession = null;
    try
    {
      sparkSession = SparkSession.builder()
        .appName("nodeData")
        .master("local[*]")
        .config("spark.es.net.http.auth.user", ELASTIC_USERNAME)
        .config("spark.es.net.http.auth.pass", ELASTIC_PASSWORD)
        .config("spark.es.nodes.wan.only","true")
        .config("spark.es.nodes", ELASTIC_HOST)
        .config("spark.es.port", ELASTIC_PORT)
        .getOrCreate();

      DataFrameReader reader = sparkSession.read()
        .format("org.elasticsearch.spark.sql")
        .option("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        .option("spark.es.nodes","localhost")
//        .option("spark.es.port","9200");

      Dataset<Row> dataset = reader
//        .option("es.net.http.auth.user","elastic")
//        .option("es.net.http.auth.pass","feydMGPzEN9m9M5vN557")
//        .option("es.nodes.wan.only","true")
        .load("nodeset4");
//      System.out.println("No of records present: " + dataset.count());
//      dataset.show();

      Dataset<Row> csvColumnRenamed = dataset.withColumnRenamed("ExpandedNodeId", "FullNodeId");
//      csvColumnRenamed.show();

      Dataset<Row> csvColumnDropped = csvColumnRenamed.drop("ServerIndex");
      System.out.println("No of records with NamespaceIndex == 1: " + csvColumnDropped.count());
//      csvColumnDropped.show();

      Dataset<Row> filter = csvColumnDropped.filter("NamespaceIndex == 1");
      System.out.println("No of records with NamespaceIndex == 1: " + filter.count());
//      filter.show();

      dataset.write()
        .option("header", true)
        .mode(SaveMode.Overwrite)
        .csv(path);

    }
    catch (Exception e)
    {
      System.out.println("===================================");
      e.printStackTrace();
      System.out.println("===================================");
      throw e;
    }
    finally
    {
      if (sparkSession != null)
        sparkSession.close();
    }

    return true;
  }

  @GetMapping(value = "/crawl/test")
  public String helloWorld()
  {
    logger.info("Test method logger....");
    return "Sai Gagan 11";
  }
}
