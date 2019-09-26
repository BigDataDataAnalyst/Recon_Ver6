import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import scala.xml.XML
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col,lit}
import org.apache.spark.storage.StorageLevel
import java.io._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf


object Recon{
  def main(args: Array[String]) 
  {
    val InputJobName = args(0)
    val WorkingDirectory =args(1)
    if (InputJobName != "" && WorkingDirectory != "")
    {
    val JobLogicalAppName = "Recon_" + InputJobName
    val ParentConfXMLPath = WorkingDirectory + "ParentConfig.xml"
    val ParentConfXML  = XML.load (ParentConfXMLPath)
    val ReportingPath = (ParentConfXML \ "ReportingDirPath").text
    val DSSDirectory = (ParentConfXML \ "DSSInputDirectory").text
    val DHSDirectory = (ParentConfXML \ "DHSInputDirectory").text
    val DSSFirstRowHeader = (ParentConfXML \ "DSSFirstRowHeader").text
    val DHSFirstRowHeader = (ParentConfXML \ "DHSFirstRowHeader").text
    val DSSDelimiter = (ParentConfXML \ "DSSDelimiter").text
    val DHSDelimiter = (ParentConfXML \ "DHSDelimiter").text
    val DSSFullFileName = DSSDirectory + InputJobName + ".csv"
    val DHSFullFileName = DHSDirectory + InputJobName + ".csv"
    val ReportingPathFull = "file://" + ReportingPath
    val summaryFilePath =  ReportingPath + InputJobName + "_Summary.txt"
    val ReportFilePath =   ReportingPath + InputJobName + "_Report.csv"
    val storageLevel = "StorageLevel.MEMORY_ONLY_SER"
    //val TimeStampFormat = "dd/MM/yyyy HH:mm:ss"
    val DSSTimeStampFormat = (ParentConfXML \ "DSSTimeStampFormat").text
    val DHSTimeStampFormat = (ParentConfXML \ "DHSTimeStampFormat").text
    val sparkMaster = (ParentConfXML \ "SparkMaster").text 
    //val spark = SparkSession.builder().master("spark://"+ sparkMaster).appName(JobLogicalAppName).getOrCreate() 
    val spark = SparkSession.builder().master(sparkMaster).appName(JobLogicalAppName).getOrCreate() 
    println("************************************************")
    println("JobConfXMLPath    : "+ ParentConfXMLPath)
    println("InputJobName      : "+ InputJobName)
    println("WorkingDirectory  : "+ WorkingDirectory)
    println("sparkMaster       : "+ sparkMaster)    
    println("DSSDirectory      : "+ DSSDirectory )
    println("DHSDirectory      : "+ DHSDirectory )
    println("DHSDirectory      : "+ DHSDirectory )
    println("DSSFirstRowHeader : "+ DSSFirstRowHeader )
    println("DHSFirstRowHeader : "+ DHSFirstRowHeader ) 
    println("DSSDelimiter      : "+ DSSDelimiter )
    println("DHSDelimiter      : "+ DHSDelimiter )
    println("DSSFullFileName   : "+ DSSFullFileName )
    println("DHSFullFileName   : "+ DHSFullFileName )
    println("ReportingPath     : "+ ReportingPath )
    println("ReportingPathFull : "+ ReportingPathFull )
    println("summaryFilePath   : "+ summaryFilePath )
    println("ReportFilePath    : "+ ReportFilePath )
    println("storageLevel      : "+ storageLevel )
    //println("TimeStampFormat.  : "+ TimeStampFormat)
    println("DSSTimeStampFormat  : "+ DSSTimeStampFormat)
    println("DHSTimeStampFormat  : "+ DHSTimeStampFormat)
    println("************************************************")
    val schema=new StructType().add("ROW_ID","string").add("LAST_UPD","timestamp")
    val dssDF = spark.read.options(Map("inferSchema"->"false","sep"->DSSDelimiter,"header"->DSSFirstRowHeader,"timestampFormat"-> DSSTimeStampFormat )).schema(schema).csv(DSSFullFileName).toDF("DSS_ROW_ID","DSS_LAST_UPD")
    dssDF.printSchema()

    val dhsDF = spark.read.options(Map("inferSchema"->"false","sep"->DHSDelimiter,"header"->DSSFirstRowHeader,"timestampFormat"-> DHSTimeStampFormat )).schema(schema).csv(DHSFullFileName).toDF("DHS_ROW_ID","DHS_LAST_UPD")
    dhsDF.printSchema()
    val joinedDF=dssDF.join(dhsDF,(col("DSS_ROW_ID") === col("DHS_ROW_ID")),"fullouter").persist(StorageLevel.MEMORY_AND_DISK_SER)
    //val exactmatchCount=joinedDF.filter(col("DSS_LAST_UPD") === col("DHS_LAST_UPD")).count
    val file = new File(summaryFilePath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("Input DSS records: " + dssDF.count + "\r\n")
    bw.write("Input DHS records: " + dhsDF.count + "\r\n")
    bw.write("Exact matches: " + joinedDF.filter((col("DSS_LAST_UPD") === col("DHS_LAST_UPD"))).count + "\r\n")
    val diffDF = joinedDF.filter((col("DHS_ROW_ID") isNull) || (col("DSS_ROW_ID") isNull) || (col("DSS_LAST_UPD") > col("DHS_LAST_UPD")) || (col("DSS_LAST_UPD") < col("DHS_LAST_UPD"))).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val missingDSSDF=diffDF.filter(col("DSS_ROW_ID") isNull).select(col("DHS_ROW_ID") as "ROW_ID",col("DSS_LAST_UPD"),col("DHS_LAST_UPD"),lit("Record found in DHS extract but not found in DSS extract  -  requires investigation") as "Result")

    val missingDHSDF=diffDF.filter(col("DHS_ROW_ID") isNull).select(col("DSS_ROW_ID") as "ROW_ID",col("DSS_LAST_UPD"),col("DHS_LAST_UPD"),lit("Record not found in DHS extract") as "Result")
    val DHSstaleDF=diffDF.filter(col("DHS_LAST_UPD") < col("DSS_LAST_UPD")).select(col("DSS_ROW_ID") as "ROW_ID",col("DSS_LAST_UPD"),col("DHS_LAST_UPD"),lit("Record found in DHS extract but not the latest version") as "Result")
    val DSSstaleDF=diffDF.filter(col("DHS_LAST_UPD") > col("DSS_LAST_UPD")).select(col("DSS_ROW_ID") as "ROW_ID",col("DSS_LAST_UPD"),col("DHS_LAST_UPD"),lit("Record found in DHS extract but version mismatch  -  requires investigation") as "Result")
    var diffFileDF = missingDSSDF.union(missingDHSDF).union(DHSstaleDF).union(DSSstaleDF)
    bw.write("In DHS extract but not found in DSS extract  -  requires investigation: " + missingDSSDF.count + "\r\n")
    bw.write("Records not found in DHS extract: " + missingDHSDF.count + "\r\n")
    bw.write("Records found in DHS extract but not the latest version: " + DHSstaleDF.count + "\r\n")
    bw.write("Records found in DHS extract but version mismatch  -  requires investigation: " + DSSstaleDF.count + "\r\n")
    bw.close()
    joinedDF.unpersist()
    diffFileDF.coalesce(1).write.mode("overwrite").option("header", "true").option("inferSchema","true").option("timestampFormat", "dd/MM/yyyy HH:mm:ss").csv(ReportFilePath) 
    spark.sqlContext.clearCache()
    }
    else
    {
    println("************************************************************************************")
    println("******* ERROR : Missing  Required Job Input Argumetns")
    println("************************************************************************************")
    }
  }
}

