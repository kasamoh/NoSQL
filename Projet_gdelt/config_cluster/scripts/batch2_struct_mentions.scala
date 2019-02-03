import sys.process._
import java.net.URL
import java.io.File
import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.net.HttpURLConnection 
import org.apache.spark.sql.functions._

import org.apache.spark.input.PortableDataStream
import java.util.zip.ZipInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.spark.{SparkConf, SparkContext}

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.types._;
import org.apache.spark.sql._;
import org.apache.spark.sql.SQLContext
import java.sql.Date
import java.text.SimpleDateFormat

import scala.util.Try

    
val AWS_ID = "AKIAJNW545DSRAJK6IOQ"
val AWS_KEY = "OrTgAeLFrgL76/+7XFieHC4hoz9gtFZpNx4lCnW/"
val awsClient = new AmazonS3Client(new BasicAWSCredentials(AWS_ID, AWS_KEY))

sc.hadoopConfiguration.set("fs.s3a.access.key", AWS_ID) // mettre votre ID du fichier credentials.csv
sc.hadoopConfiguration.set("fs.s3a.secret.key", AWS_KEY) // mettre votre secret du fichier credentials.csv


val textRDD = sc.binaryFiles("s3a://gdelt-tdenimal/201812.mentions.CSV.zip"). // charger quelques fichers via une regex
   flatMap {  // decompresser les fichiers
       case (name: String, content: PortableDataStream) =>
          val zis = new ZipInputStream(content.open)
          Stream.continually(zis.getNextEntry).
                takeWhile(_ != null).
                flatMap { _ =>
                    val br = new BufferedReader(new InputStreamReader(zis))
                    Stream.continually(br.readLine()).takeWhile(_ != null)
                }
    }

def convertExportarrayToRow (array: Array[String]) : Row = {
    //recuperer les fields et les transformer en Row

    // var r :Seq[Any]= Seq( array(0).toLong ,array(1).toInt, array(2).toInt, array(3).toInt)
    var r :Seq[Any] = Seq()
    
    //GlobalEventID
    r = r :+ array(0).toLong
    // // EventTimeDate.
    val dateFormat = new SimpleDateFormat("yyyyMMddhhmmss")
    if(array(1) == null) {r = r :+ array(1)}
      else{ r = r :+ new java.sql.Date(dateFormat.parse(array(1)).getTime()) }
    // // MentionTimeDate
    if(array(2) == null) {r = r :+ array(2)}
      else{ r = r :+ new java.sql.Date(dateFormat.parse(array(2)).getTime()) }
      
    //MentionType
    if(array(3) == null) {r = r :+ array(3)}
    else {r = r :+ array(3).toInt}
    //MentionSourceName
    if(array(4) == null) {r = r :+ array(4)}
    else {r = r :+ array(4)}
    //MentionIdentifier
    if(array(5) == null) {r = r :+ array(5)}
    else {r = r :+ array(5)}
    
    //SentenceID
    if(array(6) == null) {r = r :+ array(6)}
    else {r = r :+ array(6).toInt}
    
    //Actor1CharOffset
    if(array(7) == null) {r = r :+ array(7)}
    else {r = r :+ array(7).toInt}
    
    //Actor2CharOffset
    if(array(8) == null) {r = r :+ array(8)}
    else {r = r :+ array(8).toInt}
    
    //ActionCharOffset
    if(array(9) == null) {r = r :+ array(9)}
    else {r = r :+ array(9).toInt}
    
    //InRawText
    if(array(10) == null) {r = r :+ array(10)}
    else {r = r :+ "1".equals(array(10))}
    
    //Confidence
    if(array(11) == null) {r = r :+ array(11)}
    else {r = r :+ array(11).toInt}
    
    //MentionDocLen
    if(array(12) == null) {r = r :+ array(12)}
    else {r = r :+ array(12).toInt}
    
    //MentionDocTone
   // if(array.size > 13)
    //{
    //if(array(13) == null) {r = r :+ array(13)}
    //else{ r = r :+ array(13).toDouble }
    //}
    //else
    //{
    
    //r = r :+ None
    //}

   val temp = scala.util.Try(array(13)).getOrElse(null)

   if(temp == null) {r = r :+ temp}
   else{ r = r :+ array(13).toDouble }

    // //MentionDocTranslationInfo
    // r = r :+ array(14)
    
    // //Extras
    // r = r :+ array(15)

  ////  tranform seq to  row
   val rowfromseq= Row.fromSeq(r)

   return  rowfromseq
 }


 val dfSchema =  StructType(
    Seq(
      StructField(name = "GlobalEventID", dataType = LongType, nullable = true),
      StructField(name = "EventTimeDate", dataType = DateType, nullable = true),
      StructField(name = "MentionTimeDate", dataType = DateType, nullable = true),
      StructField(name = "MentionType", dataType = IntegerType, nullable = true),
      StructField(name = "MentionSourceName", dataType = StringType, nullable = true),
      StructField(name = "MentionIdentifier", dataType = StringType, nullable = true),
      StructField(name = "SentenceID", dataType = IntegerType, nullable = true),
      StructField(name = "Actor1CharOffset", dataType = IntegerType, nullable = true),
      StructField(name = "Actor2CharOffset", dataType = IntegerType, nullable = true),
      StructField(name = "ActionCharOffset", dataType = IntegerType, nullable = true),
      StructField(name = "InRawText", dataType = BooleanType, nullable = true),
      StructField(name = "Confidence", dataType = IntegerType, nullable = true),
      StructField(name = "MentionDocLen", dataType = IntegerType, nullable = true),
      StructField(name = "MentionDocTone", dataType = DoubleType, nullable = true)
    //   StructField(name = "MentionDocTranslationInfo", dataType = StringType, nullable = true)
    //   StructField(name = "Extras", dataType = StringType, nullable = true)
    )
  )


//Structure data and store dataframe in parquet Format in S3
val data = textRDD.map(_.split("\t")).map(array => convertExportarrayToRow(array))
val dataFrame = spark.createDataFrame(data, dfSchema)
dataFrame.write.format("parquet").mode("overwrite").save("s3a://gdelt-tdenimal/201812.mentions")

System.exit(0)
