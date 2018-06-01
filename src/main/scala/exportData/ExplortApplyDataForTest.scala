package exportData

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/5/9 0009.
  */
object ExplortApplyDataForTest extends Serializable{

  val sparkConf = new SparkConf().setAppName("AntiFraudNeo4j")
  val sc = new SparkContext(sparkConf)
  val hc = new HiveContext(sc)

  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkConf.set("spark.rdd.compress","true")
  sparkConf.set("spark.hadoop.mapred.output.compress","true")
  sparkConf.set("spark.hadoop.mapred.output.compression.codec","true")
  sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
  sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")

  private val ip = "jdbc:neo4j:bolt:10.10.206.35:7687"
  private val username = "neo4j"
  private val password = "1qaz2wsx"

  /**
    * 一度关联导出入口
    * */
  def main(args: Array[String]): Unit = {



      // setUP
      //已建立实体 Apply, BankCard, Phone, IDCard, Email, Device
      //暂时未建立实体 Terminal,Company, CompanyAddress，IPV4 ,LBS
      //val modelRdd = sc.parallelize(List("Apply", "BankCard", "Phone", "IDCard", "Email", "Device"))
      //关联边 IDCARD, BANKCARD,RETURNBANKCARD ,MYPHONE, EMAIL, CONTACT, EMERGENCY, DEVICE,COMPANYPHONE
      //val broadcastVar = sc.broadcast(List("IDCARD", "BANKCARD", "MYPHONE", "CONTACT", "EMERGENCY", "COMPANYPHONE", "EMAIL","DEVICE"))
      //关联关系
      val relations = "IDCARD|BANKCARD|MYPHONE|CONTACT|EMERGENCY|COMPANYPHONE|EMAIL|DEVICE"
      //映射字段类型
      val schema = StructType(
        List(
          StructField("order_id_src", StringType, true),
          StructField("edg_type_src1", StringType, true),
          StructField("contact_value1", StringType, true),
          StructField("edg_type_dst1", StringType, true),
          StructField("order_id_dst1", StringType, true)
        )
      )

      try{
            //一度关联导出
            //val con: Connection = DriverManager.getConnection(ip, username, password)
            println("====================创建连接===========================")
            //runQueryApplyByApplyLevel1(stmt,relations,sourceOrderIds)
            println("===============测试 序列化问题 start ==============")
            println("===========================================================")
            val rddResult = hc.sql("select order_id from fqz.fqz_apply_contract_data limit 10").map{
                orderId =>
                    val con: Connection = DriverManager.getConnection("jdbc:neo4j:bolt:10.10.206.35:7687", "neo4j", "1qaz2wsx")
                    val stmt: Statement = con.createStatement
                    stmt.setQueryTimeout(120)
                    val querySql = "match (n:Apply {contentKey:'248776069293887'})-[r:IDCARD|BANKCARD|MYPHONE|CONTACT|EMERGENCY|COMPANYPHONE|EMAIL|DEVICE]-(p)-[r1:IDCARD|BANKCARD|MYPHONE|CONTACT|EMERGENCY|COMPANYPHONE|EMAIL|DEVICE]-(m:Apply) " +
                      "return n.contentKey,type(r) as r,p.contentKey,type(r1) as r1,m.contentKey  limit 100000"
                    println("==========================================" + querySql)
                    println("========================start runQueryApplyByApplyLevel1===============================")
                    val rs = stmt.executeQuery(querySql)
                    println("======================== 执行sql")
                    val buff = new ArrayBuffer[String]()
                    while (rs.next) {
                      println("=========== 执行rs.next================= ")
                      println(s"${rs.getString("n.contentKey")},${rs.getString("r")},${rs.getString("p.contentKey")},${rs.getString("r1")},${rs.getString("m.contentKey")}")
                      buff += s"${rs.getString("n.contentKey")},${rs.getString("r")},${rs.getString("p.contentKey")},${rs.getString("r1")},${rs.getString("m.contentKey")}"
                    }
                    buff.toList
            }

            //返回 RDD[List[String]]，转换成 RDD
            println("======================RDD[List[String]] 转换成 RDD")
            val degree1 = rddResult.flatMap(k => k)/*.distinct()*/
              .map { v =>
                val arr: Array[String] = v.split(",")
                Row(arr(0),arr(1),arr(2),arr(3),arr(4))
              }
            println("======================================="+  degree1.count())

            val df = hc.createDataFrame(degree1,schema)
            df.write.mode(SaveMode.Overwrite).saveAsTable("fqz.temp_degree1")
            //df.registerTempTable("fqz_black_related_data0_tmp")

            //    degree0.saveAsTextFile(args(1))

            /**hc.sql("use lkl_card_score ")
            //    hc.sql(" drop table fqz_black_related_data0")
            hc.sql(" CREATE TABLE IF NOT EXISTS  fqz_black_related_data0 (degreeType STRING, tagging_edge STRING,edge0 STRING,apply0 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
            hc.sql(" truncate table fqz_black_related_data0 ")
            val indf = hc.sql("INSERT OVERWRITE TABLE fqz_black_related_data0 select degreeType,tagging_edge,edge0,apply0 FROM fqz_black_related_data0_tmp")
            println(s"total 0 degree insert into table fqz_black_related_data0 size  ${indf.count()}")
            println(degree1.count())

            closeDown*/

      } catch {
          case e: Exception => {
            println("main exception:" + e.getMessage + " \n" + "exception2:" + e.getStackTraceString)
          }
      }

      //查询一度关联关系 （进件到进件之间）
      /*def runQueryApplyByApplyLevel1(stmt: Statement, relations: String,sourceOrderIds:DataFrame):Unit = {
        val querySql = "match (n:Apply {contentKey:'248776069293887'})-[r:IDCARD|BANKCARD|MYPHONE|CONTACT|EMERGENCY|COMPANYPHONE|EMAIL|DEVICE]-(p)-[r1:IDCARD|BANKCARD|MYPHONE|CONTACT|EMERGENCY|COMPANYPHONE|EMAIL|DEVICE]-(m:Apply) " +
          "return n.contentKey,type(r) as r,p.contentKey,type(r1) as r1,m.contentKey  limit 100000"
        println("==========================================" + querySql)
        println("========================start runQueryApplyByApplyLevel1===============================")
        val rs = stmt.executeQuery(querySql)
        println("======================== 执行sql")
        val buff = new ArrayBuffer[String]()
        while (rs.next) {
          println("=========== 执行rs.next================= ")
          println(s"${rs.getString("n.contentKey")},${rs.getString("r")},${rs.getString("p.contentKey")},${rs.getString("r1")},${rs.getString("m.contentKey")}")
          buff += s"${rs.getString("n.contentKey")},${rs.getString("r")},${rs.getString("p.contentKey")},${rs.getString("r1")},${rs.getString("m.contentKey")}"
        }
        //返回 List[String]，转换成 RDD
        println("======================转换成 RDD")
        val rdd = sc.parallelize(buff.toList)
        val degree1 = rdd.map{
          row =>
            val arr:Array[String] = row.split(",")
            Row(arr(0),arr(1),arr(2),arr(3),arr(4))
        }
        println("======================================="+  degree1.count())

        val df = hc.createDataFrame(degree1,schema)
        df.write.mode(SaveMode.Overwrite).saveAsTable("fqz.temp_degree1")
      }*/

  }
}
