package exportData.exportDataByCertNo

import java.io.Serializable
import java.sql.{Connection, Statement}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import utils.DBConnectionPool

import scala.collection.mutable.ArrayBuffer

/**
  * 1、增加jdbc连接池控制
  * 2、输入开始结束日期，按天分区遍历导出
  * */
object ExportApplyData1ByCertNo extends Serializable{

    /**
      * 一度关联导出入口
      * */
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("AntiFraudNeo4jDegree1.ExportApplyData1ByCertNo")
        val sc = new SparkContext(sparkConf)
        val hc = new HiveContext(sc)

        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.set("spark.rdd.compress","true")
        sparkConf.set("spark.hadoop.mapred.output.compress","true")
        sparkConf.set("spark.hadoop.mapred.output.compression.codec","true")
        sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
        sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")

      /*if(args.length!=2){
        println("请输入参数：database、table以及日期")
        System.exit(0)
      }

      //分别传入库名、表名、mysql相关参数
      val database = args(0)
      val table = args(1)*/

      exportLevel1Data(sc,hc)
    }

    //导出一度关联数据
    def exportLevel1Data(sc:SparkContext,hc:HiveContext) : Unit={
      //关联关系
      val relations = "BANKCARD|MYPHONE|CONTACT|EMERGENCY|COMPANYPHONE|EMAIL|DEVICE"
      try{
        val sqlDF = hc.sql("select tab.cert_no from (\nselect cert_no from knowledge_graph.fqz_cert_no_newest_sample_good a \nunion all \nselect cert_no from knowledge_graph.fqz_cert_no_newest_sample_black a) tab\ngroup by tab.cert_no").map{
          pr =>
            val certNo = pr.getString(0)
            /*"match (n:Apply {contentKey:'"+certNo+"'})-[r:"+relations+"]-(p)-[r1:"+relations+"]-(m:Apply) " +
              ("return n.contentKey,n.applyDate,type(r) as r,p.contentKey,p.applyDate,type(r1) as r1,m.contentKey,m.applyDate limit 100000").toString*/
            //certNo的所有关联属性
            "match (n:Apply {contentKey:'"+certNo+"'})-[r:"+relations+"]-(p) " +
              ("return n.contentKey,n.applyDate,type(r) as r,p.contentKey,p.applyDate limit 100000").toString
        } //缩小partition
        println("================runQueryApplyByApplyLevel1 start ========")
        sqlDF.saveAsTextFile("hdfs://zhengcemoxing.lkl.com:8020/user/luhuamin/sql2/")

        val rddResult = sqlDF.map{
          row =>
              /*val con: Connection = DriverManager.getConnection("jdbc:neo4j:bolt:10.10.206.35:7687", "neo4j", "1qaz2wsx")
              val stmt: Statement = con.createStatement*/
              //使用连接池的方式
              println("=================getConn===================================================")
              val con:Connection = DBConnectionPool.getConn
              println("=================createStatement===================================================")
              val stmt:Statement = con.createStatement
              stmt.setQueryTimeout(120)
              val rs = stmt.executeQuery(row)
              val buff = new ArrayBuffer[String]()
              while(rs.next()){
                buff += s"${rs.getString("n.contentKey")},${rs.getString("n.applyDate")}," +
                  s"${rs.getString("r")},${rs.getString("p.contentKey")},${rs.getString("p.applyDate")}" /*,${rs.getString("r1")},${rs.getString("m.contentKey")},${rs.getString("m.applyDate")}*/
              }
              //每次连接释放，如果conn放在外面，会报错 Task not serializable
              /*stmt.close()
              con.close()*/
              DBConnectionPool.releaseCon(con)
              buff.toList
        }/*.repartition(300)*/
        println("========================runQueryApplyByApplyLevel1 end===============================")
        //println("======================== 一度关联的订单总数： "+ rddResult.count())
        println("========================组装一度关联结果===============================" )
        rddResult.saveAsTextFile("hdfs://zhengcemoxing.lkl.com:8020/user/luhuamin/spark2/")
        //过滤rddResult中List()的情况
        val degree1 = rddResult.flatMap(k => k).distinct()
          .map { v =>
            val arr: Array[String] = v.split(",")
            Row(arr(0),arr(1),arr(2),arr(3),arr(4)/*,arr(5),arr(6),arr(7)*/)
          }
        println("=======================================一度关联总记录数：")

        //映射字段类型
        val schema = StructType(
          List(
            StructField("cert_no_src", StringType, true),
            StructField("apply_date_src",StringType,true),
            StructField("edge_src",StringType,true),
            StructField("content_key_mid",StringType,true),
            StructField("apply_date_mid",StringType,true)
            /*StructField("edge_dst",StringType,true),
            StructField("cert_no_dst",StringType,true),
            StructField("apply_date_dst",StringType,true)*/
          )
        )
        //保存一度关联结果
        val df = hc.createDataFrame(degree1,schema)
        //分区保存数据
        df.registerTempTable("cert_no_temp_degree1")
        hc.sql("use knowledge_graph")
        hc.sql("create table  cert_no_fqz_degree1_relation as select * from cert_no_temp_degree1")
        //df.write.mode(SaveMode.Overwrite).saveAsTable("fqz.temp_degree1")
        //sc.stop()
      }catch {
        case e: Exception => {
          println("main exception:" + e.getMessage + " \n" + "exception2:" + e.getStackTraceString)
        }
      }

    }

}
