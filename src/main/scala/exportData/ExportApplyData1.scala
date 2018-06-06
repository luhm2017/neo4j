package exportData

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object ExportApplyData1 {

    /*private val ip = "jdbc:neo4j:bolt:10.10.206.35:7687"
    private val username = "neo4j"
    private val password = "1qaz2wsx"*/

    /**
      * 一度关联导出入口
      * */
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("AntiFraudNeo4jDegree1")
        val sc = new SparkContext(sparkConf)
        val hc = new HiveContext(sc)

        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.set("spark.rdd.compress","true")
        sparkConf.set("spark.hadoop.mapred.output.compress","true")
        sparkConf.set("spark.hadoop.mapred.output.compression.codec","true")
        sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
        sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")

        if(args.length!=3){
          println("请输入参数：database、table以及分区参数")
          System.exit(0)
        }

        //分别传入库名、表名、mysql相关参数
        val database = args(0)
        val table = args(1)
        val pt_month = args(2)

        //关联关系
        val relations = "IDCARD|BANKCARD|MYPHONE|CONTACT|EMERGENCY|COMPANYPHONE|EMAIL|DEVICE"

        try{
            //按月分区跑数
            val sqlDF = hc.sql(s"SELECT order_id FROM $database.$table where pt_month = '$pt_month'").map{
              pr =>
                val orderId = pr.getString(0)
                "match (n:Apply {contentKey:'"+orderId+"'})-[r:"+relations+"]-(p)-[r1:"+relations+"]-(m:Apply) " +
                  "return n.contentKey,type(r) as r,p.contentKey,type(r1) as r1,m.contentKey  limit 100000".toString
            }
            println("================runQueryApplyByApplyLevel1 start ========")
            //sqlDF.saveAsTextFile("hdfs://zhengcemoxing.lkl.com:8020/user/luhuamin/sql/")
            val rddResult = sqlDF.map{
                row =>
                  val con: Connection = DriverManager.getConnection("jdbc:neo4j:bolt:10.10.206.35:7687", "neo4j", "1qaz2wsx")
                  val stmt: Statement = con.createStatement
                  stmt.setQueryTimeout(120)
                  val rs = stmt.executeQuery(row)
                  val buff = new ArrayBuffer[String]()
                  while(rs.next()){
                    buff += s"${rs.getString("n.contentKey")},${rs.getString("r")},${rs.getString("p.contentKey")},${rs.getString("r1")},${rs.getString("m.contentKey")}"
                  }
                  //每次连接释放，如果conn放在外面，会报错 Task not serializable
                  con.close()
                buff.toList
            }.repartition(10)
            println("========================runQueryApplyByApplyLevel1 end===============================")
            println("======================== 一度关联的订单总数： ")
            println("========================组装一度关联结果===============================")
            //rddResult.saveAsTextFile("hdfs://zhengcemoxing.lkl.com:8020/user/luhuamin/spark/")
            val degree1 = rddResult.flatMap(k => k)/*.distinct()*/
              .map { v =>
              val arr: Array[String] = v.split(",")
              Row(arr(0),arr(1),arr(2),arr(3),arr(4))
            }.repartition(100)
            //println("=======================================一度关联总记录数："+  degree1.count())

            //映射字段类型
            val schema = StructType(
              List(
                StructField("order_id_src", StringType, true),
                StructField("edg_type_src1", StringType, true),
                StructField("contact_value1", StringType,true),
                StructField("edg_type_dst1", StringType, true),
                StructField("order_id_dst1", StringType, true)
              )
            )
            //保存一度关联结果
            val df = hc.createDataFrame(degree1,schema)
            //分区保存数据
            df.registerTempTable("temp_degree1")
            hc.sql("use fqz")
            hc.sql(s"insert into fqz_relation_degree1 partition(pt_month='$pt_month') select order_id_src,edg_type_src1,contact_value1,edg_type_dst1,order_id_dst1 from temp_degree1")
            //df.write.mode(SaveMode.Overwrite).saveAsTable("fqz.temp_degree1")

        }catch {
            case e: Exception => {
              println("main exception:" + e.getMessage + " \n" + "exception2:" + e.getStackTraceString)
            }
        }

    }
}
