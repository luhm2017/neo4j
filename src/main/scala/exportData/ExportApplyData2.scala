package exportData

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object ExportApplyData2 {
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
      * 二度关联导出入口
      * */
    def main(args: Array[String]): Unit = {

        if(args.length!=3){
          println("请输入参数：database、table以及mysql相关参数")
          System.exit(0)
        }
        //分别传入库名、表名、mysql相关参数
        val database = args(0)
        val table = args(1)
        val partition = args(2)

        //关联关系
        val relations = "IDCARD|BANKCARD|MYPHONE|CONTACT|EMERGENCY|COMPANYPHONE|EMAIL|DEVICE"

        try{
            //提前准备好数据源
            val sourDF = hc.sql(s"select order_id from $database.$table where pt_month = '$partition' ")
            println("==================数据源总数: "+ sourDF.count())
            println("==============================组装需要执行的SQL start ")
            val sqlDF = sourDF.map{
              pr =>
                "match (n:Apply {contentKey:'"+pr+"'})-[r:"+relations+"]-(p)-[r1:"+relations+"]-(m:Apply)-[r2:"+relations+"]-(p2)-[r3:"+relations+"]-(m2:Apply)\n          " +
                  "return n.contentKey,type(r) as r,p.contentKey,type(r1) as r1,m.contentKey,type(r2) as r2, p2.contentKey,type(r3) as r3,m2.contentKey  limit 100000".toString
            }
            println("================runQueryApplyByApplyLevel2 start ")
            val rddResult = sqlDF.map{
                row =>
                  val con: Connection = DriverManager.getConnection("jdbc:neo4j:bolt:10.10.206.35:7687", "neo4j", "1qaz2wsx")
                  val stmt: Statement = con.createStatement
                  stmt.setQueryTimeout(120)
                  val rs = stmt.executeQuery(row)
                  val buff = new ArrayBuffer[String]()
                  while(rs.next()){
                    //获取二度关联的字段值
                    buff += s"${rs.getString("n.contentKey")},${rs.getString("r")},${rs.getString("p.contentKey")},${rs.getString("r1")},${rs.getString("m.contentKey")},${rs.getString("r2")},${rs.getString("p2.contentKey")},${rs.getString("r3")},${rs.getString("m2.contentKey")}"
                  }
                buff.toList
            }
            println("========================runQueryApplyByApplyLevel1 end===============================")
            //println("======================== 二度关联的订单总数： " + rddResult.count())
            println("========================组装二度关联结果===============================")
            val degree2 = rddResult.flatMap(k => k)/*.distinct()*/
              .map { v =>
              val arr: Array[String] = v.split(",")
              Row(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8))
            }
            //println("=======================================二度关联总记录数："+  degree2.count())

            //映射字段类型
            val schema = StructType(
              List(
                StructField("order_id_src", StringType, true),
                StructField("edg_type_src1", StringType, true),
                StructField("contact_value1", StringType, true),
                StructField("edg_type_dst1", StringType, true),
                StructField("order_id_dst1", StringType, true),
                StructField("edg_type_src2", StringType, true),
                StructField("contact_value2", StringType, true),
                StructField("edg_type_dst2", StringType, true),
                StructField("order_id_dst2", StringType, true)
              )
            )
            //保存一度关联结果
            val df = hc.createDataFrame(degree2,schema)
            df.write.mode(SaveMode.Overwrite).saveAsTable("fqz.temp_degree2")

        }catch {
            case e: Exception => {
              println("main exception:" + e.getMessage + " \n" + "exception2:" + e.getStackTraceString)
            }
        }

    }
}
