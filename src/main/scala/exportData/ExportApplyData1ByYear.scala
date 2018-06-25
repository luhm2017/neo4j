package exportData

import java.io.Serializable
import java.sql.{Connection, DriverManager, Statement}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import utils.DBConnectionPool

import scala.collection.mutable.ArrayBuffer

object ExportApplyData1ByYear extends Serializable{

    /**
      * 一度关联导出入口
      * */
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("AntiFraudNeo4jDegree1.ExportApplyData1ByYear")
        val sc = new SparkContext(sparkConf)
        val hc = new HiveContext(sc)

        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.set("spark.rdd.compress","true")
        sparkConf.set("spark.hadoop.mapred.output.compress","true")
        sparkConf.set("spark.hadoop.mapred.output.compression.codec","true")
        sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
        sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")

      if(args.length!=4){
        println("请输入参数：database、table以及日期")
        System.exit(0)
      }

      //分别传入库名、表名、mysql相关参数
      val database = args(0)
      val table = args(1)
      val startDateStr = args(2)
      val endDateStr = args(3)

      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val startDate = dateFormat.parse(startDateStr)
      val endDate = dateFormat.parse(endDateStr)
      val cal: Calendar = Calendar.getInstance()
      cal.setTime(startDate)
      val between = endDate.getTime - startDate.getTime
      //计算相隔天数
      val days = between / 1000 / 3600 / 24
      var cnt = 0 //记数标志
      while (cnt <=  days){
          var year = cal.get(Calendar.YEAR).toString
          var month = (cal.get(Calendar.MONTH)+1).toString
          var day = cal.get(Calendar.DAY_OF_MONTH).toString
          if (month.length == 1 )
            month = "0".concat(month)
          if (day.length == 1 )
            day = "0".concat(day)
          println("================  year ：" + year +", month :"+ month +", day"+ day)
          println("================exportLevel1Data start ========")
          exportLevel1Data(sc,hc,database,table,year,month,day)
          cal.add(Calendar.DATE,1)
          cnt +=  1
      }
    }

    def exportLevel1Data(sc:SparkContext,hc:HiveContext,database:String,table:String,year:String,month:String,day:String) : Unit={
      //关联关系
      val relations = "IDCARD|BANKCARD|MYPHONE|CONTACT|EMERGENCY|COMPANYPHONE|EMAIL|DEVICE"

      try{
        //按月分区跑数
        val sqlDF = hc.sql(s"SELECT order_id FROM $database.$table where year = $year and month = $month and day = $day ").map{
          pr =>
            val orderId = pr.getString(0)
            "match (n:Apply {contentKey:'"+orderId+"'})-[r:"+relations+"]-(p)-[r1:"+relations+"]-(m:Apply) where n.applyDate > m.applyDate " +
              ("return n.contentKey,n.applyDate,n.applyLastState,n.applyState,n.currentDueDay,n.historyDueDay,n.failReason,n.performance,n.cert_no," +
                "type(r) as r,p.contentKey,type(r1) as r1,m.contentKey,m.applyDate, m.applyLastState,m.applyState,m.currentDueDay," +
                " m.historyDueDay,m.failReason,m.performance,m.cert_no  limit 100000").toString
        }.repartition(10)  //缩小partition
        println("================runQueryApplyByApplyLevel1 start ========")
        sqlDF.saveAsTextFile("hdfs://zhengcemoxing.lkl.com:8020/user/luhuamin/sql/")
        //调整连接串位置

        val rddResult = sqlDF.map{
          row =>
              /*val con: Connection = DriverManager.getConnection("jdbc:neo4j:bolt:10.10.206.35:7687", "neo4j", "1qaz2wsx")
              val stmt: Statement = con.createStatement*/
              //使用连接池的方式
              val con:Connection = DBConnectionPool.getConn()
              val stmt:Statement = con.createStatement()
              stmt.setQueryTimeout(120)
              val rs = stmt.executeQuery(row)
              val buff = new ArrayBuffer[String]()
              while(rs.next()){
                buff += s"${rs.getString("n.contentKey")},${rs.getString("n.applyDate")},${rs.getString("n.applyLastState")},${rs.getString("n.applyState")}," +
                  s"${rs.getString("n.currentDueDay")},${rs.getString("n.historyDueDay")},${rs.getString("n.failReason")},${rs.getString("n.performance")},${rs.getString("n.cert_no")}," +
                  s"${rs.getString("r")},${rs.getString("p.contentKey")},${rs.getString("r1")},${rs.getString("m.contentKey")},${rs.getString("m.applyDate")}," +
                  s"${rs.getString("m.applyLastState")},${rs.getString("m.applyState")},${rs.getString("m.currentDueDay")},${rs.getString("m.historyDueDay")},${rs.getString("m.failReason")},${rs.getString("m.performance")},${rs.getString("m.cert_no")}"
              }
              //每次连接释放，如果conn放在外面，会报错 Task not serializable
              stmt.close()
              con.close()
              buff.toList
        }/*.repartition(300)*/
        println("========================runQueryApplyByApplyLevel1 end===============================")
        //println("======================== 一度关联的订单总数： "+ rddResult.count())
        println("========================组装一度关联结果===============================" )
        rddResult.saveAsTextFile("hdfs://zhengcemoxing.lkl.com:8020/user/luhuamin/spark/")
        //过滤rddResult中List()的情况
        val degree1 = rddResult.flatMap(k => k).distinct()
          .map { v =>
            val arr: Array[String] = v.split(",")
            Row(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12),arr(13),arr(14),arr(15),arr(16),arr(17),arr(18),arr(19),arr(20))
          }
        println("=======================================一度关联总记录数：")

        //映射字段类型
        val schema = StructType(
          List(
            StructField("order_id_src", StringType, true),
            StructField("apply_date_src",StringType,true),
            StructField("apply_last_state_src",StringType,true),
            StructField("apply_state_src",StringType,true),
            StructField("current_due_day_src",StringType,true),
            StructField("history_due_day_src",StringType,true),
            StructField("fail_reason_src",StringType,true),
            StructField("performance_src",StringType,true),
            StructField("cert_no_src",StringType,true),
            StructField("edg_type_src1", StringType, true),
            StructField("contact_value1", StringType,true),
            StructField("edg_type_dst1", StringType, true),
            StructField("order_id_dst1", StringType, true),
            StructField("apply_date_dst1",StringType,true),
            StructField("apply_last_state_dst1",StringType,true),
            StructField("apply_state_dst1",StringType,true),
            StructField("current_due_day_dst1",StringType,true),
            StructField("history_due_day_dst1",StringType,true),
            StructField("fail_reason_dst1",StringType,true),
            StructField("performance_dst1",StringType,true),
            StructField("cert_no_dst1",StringType,true)
          )
        )
        //保存一度关联结果
        val df = hc.createDataFrame(degree1,schema)
        //分区保存数据
        df.registerTempTable("temp_degree1")
        hc.sql("use fqz")
        hc.sql(s"insert into fqz_relation_degree1 partition(year=$year,month=$month,day=$day) select order_id_src,apply_date_src," +
          s"apply_last_state_src,apply_state_src,current_due_day_src,history_due_day_src,fail_reason_src,performance_src," +
          s"cert_no_src,edg_type_src1,contact_value1,edg_type_dst1,order_id_dst1,apply_date_dst1,apply_last_state_dst1," +
          s"apply_state_dst1,current_due_day_dst1,history_due_day_dst1,fail_reason_dst1,performance_dst1,cert_no_dst1 from temp_degree1")
        //df.write.mode(SaveMode.Overwrite).saveAsTable("fqz.temp_degree1")
        sc.stop()
      }catch {
        case e: Exception => {
          println("main exception:" + e.getMessage + " \n" + "exception2:" + e.getStackTraceString)
        }
      }

    }

    //获取配置文件信息
    def getProperties(): Properties ={
      val props = new Properties()
      val in=this.getClass.getClassLoader.getResourceAsStream("dbconfig.properties")
      props.load(in)
      props
    }
}
