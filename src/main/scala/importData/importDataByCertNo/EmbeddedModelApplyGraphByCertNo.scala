package importData.importDataByCertNo

import java.io.File

import org.joda.time.DateTime
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.io.fs.FileUtils
import org.slf4j.LoggerFactory

/**
  * Created by luhm on 20180713
  */
object EmbeddedModelApplyGraphByCertNo {
  private val logger = LoggerFactory.getLogger("EmbeddedModelApplyGraphByCertNo")

  //从清洗好的表中导入数据
  def main(args: Array[String]): Unit = {
    try{
      //data file path
      val filepath=args(0) // /data/
      val mainTime = DateTime.now()
      println("start EmbeddedModelApplyGraphByCertNo time " + DateTime.now())
      logger.info("start EmbeddedModelApplyGraphByCertNo time " + DateTime.now())
      val inputPath=filepath+"/neo4j_data/"  //输入文件路径
      val outputPath=filepath+"/neo4j_db/graph.db" //输出数据库文件
      generateGraphData(filepath,inputPath,outputPath)//建图逻辑代码
      val endtime = DateTime.now()
      println("end EmbeddedModelApplyGraphByCertNo time " + endtime + " run long time " + (endtime.getMillis - mainTime.getMillis) / 36000)
      logger.info("end EmbeddedModelApplyGraphByCertNo time " + endtime + " run long time " + (endtime.getMillis - mainTime.getMillis) / 36000)

    } catch {
      case e: Exception => {
        println("main exception:" + e.getMessage + " \n" + "exception2:" + e.getStackTraceString)
        logger.info("main exception:" + e.getMessage + " \n" + "exception2:" + e.getStackTraceString)
      }
    }

  }

  /**
    *  生成图数据库
    * @param rootPath
    * @param inputPath
    * @param outputPath
    */
  def generateGraphData(rootPath:String,inputPath:String,outputPath: String): Unit = {
    //删除原有的图文件
    FileUtils.deleteRecursively(new File(outputPath))
    //创建数据库
    var graphdb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(outputPath))
    //生成数据文件
    val neo4jDataGenerator = new Neo4jDataGeneratorByCertNo(graphdb)
    //生成数据
    neo4jDataGenerator.generateGraphdb(rootPath,inputPath)

    registerShutdownHook(graphdb)
  }

  /**
    * 用钩子函数关闭进程
    * @param graph
    */
  def registerShutdownHook(graph: GraphDatabaseService): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        graph.shutdown()
      }
    })
  }
}
