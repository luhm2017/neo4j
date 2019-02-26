package importData

import java.io.File
import java.text.SimpleDateFormat

import org.joda.time.DateTime

object Test {

  def main(args: Array[String]): Unit = {
    /*var orderId = "test:test:1000"
    if(orderId.contains(":"))
      orderId = orderId.replace(":","")
    print(orderId)*/

    /*val filepath = "D:\\workspace\\neo4j\\data"
    val inputPath=filepath+"/neo4j_data/"  //输入文件路径
    for (file <- subdirs(new File(inputPath))) {
      println(file.getPath)
    }*/
   /* var test = "010|2762235"
    println(test.replaceAll("|","").replaceAll("-",""))*/

    //遍历日期
    val startDateStr = "2018-03-01"
    val endDateStr = "2018-03-25"
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val startDate = sdf.parse(startDateStr)
    val endDate = sdf.parse(endDateStr)
    val between = endDate.getTime - startDate.getTime
    //计算相隔天数
    val days = between / 1000 / 3600 / 24
    val cnt = 1 //记数标志
    while (cnt <=  days){
          println("打印日期, 年份：" + new SimpleDateFormat("yyyy").parse(startDateStr) +",月份："+ new SimpleDateFormat("MM").parse(startDateStr)+", 天"+new SimpleDateFormat("dd").parse(startDateStr))
    }

  }

  def subdirs(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory)
    val files = dir.listFiles().filter(_.isFile).toIterator
    files ++ dirs.toIterator.flatMap(subdirs _)
  }


}
