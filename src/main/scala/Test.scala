import java.io.File

import org.joda.time.DateTime

object Test {

  def main(args: Array[String]): Unit = {
    /*var orderId = "test:test:1000"
    if(orderId.contains(":"))
      orderId = orderId.replace(":","")
    print(orderId)*/

    val filepath = "D:\\workspace\\neo4j\\data"
    val inputPath=filepath+"/neo4j_data/"  //输入文件路径
    for (file <- subdirs(new File(inputPath))) {
      println(file.getPath)
    }

  }

  def subdirs(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory)
    val files = dir.listFiles().filter(_.isFile).toIterator
    files ++ dirs.toIterator.flatMap(subdirs _)
  }

}
