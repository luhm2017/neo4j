package utils

import java.sql.{Connection, DriverManager}
import java.util.{LinkedList, ResourceBundle}

/**
  * 数据库连接池工具类
  * 语言：scala
  * 时间：2016-07-09
  */
object DBConnectionPool {
  /*private val reader = ResourceBundle.getBundle("connection")
  private val max_connection = reader.getString("neo4j.max_connection") //连接池总数
  private val connection_num = reader.getString("neo4j.connection_num") //产生连接数
  private var current_num = 0 //当前连接池已产生的连接数
  private val pools = new LinkedList[Connection]() //连接池
  private val driver = reader.getString("neo4j.driver")
  private val url = reader.getString("neo4j.url")
  private val username = reader.getString("neo4j.username")
  private val password = reader.getString("neo4j.password")*/
  //代码替换
  private val max_connection = 25 //连接池总数
  private val connection_num = 23 //产生连接数
  private var current_num = 0 //当前连接池已产生的连接数
  private val pools = new LinkedList[Connection]() //连接池
  private val driver = "org.neo4j.driver.v1.Driver"
  private val url = "jdbc:neo4j:bolt:10.10.206.35:7687"
  private val username = "neo4j"
  private val password = "1qaz2wsx"


  /**
    * 加载驱动
    */
  private def before() {
    if (current_num > max_connection.toInt && pools.isEmpty()) {
      println("=================================busyness=================================")
      Thread.sleep(2000)
      before()
    } else {
      println("=================before===================================================")
      Class.forName(driver)
    }
  }
  /**
    * 获得连接
    */
  private def initConn(): Connection = {
    println("=================initConn===================================================")
    val conn = DriverManager.getConnection(url, username, password)
    conn
  }
  /**
    * 初始化连接池
    */
  private def initConnectionPool(): LinkedList[Connection] = {
    println("=================initConnectionPool===================================================")
    AnyRef.synchronized({
      if (pools.isEmpty()) {
        before()
        for (i <- 1 to connection_num.toInt) {
          println("============================foreach " + i)
          pools.push(initConn())
          current_num += 1
        }
      }
      pools
    })
  }
  /**
    * 获得连接
    */
  def getConn():Connection={
    println("============================getConn============================" )
    initConnectionPool()
    pools.poll()
  }
  /**
    * 释放连接
    */
  def releaseCon(con:Connection){
    println("============================releaseCon============================" )
    pools.push(con)
  }

}
