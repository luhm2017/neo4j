package importData

import java.io.File

import com.google.common.base.Splitter
import org.apache.commons.lang3.StringUtils
import org.neo4j.graphdb._
//import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by zhijie.guo on 2017/12/27 0031.
  */
class Neo4jDataGenerator /*extends DataGenerator*/ {
  //private val logger = LoggerFactory.getLogger("Neo4jDataGenerator")
  val BATCH_SIZE: Int = 3000

  private var graphdb: GraphDatabaseService = null
  private val nodeMap: mutable.HashMap[String, Node] with Object = new scala.collection.mutable.HashMap[String, Node]()

  def this(graphdb: GraphDatabaseService) {
    this()
    this.graphdb = graphdb
  }

  def generateGraphdb(rootPath:String,inputPath: String): Unit = {

    //启动事务
    var tx = graphdb.beginTx
    var rowline=""
    try {
      var i: Int = 0  //记录行号
      var j: Int = 0  //记录文件块个数
      //遍历所有的输入文件读取数据集
      for (file <- subdirs(new File(inputPath))) {
        j += 1
        //记录当前文件块名称
        val currentFilePath = file.getAbsolutePath
        //记录每个批次的数据
        val lineList=new ArrayBuffer[String]
        var batchStartAppNo=""  //每个批次开始的申请订单号
        //每个文件逐行读取
        for (line <- Source.fromFile(file,"UTF-8").getLines()) {
          i += 1
          lineList+=line  //记录每行记录
          import scala.collection.JavaConversions._
          val list = Splitter.on(",").trimResults().split(line).toList //csv文件以","作为分割符
          try {
            if(i % BATCH_SIZE == 1){
              batchStartAppNo=list.get(0) //记录每个批次开始时候的订单号
            }
            //以每个申请进件创建节点
            createNodeSetProperty(graphdb, list)
          } catch {
            //异常情况处理
            case e: Exception => {
              println("createNode failure，current filename"+ currentFilePath +"，curent line" + i + " content:" + line + " \n" /*+
                "exception:" + e.getMessage + " \n" + "exception2:" + e.getStackTraceString*/)
            }
          }

          //多少订单一个批次提交
          try{
            if (i % BATCH_SIZE == 0) {
              //print("batchStartAppNo:"+batchStartAppNo)
              //println("，Commited batch:" + i)
              tx.success()
              tx.close()
              tx = graphdb.beginTx
              lineList.clear()
              //list.clear()
              println("batch commited success ，filename is ："+ currentFilePath + "， batchStartAppNo is " + batchStartAppNo)
            }
          }
          catch {
            case e:Exception=>{
              /*for(row <- lineList){
                //FileUtils.writeDataToTxt(row,rootPath+"\\exceptdata\\"+file.getName+"Exdata.csv")
                print("Commited StartAppNo:"+batchStartAppNo+
                  " exception:" + e.getMessage + " \n" +
                  "exception2:" + e.getStackTraceString)
              }*/
              println("========================================================================")
              println("Commited failure!!! "+batchStartAppNo+ " exception:" + e.getMessage + " \n" + "exception2:" + e.getStackTraceString)
              println("commited failure!!! "+ "current filename"+ currentFilePath + "StartAppNo:" + batchStartAppNo  )
              println("========================================================================")
            }
          }
        }

        /*try{
          println("Commited batch" + i)
          //log.info("Commited batch" + i)
          //print("batchStartAppNo:"+batchStartAppNo)
          tx.success()
          tx.close()
          lineList.clear()
        }
        catch {
          case e:Exception=>{
            for(row <- lineList){
              //FileUtils.writeDataToTxt(row,rootPath+"\\exceptdata\\"+file.getName+"Exdata.csv")
              print("Commited  StartAppNo:"+batchStartAppNo+ " exception:" + e.getMessage + " \n" +
                  "exception2:" + e.getStackTraceString)
            }
          }
        }*/

      }

//      createSchemaIndex()
//      Thread.sleep(500)
    } catch {
      case e: Exception => {
        println("generateGraphdb failure !!! " + "exception:" + e.getMessage + "\n" +
          "exception2:" + e.getStackTraceString)
        tx.failure()
      }
    } finally tx.close()

  }

//  def createSchemaIndex(): Unit = {
//    var tx = graphdb.beginTx
//    graphdb.schema().indexFor(Labels.Apply).on("contentKey").create()
//    graphdb.schema().indexFor(Labels.Device).on("contentKey").create()
//    graphdb.schema().indexFor(Labels.BackCard).on("contentKey").create()
//    graphdb.schema().indexFor(Labels.Phone).on("contentKey").create()
//    graphdb.schema().indexFor(Labels.Email).on("contentKey").create()
//    graphdb.schema().indexFor(Labels.IDCard).on("contentKey").create()
//    tx.success()
//  }

  /**
    * for dir all file
    *
    * @param dir
    * @return
    */
  def subdirs(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory)
    val files = dir.listFiles().filter(_.isFile).toIterator
    files ++ dirs.toIterator.flatMap(subdirs _)
  }
  /**
    * 创建数据库数据
    * @param graphdb
    * @param list
    */
  def createNodeSetProperty(graphdb: GraphDatabaseService, list: java.util.List[String]): Unit = {
    //先解析实体列表
    //实体创建顺序依据源数据文件字段顺序
    var orderId = list.get(0)      //申请订单号
    val contractNo = list.get(1)  //申请合同号
    val termId = list.get(2)     //终端id
    val loanPan = list.get(3)  //贷款银行卡
    val returnPan = list.get(4)  //还款银行卡
    val insertTime = list.get(5)  //订单申请时间
    val recommend = list.get(6)  //推荐人电话
    val userId = list.get(7)   //用户id
    val certNo = list.get(8)  //身份证
    val email = list.get(9)  //邮箱
    val company = list.get(10)  //单位名称
    val mobile = list.get(11)  //申请手机
    val companyAddr = list.get(12)  //单位地址
    val companyPhone = list.get(13)  //单位固话
    val emergencyMobile = list.get(14)  //紧急联系人电话
    val contactMobile = list.get(15)  //联系人电话
    val ipv4 = list.get(16)   //ipv4
    val msgphone = list.get(17)
    val telecode = list.get(18)
    val deviceId = list.get(19)  //手机IMEI
    //关联实体：身份证、申请手机、联系人手机、紧急联系人手机、银行卡号（贷款）、IMEI、EMAIL
    var apply:Node=null;  //申请订单
    //异常数据处理
    if(orderId.contains(":"))
        orderId = orderId.replace(":","")
    if (StringUtils.isNoneEmpty(orderId)) {
      //创建nodeMap之前，查询是否已经存在
      var applyNode:Option[Node]=nodeMap.get(orderId)
      if (!applyNode.isDefined) {
        apply = graphdb.createNode(Labels.Apply)  //创建标签
        apply.setProperty(CommonConstant.CONTENTKEY, orderId)  //创建字段名称
        nodeMap.put(orderId, apply)  //将节点放入nodeMap
        applyNode = nodeMap.get(orderId)  //从nodeMap获取节点数据
      }
    }else{
      return ;
    }

    //创建身份证实体
    if (StringUtils.isNoneEmpty(certNo)) {
      var idcard: Option[Node] = nodeMap.get(certNo)
      if (!idcard.isDefined) {
        val tmpNode = graphdb.createNode(Labels.IDCard)   //创建标签
        tmpNode.setProperty(CommonConstant.CONTENTKEY, certNo)  //创建字段名称
        nodeMap.put(certNo, tmpNode)
        idcard = nodeMap.get(certNo)
      }
      apply.createRelationshipTo(idcard.get, RelationshipTypes.IDCARD)
    }

    //创建设备号，过滤设备号异常数据
    if (StringUtils.isNoneEmpty(deviceId)&& !"\\N".equals(deviceId)&& !"-".equals(deviceId)) {
      var imei: Option[Node] = nodeMap.get(deviceId)
      if (!imei.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Device)
        tmpNode.setProperty(CommonConstant.CONTENTKEY, deviceId)
        nodeMap.put(deviceId, tmpNode)
        imei = nodeMap.get(deviceId)
      }
      apply.createRelationshipTo(imei.get, RelationshipTypes.DEVICE)
    }

    //申请手机号
    if (StringUtils.isNoneEmpty(mobile)&& !"\\N".equals(mobile)&& !"-".equals(mobile)) {
      var phone: Option[Node] = nodeMap.get(mobile)
      if (!phone.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Phone)
        tmpNode.setProperty(CommonConstant.CONTENTKEY, mobile)
        nodeMap.put(mobile, tmpNode)
        phone = nodeMap.get(mobile)
      }
      apply.createRelationshipTo(phone.get, RelationshipTypes.MYPHONE)
    }

    //邮箱
    if (StringUtils.isNoneEmpty(email)&& !"\\N".equals(email)&& !"-".equals(email)) {
      var emailNode: Option[Node] = nodeMap.get(email)
      if (!emailNode.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Email)
        tmpNode.setProperty(CommonConstant.CONTENTKEY, list.get(4))
        nodeMap.put(email, tmpNode)
        emailNode = nodeMap.get(email)
      }
      apply.createRelationshipTo(emailNode.get, RelationshipTypes.EMAIL)
    }

    //贷款银行卡
    if (StringUtils.isNoneEmpty(loanPan)&& !"\\N".equals(loanPan)&& !"-".equals(loanPan)&& !"ds!".equals(loanPan)) {
      var bankcard: Option[Node] = nodeMap.get(loanPan)
      if (!bankcard.isDefined) {
        val tmpNode = graphdb.createNode(Labels.BankCard)
        tmpNode.setProperty(CommonConstant.CONTENTKEY, loanPan)
        nodeMap.put(loanPan, tmpNode)
        bankcard = nodeMap.get(loanPan)
      }
      apply.createRelationshipTo(bankcard.get, RelationshipTypes.BANKCARD)
    }

    //联系人电话
    if (StringUtils.isNoneEmpty(contactMobile)&& !"\\N".equals(contactMobile)&& !"-".equals(contactMobile)) {
          var relatives: Option[Node] = nodeMap.get(contactMobile)  //节点是否存在，以实体值判断
            if (!relatives.isDefined) {
              val tmpNode = graphdb.createNode(Labels.Phone)  //所有电话类，标签均一致
              tmpNode.setProperty(CommonConstant.CONTENTKEY, contactMobile)
              nodeMap.put(contactMobile, tmpNode)
              relatives = nodeMap.get(contactMobile)
            }
            apply.createRelationshipTo(relatives.get, RelationshipTypes.CONTACT)
    }

    //紧急联系人电话
    if (StringUtils.isNoneEmpty(emergencyMobile)&& !"\\N".equals(emergencyMobile)&& !"-".equals(emergencyMobile)) {
      var relatives: Option[Node] = nodeMap.get(emergencyMobile)  //节点是否存在，以实体值判断
      if (!relatives.isDefined) {
        val tmpNode = graphdb.createNode(Labels.Phone)  //所有电话类，标签均一致
        tmpNode.setProperty(CommonConstant.CONTENTKEY, emergencyMobile)
        nodeMap.put(emergencyMobile, tmpNode)
        relatives = nodeMap.get(emergencyMobile)
      }
      apply.createRelationshipTo(relatives.get, RelationshipTypes.EMERGENCY)  //关系类别不同
    }


  }
}


