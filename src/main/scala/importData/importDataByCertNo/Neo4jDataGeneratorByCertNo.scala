package importData.importDataByCertNo

import java.io.File

import com.google.common.base.Splitter
import importData.{CommonConstant, Labels, RelationshipTypes}
import org.apache.commons.lang3.StringUtils
import org.neo4j.graphdb._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by zhijie.guo on 2017/12/27 0031.
  */
class Neo4jDataGeneratorByCertNo /*extends DataGenerator*/ {
  private val logger = LoggerFactory.getLogger("Neo4jDataGeneratorByCertNo")
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
        var batchStartCertNo=""  //每个批次开始的certNo
        //每个文件逐行读取
        for (line <- Source.fromFile(file,"UTF-8").getLines()) {
          i += 1
          lineList+=line  //记录每行记录
          import scala.collection.JavaConversions._
          val list = Splitter.on(",").trimResults().split(line).toList //csv文件以","作为分割符
          try {
            if(i % BATCH_SIZE == 1){
              batchStartCertNo=list.get(0) //记录每个批次开始时候的CertNo
            }
            //以每个CertNo创建节点
            createNodeSetProperty(graphdb, list)
          } catch {
            //异常情况处理
            case e: Exception => {
              println("createNode failure，current filename"+ currentFilePath +"，curent line" + i + " content:" + line + " \n" +
                "exception:" + e.getMessage + " \n" + "exception2:" + e.getStackTraceString)
              logger.info("createNode failure，current filename"+ currentFilePath +"，curent line" + i + " content:" + line + " \n" /*+
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
              println("batch commited success ，filename is ："+ currentFilePath + "， batchStartAppNo is " + batchStartCertNo)
              logger.info("batch commited success ，filename is ："+ currentFilePath + "， batchStartAppNo is " + batchStartCertNo)
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
              println("Commited failure!!! "+batchStartCertNo+ " exception:" + e.getMessage + " \n" + "exception2:" + e.getStackTraceString)
              println("commited failure!!! "+ "current filename"+ currentFilePath + "StartAppNo:" + batchStartCertNo  )
              println("========================================================================")
              logger.info("========================================================================")
              logger.info("Commited failure!!! "+batchStartCertNo+ " exception:" + e.getMessage + " \n" + "exception2:" + e.getStackTraceString)
              logger.info("commited failure!!! "+ "current filename"+ currentFilePath + "StartAppNo:" + batchStartCertNo  )
              logger.info("========================================================================")
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
    val certNo = list.get(0) //身份证
    val insertTime = list.get(1) //客户最初始时间
    val mobileSet = list.get(2) //申请手机
    val contactMobileSet = list.get(3) //联系人手机
    val emergencyContactMobileSet = list.get(4) //紧急联系人手机
    val deviceIdSet = list.get(5) //IMEI
    val emailSet = list.get(6) //EMAIL
    val compPhoneSet = list.get(7) //公司电话
    val loanPanSet = list.get(8) //贷款银行

    var apply: Node = null; //申请人
    if (StringUtils.isNotEmpty(certNo)) {
      //创建nodeMap之前，查询是否已经存在
      var applyNode: Option[Node] = nodeMap.get(certNo)
      if (!applyNode.isDefined) {
        apply = graphdb.createNode(Labels.Apply) //创建标签
        apply.setProperty(CommonConstant.CONTENTKEY, certNo) //创建字段名称
        //订单节点添加进件属性
        apply.setProperty(CommonConstant.APPLY_DATE,insertTime) //申请时间
        /*apply.setProperty(CommonConstant.PRODUCT_NAME,productName) //产品名称
        apply.setProperty(CommonConstant.PERFORMANCE,performance) // 是否Q标拒绝 other_refuse 或者 q_refuse
        apply.setProperty(CommonConstant.APPLY_STATE,applyState) //申请状态
        apply.setProperty(CommonConstant.APPLY_LAST_STATE,applyLastState) //申请状态名称
        apply.setProperty(CommonConstant.FAIL_REASON,failReason) //拒绝原因
        apply.setProperty(CommonConstant.CURRENT_DUE_DAY,currentDueDay) //当前逾期天数
        apply.setProperty(CommonConstant.HISTORY_DUE_DAY,historyDueDay) //历史最大逾期天数*/
        //添加身份证号，以判断是否本身申请
        //apply.setProperty(CommonConstant.CERT_NO,certNo)

        nodeMap.put(certNo, apply) //将节点放入nodeMap
        applyNode = nodeMap.get(certNo) //从nodeMap获取节点数据
      }
    } else {
      return;
    }

    //创建设备号，过滤设备号异常数据
    if (StringUtils.isNotEmpty(deviceIdSet) && !"\\N".equals(deviceIdSet) && !"-".equals(deviceIdSet)) {
      val deviceIdLists = deviceIdSet.split("##").toList
      for (deviceIdList <- deviceIdLists) {
        //提取时间
        val deviceId = deviceIdList.split("@@").toList(0)
        val insertTime = deviceIdList.split("@@").toList(1)
        var imei: Option[Node] = nodeMap.get(deviceId)
        if (!imei.isDefined) {
          val tmpNode = graphdb.createNode(Labels.Device)
          tmpNode.setProperty(CommonConstant.CONTENTKEY, deviceId)
          //添加时间
          tmpNode.setProperty(CommonConstant.APPLY_DATE,insertTime)
          nodeMap.put(deviceId, tmpNode)
          imei = nodeMap.get(deviceId)
        }
        apply.createRelationshipTo(imei.get, RelationshipTypes.DEVICE)
      }
    }

    //申请手机号
    if (StringUtils.isNotEmpty(mobileSet) && !"\\N".equals(mobileSet) && !"-".equals(mobileSet)) {
      val mobileLists = mobileSet.split("##").toList
      for (mobileList <- mobileLists) {
        //提取时间
        val mobile = mobileList.split("@@").toList(0)
        val insertTime = mobileList.split("@@").toList(1)
        var phone: Option[Node] = nodeMap.get(mobile)
        if (!phone.isDefined) {
          val tmpNode = graphdb.createNode(Labels.Phone)
          tmpNode.setProperty(CommonConstant.CONTENTKEY, mobile)
          //添加时间
          tmpNode.setProperty(CommonConstant.APPLY_DATE,insertTime)
          nodeMap.put(mobile, tmpNode)
          phone = nodeMap.get(mobile)
        }
        apply.createRelationshipTo(phone.get, RelationshipTypes.MYPHONE)
      }
    }

    //邮箱
    if (StringUtils.isNotEmpty(emailSet) && !"\\N".equals(emailSet) && !"-".equals(emailSet)) {
      val emailLists = emailSet.split("##").toList
      for (emailList <- emailLists) {
        //提取时间
        val email = emailList.split("@@").toList(0)
        val insertTime = emailList.split("@@").toList(1)
        var emailNode: Option[Node] = nodeMap.get(email)
        if (!emailNode.isDefined) {
          val tmpNode = graphdb.createNode(Labels.Email)
          tmpNode.setProperty(CommonConstant.CONTENTKEY, email)
          //添加时间
          tmpNode.setProperty(CommonConstant.APPLY_DATE,insertTime)
          nodeMap.put(email, tmpNode)
          emailNode = nodeMap.get(email)
        }
        apply.createRelationshipTo(emailNode.get, RelationshipTypes.EMAIL)
      }
    }

    //贷款银行卡
    if (StringUtils.isNotEmpty(loanPanSet) && !"\\N".equals(loanPanSet) && !"-".equals(loanPanSet) && !"ds!".equals(loanPanSet)) {
      val loanPanLists = loanPanSet.split("##").toList
      for (loanPanList <- loanPanLists) {
        //提取时间
        val loanPan = loanPanList.split("@@").toList(0)
        val insertTime = loanPanList.split("@@").toList(1)
        var bankcard: Option[Node] = nodeMap.get(loanPan)
        if (!bankcard.isDefined) {
          val tmpNode = graphdb.createNode(Labels.BankCard)
          tmpNode.setProperty(CommonConstant.CONTENTKEY, loanPan)
          //添加时间
          tmpNode.setProperty(CommonConstant.APPLY_DATE,insertTime)
          nodeMap.put(loanPan, tmpNode)
          bankcard = nodeMap.get(loanPan)
        }
        apply.createRelationshipTo(bankcard.get, RelationshipTypes.BANKCARD)
      }
    }

    //联系人电话
    if (StringUtils.isNotEmpty(contactMobileSet) && !"\\N".equals(contactMobileSet) && !"-".equals(contactMobileSet)) {
      val contactMobileLists = contactMobileSet.split("##").toList
      for (contactMobileList <- contactMobileLists) {
        //提取时间
        val contactMobile = contactMobileList.split("@@").toList(0)
        val insertTime = contactMobileList.split("@@").toList(1)
        var relatives: Option[Node] = nodeMap.get(contactMobile) //节点是否存在，以实体值判断
        if (!relatives.isDefined) {
          val tmpNode = graphdb.createNode(Labels.Phone) //所有电话类，标签均一致
          tmpNode.setProperty(CommonConstant.CONTENTKEY, contactMobile)
          //添加时间
          tmpNode.setProperty(CommonConstant.APPLY_DATE,insertTime)
          nodeMap.put(contactMobile, tmpNode)
          relatives = nodeMap.get(contactMobile)
        }
        apply.createRelationshipTo(relatives.get, RelationshipTypes.CONTACT)
      }
    }

    //紧急联系人电话
    if (StringUtils.isNotEmpty(emergencyContactMobileSet) && !"\\N".equals(emergencyContactMobileSet) && !"-".equals(emergencyContactMobileSet)) {
      val emergencyMobileLists = emergencyContactMobileSet.split("##").toList
      for (emergencyMobileList <- emergencyMobileLists) {
        //提取时间
        val emergencyMobile = emergencyMobileList.split("@@").toList(0)
        val insertTime = emergencyMobileList.split("@@").toList(1)
        var relatives: Option[Node] = nodeMap.get(emergencyMobile) //节点是否存在，以实体值判断
        if (!relatives.isDefined) {
          val tmpNode = graphdb.createNode(Labels.Phone) //所有电话类，标签均一致
          tmpNode.setProperty(CommonConstant.CONTENTKEY, emergencyMobile)
          //添加时间
          tmpNode.setProperty(CommonConstant.APPLY_DATE,insertTime)
          nodeMap.put(emergencyMobile, tmpNode)
          relatives = nodeMap.get(emergencyMobile)
        }
        apply.createRelationshipTo(relatives.get, RelationshipTypes.EMERGENCY) //关系类别不同
      }
    }

    //单位电话 companyPhone
    if (StringUtils.isNotEmpty(compPhoneSet) && !"\\N".equals(compPhoneSet) && !"-".equals(compPhoneSet)) {
      val companyPhoneLists = compPhoneSet.split("##").toList
      for (companyPhoneList <- companyPhoneLists) {
        //提取时间
        val companyPhone = companyPhoneList.split("@@").toList(0)
        val insertTime = companyPhoneList.split("@@").toList(1)
        //剔除单位电话中特殊字符
        val companyTele = companyPhone.replace("|", "").replace("-", "")
        var relatives: Option[Node] = nodeMap.get(companyTele) //节点是否存在，以实体值判断
        if (!relatives.isDefined) {
          val tmpNode = graphdb.createNode(Labels.Phone) //所有电话类，标签均一致
          tmpNode.setProperty(CommonConstant.CONTENTKEY, companyTele)
          //添加时间
          tmpNode.setProperty(CommonConstant.APPLY_DATE,insertTime)
          nodeMap.put(companyTele, tmpNode)
          relatives = nodeMap.get(companyTele)
        }
        apply.createRelationshipTo(relatives.get, RelationshipTypes.COMPANYPHONE) //关系类别不同
      }
    }
  }
}


