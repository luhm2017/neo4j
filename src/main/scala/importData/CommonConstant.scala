package importData

/**
  * Created by jack on 2017/12/27 0003.
  */

object CommonConstant extends Serializable {
  //配置文件名称
  val CONFIG = "config.properties"
  val NEOIP = "neoIP"
  val BAKNEOIP = "bakneoIP"
  val USER = "user"
  val PASSWORD = "password"

  val GENERALAPPLY_CREDITLOAN = "generalApply_CreditLoan"
  val LOGNO = "logNo"
  val FLATMAP = "flatMap"

  //audit json 字段名称
  val cLoanApply = "cLoanApply"
  val cApplyUser = "cApplyUser"
  val sendTime = "sendTime"


  // kafka 配置文件参数
  val METADATA_BROKER_LIST = "metadata.broker.list"
  val AUTO_OFFSET_RESET = "auto.offset.reset"
  val KAFKA_GROUP_ID = "group.id"
  val CODE = "UTF-8"
  //spark stream 配置参数
  val SPARK_UI_SHOWCONSOLEPROGRESS = "spark.ui.showConsoleProgress"
  val SPARK_LOCALITY_WAIT = "spark.locality.wait"
  val SPARK_STREAMING_KAFKA_MAXRETRIES = "spark.streaming.kafka.maxRetries"
  val SPARK_SERIALIZER = "spark.serializer"
  val KAFKA_MAXRATEPERPARTITION = "spark.streaming.kafka.maxRatePerPartition"
  val SPARK_STREAMING_CONCURRENTJOBS = "spark.streaming.concurrentJobs"
  val SPARK_STREAMING_BACKPRESSURE_ENABLED = "spark.streaming.backpressure.enabled"

  //neo4j 执行需要建立字段名称
  val ORDERNO = "appNo"
  val CONTENTKEY = "contentKey"


  val BANKCARD = "bankcard"
  val RECOMMEND = "recommend"
  val USER_ID = "user_id"
  val DEVICE_ID = "device_id"
  val CERT_NO = "cert_no"
  val CERTNO = "certno"
  val EMAIL = "email"
  val COMPANY = "company"
  val MOBILE = "mobile"
  val EMERGENCYMOBILE = "emergencymobile"


//  val jsonVNeo4jFieldMap = Map(
//    "orderno" -> "orderno",
//    "userid" -> "user_id",
//    "insertTime" -> "insertTime",
//    "emergencymobile" -> "emergencymobile",
//    "usermobile" -> "usermobile",
//    "contactmobile" -> "contact_mobile",
//    "cert_no" -> "cert_no",
//    "debitcard" -> "debitcard",
//    "hometel" -> "hometel",
//    "termid" -> "termid",
//    "email" -> "email",
//    "creditcard" -> "creditcard",
//    "ipv4" -> "ipv4",
//    "mobile" -> "mobile",
//    "usercoremobile" -> "usercoremobile",
//    "recommend" -> "recommend",
//    "_DeviceId" -> "device_id",
//    "companyname" -> "company",
//    "companyaddress" -> "comp_addr",
//    "companytel" -> "comp_phone",
//    "merchantmobile" -> "merchantmobile",
//    "channelmobile" -> "channelmobile",
//    "partnercontantmobile" -> "merchantmobile")
//
//  val neo4jVRelationFieldMap = Map(
//    "emergencymobile" -> "recommend",
//    "usermobile" -> "recommend",
//    "contact_mobile" -> "relativemobile",
//    "cert_no" -> "identification",
//    "debitcard" -> "bankcard",
//    "hometel" -> "hometel",
//    "termid" -> "terminal",
//    "email" -> "email",
//    "creditcard" -> "bankcard",
//    "ipv4" -> "ipv4",
//    "mobile" -> "applymymobile",
//    "usercoremobile" -> "loginmobile",
//    "recommend" -> "recommend",
//    "device_id" -> "device",
//    "company" -> "company",
//    "comp_addr" -> "companyaddress",
//    "comp_phone" -> "companytel",
//    "merchantmobile" -> "merchantmobile",
//    "channelmobile" -> "channelmobile",
//    "merchantmobile" -> "merchantmobile")
//
//  val neo4jVLabelMap = Map(
//    "orderno" -> "ApplyInfo",
//    "emergencymobile" -> "Mobile",
//    "usermobile" -> "Mobile",
//    "contact_mobile" -> "Mobile",
//    "cert_no" -> "Identification",
//    "debitcard" -> "BankCard",
//    "hometel" -> "Mobile",
//    "termid" -> "Terminal",
//    "email" -> "Email",
//    "creditcard" -> "BankCard",
//    "ipv4" -> "IPV4",
//    "mobile" -> "Mobile",
//    "usercoremobile" -> "Mobile",
//    "recommend" -> "Mobile",
//    "device_id" -> "Device",
//    "company" -> "Company",
//    "comp_addr" -> "CompanyAddress",
//    "comp_phone" -> "CompanyTel",
//    "merchantmobile" -> "Mobile",
//    "channelmobile" -> "Mobile",
//    "merchantmobile" -> "Mobile")
//
//  val auditJsonVNeo4jFieldMap = Map("orderId" -> "orderno", "userId" -> "user_id",
//    "insertTime" -> "insertTime", "termId" -> "termid", "loanPan" -> "debitcard",
//    "mobile" -> "mobile", "certNo" -> "cert_no", "contactMobile" -> "contact_mobile",
//    "emergencyContactMobile" -> "emergencymobile", "compAddr" -> "comp_addr",
//    "email" -> "email", "company" -> "company")
//
//  val fieldMap = Map(
//    "emergencymobile" -> "emergencymobile,recommend",
//    "usermobile" -> "usermobile,recommend",
//    "contactmobile" -> "contact_mobile,relativemobile",
//    "certno" -> "cert_no,identification",
//    "debitcard" -> "debitcard,bankcard",
//    "hometel" -> "hometel,hometel",
//    "termid" -> "termid,terminal",
//    "email" -> "email,email",
//    "creditcard" -> "creditcard,bankcard",
//    "ipv4" -> "ipv4,ipv4",
//    "mobile" -> "mobile,applymymobile",
//    "usercoremobile" -> "usercoremobile,loginmobile",
//    "recommend" -> "recommend,recommend",
//    "_DeviceId" -> "device_id,device",
//    "companyname" -> "company,company",
//    "companyaddress" -> "comp_addr,companyaddress",
//    "companytel" -> "comp_phone,companytel",
//    "merchantmobile" -> "merchantmobile,merchantmobile",
//    "channelmobile" -> "channelmobile,channelmobile",
//    "partnercontantmobile" -> "merchantmobile,merchantmobile")

//  val labelMap = Map(
//    "emergencymobile" -> Labels.Mobile,
//    "usermobile" -> Labels.Mobile,
//    "contactmobile" -> Labels.Mobile,
//    "certno" -> Labels.Identification,
//    "debitcard" -> Labels.BankCard,
//    "hometel" -> Labels.Mobile,
//    "termid" -> Labels.Terminal,
//    "email" -> Labels.Email,
//    "creditcard" -> Labels.BankCard,
//    "ipv4" -> Labels.IPV4,
//    "mobile" -> Labels.Mobile,
//    "usercoremobile" -> Labels.Mobile,
//    "recommend" -> Labels.Mobile,
//    "_DeviceId" -> Labels.Device,
//    "companyname" -> Labels.Company,
//    "companyaddress" -> Labels.CompanyAddress,
//    "companytel" -> Labels.CompanyTel,
//    "merchantmobile" -> Labels.Mobile,
//    "channelmobile" -> Labels.Mobile,
//    "partnercontantmobile" -> Labels.Mobile)


//  val relationShipMap = Map(
//    "emergencymobile" -> RelationshipTypes.recommend,
//    "usermobile" -> RelationshipTypes.recommend,
//    "contactmobile" -> RelationshipTypes.relativemobile,
//    "certno" -> RelationshipTypes.identification,
//    "debitcard" -> RelationshipTypes.bankcard,
//    "hometel" -> RelationshipTypes.hometel,
//    "termid" -> RelationshipTypes.terminal,
//    "email" -> RelationshipTypes.email,
//    "creditcard" -> RelationshipTypes.bankcard,
//    "ipv4" -> RelationshipTypes.ipv4,
//    "mobile" -> RelationshipTypes.applymymobile,
//    "usercoremobile" -> RelationshipTypes.loginmobile,
//    "recommend" -> RelationshipTypes.recommend,
//    "_DeviceId" -> RelationshipTypes.device,
//    "companyname" -> RelationshipTypes.company,
//    "companyaddress" -> RelationshipTypes.companyaddress,
//    "companytel" -> RelationshipTypes.companytel,
//    "merchantmobile" -> RelationshipTypes.merchantmobile,
//    "channelmobile" -> RelationshipTypes.channelmobile,
//    "partnercontantmobile" -> RelationshipTypes.merchantmobile)

  val PSUBSCRIBE = "psubscribe"

}
