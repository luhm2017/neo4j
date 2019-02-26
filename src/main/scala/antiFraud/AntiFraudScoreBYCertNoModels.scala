package antiFraud

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/7/29
  * 反欺诈风险评分
  */
object AntiFraudScoreBYCertNoModels /*extends Logging*/{
  val sparkConf = new SparkConf().setAppName("AntiFraudScoreBYCertNoModels")
  val sc = new SparkContext(sparkConf)
  val hc = new HiveContext(sc)

  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkConf.set("spark.rdd.compress","true")
  sparkConf.set("spark.hadoop.mapred.output.compress","true")
  sparkConf.set("spark.hadoop.mapred.output.compression.codec","true")
  sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
  sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")

  def main(args: Array[String]): Unit = {
    if(args.length!=16){
      println("请输入参数：database、table以及mysql相关参数")
      System.exit(0)
    }

    //分别传入库名、表名、mysql相关参数
    val database = args(0)
    val table = args(1)

  }

  //特征变量
  def dataProcessing(): Unit ={
    //特征变量处理
    //统计每列缺失率、统计值
    val rawData = hc.sql("select * from knowledge_graph.fqz_sample_data")
    //遍历每列，计算每列的统计值
    //非空元素计算、
    rawData.describe("bankcard_cnt3_idcard_idcard", "degree1_bankcard_cnt1_exception_self_companyphone","degree1_black_imei_cnt_exception_self","bankcard_cnt_idcard_idcard").show()
    val df = rawData.select("bankcard_cnt3_idcard_idcard", "degree1_bankcard_cnt1_exception_self_companyphone","degree1_black_imei_cnt_exception_self")
    df.select("bankcard_cnt3_idcard_idcard","degree1_bankcard_cnt1_exception_self_companyphone").count()
    df.count()
    val statData = rawData.describe()
    statData.write.saveAsTable("knowledge_graph.temp_feature_analysis")
    rawData.columns
    rawData.describe("degree1_bankcard_cnt1").show()
    val cols = rawData.columns

  }

  //spark shell 训练模型
  def trainGBDTModel(database:String,table:String): Unit ={
    //好样本数据
    val data0 = hc.sql(s" select * from knowledge_graph.cert_no_relation_features a where a.label = 0  DISTRIBUTE BY RAND() SORT BY RAND() limit 13000").map{
      row =>
        val arr = new ArrayBuffer[Double]()
        //剔除label、contact字段
        for(i <- 3 until row.size){
          if(row.isNullAt(i)){
            arr += 0.0
          }else if(row.get(i).isInstanceOf[Double])
            arr += row.getDouble(i)
          else if(row.get(i).isInstanceOf[Long])
            arr += row.getLong(i).toDouble
          else if(row.get(i).isInstanceOf[Int])
            arr += row.getInt(i).toDouble
          else arr += 0.0
        }
        LabeledPoint(row.getInt(0).toDouble, Vectors.dense(arr.toArray))
    }

    //坏样本数据
    val data1 = hc.sql(s"select * from knowledge_graph.cert_no_rel ation_features  where label = 1 ").map{
      row =>
        val arr = new ArrayBuffer[Double]()
        //剔除label、contact字段
        for(i <- 3 until row.size){
          if(row.isNullAt(i)){
            arr += 0.0
          }else if(row.get(i).isInstanceOf[Double])
            arr += row.getDouble(i)
          else if(row.get(i).isInstanceOf[Long])
            arr += row.getLong(i).toDouble
          else if(row.get(i).isInstanceOf[Int])
            arr += row.getInt(i).toDouble
          else arr += 0.0
        }
        LabeledPoint(row.getInt(0).toDouble, Vectors.dense(arr.toArray))
    }

    //合并数据
    val data = data0.union(data1)

    //data.take(10)
    //train a gbdt model
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    //组合参数调试
    val subSamplingRate = 0.8
    val maxBins = 32
    val minInstancePerNode = 50
    val numTrees = 30
    val maxDepth = 5
    boostingStrategy.treeStrategy.setMinInstancesPerNode(minInstancePerNode)
    boostingStrategy.treeStrategy.setMaxBins(maxBins) //
    boostingStrategy.treeStrategy.setSubsamplingRate(subSamplingRate)
    // 逻辑回归是迭代算法，所以缓存训练数据的RDD
    data.cache()
    // Split data into training (60%) and test (40%)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 11L)
    //============================start time
    boostingStrategy.setNumIterations(numTrees)
    boostingStrategy.treeStrategy.setMaxDepth(maxDepth)
    //train model
    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)
    println("======================="+model.toString)
    //evaluation model on test data
    val predictionAndLabels = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    //===================================================================
    /*//使用BinaryClassificationMetrics评估模型
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    // Precision by threshold
    val precision = metrics.precisionByThreshold//.filter(x => x._1%0.1 ==0)
    // Recall by threshold
    val recall = metrics.recallByThreshold.filter(x => x._1%0.1 ==0)

    //the beta factor in F-Measure computation.
    val f1Score = metrics.fMeasureByThreshold//.filter(x => x._1%0.1 ==0)
    // flScore avg
    val flScoreAvg = f1Score.map(x => x._2).reduce(_+_)/f1Score.count
    //合并precision、recall、f1score
    //auc
    val auc = metrics.areaUnderROC()*/
    //======================================================================
    //使用混淆矩阵BinaryClassificationMetrics评估模型
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    // Precision by threshold
    val precision = metrics.precisionByThreshold
    precision/*.filter(x => x._1%0.1 ==0)*/.map({case (threshold, precision) =>
      "Threshold: "+threshold+",Precision:"+precision
    })
    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall/*.filter(x => x._1%0.1 ==0)*/.map({case (threshold, recall) =>
      "Threshold: "+threshold+",Recall:"+recall
    }).saveAsTextFile("")
    //the beta factor in F-Measure computation.
    val f1Score = metrics.fMeasureByThreshold
    f1Score/*.filter(x => x._1%0.1 ==0)*/.map(x => {"Threshold: "+x._1+"--> F-score:"+x._2+"--> Beta = 1"})
    val beta=0.5
    val f1Score_1= metrics.fMeasureByThreshold(beta)
    f1Score_1/*.filter(x => x._1%0.1 ==0)*/.map(x => {"Threshold: "+x._1+"--> F-score:"+x._2+"--> Beta = 0.5"})

    // Precision-Recall Curve
    val prc = metrics.pr
    prc.map(x => {"Recall: " + x._1 + "--> Precision: "+x._2 })
    // AUPRC，精度，召回曲线下的面积
    val auPRC = metrics.areaUnderPR
    sc.makeRDD(Seq("Area under precision-recall curve = " +auPRC))
    //roc
    val roc = metrics.roc
    roc.map(x => {"FalsePositiveRate:" + x._1 + "--> Recall: " +x._2})
    // AUC
    val auROC = metrics.areaUnderROC
    sc.makeRDD(Seq("Area under ROC = " + +auROC))
    println("Area under ROC = " + auROC)
    val testMSE = predictionAndLabels.map{  case(v, p) => math.pow((v - p), 2)}.mean()
    sc.makeRDD(Seq("Test Mean Squared Error = " + testMSE))
    sc.makeRDD(Seq("GradientBoostingRegression model: " + model.toDebugString))
    //}
  }

}
