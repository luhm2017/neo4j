#!/bin/sh
startdate=`date -d "$1" +%Y-%m-%d`
enddate=`date -d "$2" +%Y-%m-%d`

while [[ $startdate < $enddate  ]]
do
    echo "########`date -d "$startdate" +%Y-%m`#########"
    pt_month=`date -d "$startdate" +%Y-%m`
    ./spark-submit --master yarn-client \
    --deploy-mode client \
    --conf spark.locality.wait=1 --conf spark.driver.memory=2g \
    --conf spark.executor.cores=40 --total-executor-cores 120 --num-executors 3 --executor-memory 80g \
    --jars /usr/share/java/mysql-connector-java-5.1.46.jar \
    --driver-class-path /usr/share/java/mysql-connector-java-5.1.46.jar \
    --class exportData.ExportApplyData1 \
    /bigdata/neo4j-1.0-SNAPSHOT.jar fqz fqz_apply_contract_data_month $pt_month > temp$pt_month.log  &
    #将开始时间增加一个月
    startdate=`date -d "+1 month $startdate" +%Y-%m-%d`

done

##ssh脚本提交任务
##=====================================
#!/bin/sh
echo "############开始执行脚本####################"
hive sql > temp.log