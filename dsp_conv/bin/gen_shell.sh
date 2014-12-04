#!/bin/bash
. /etc/profile
SEM_HOME=$(cd "$(dirname "$0")/..";pwd)

. $SEM_HOME/conf/data.conf
. $SEM_HOME/common/common.sh
. $SEM_HOME/bin/conv_proc_by_name.sh

today=$(date +"%Y%m%d")
yesterday=$(date -d "-1 days" +%Y%m%d)
delday=$(date -d "-7 days" +%Y%m%d)

#today=20141126
#yesterday=20141125
hour=$(date -d "0 hours" +%H);
#hour=12

#if [[ $hour -eq 0 ]];then
#  ./conversion_process.sh 1
#fi
#./conversion_process.sh 0 

#Get click file
download_process $today $hour;


#初始化Internal日志路径 
PVUV_DIR="/user/log/trc/${today}/{atac.conv.${hour},atac.conv_x.${hour}}.*" 
CLICK_DIR="/user/log/trc/${today}/CL.${hour}.*"
ORDER_DIR="/user/log/trc/${today}/CV.${hour}.*"

#HDFS ROOT PATH
datasage_hdfs_path="/datapath/adop/dsp/conversion"

hadoop dfs -rmr $datasage_hdfs_path/clickdata/delday

#初始化External日志路径
hdfs_29day_path="${datasage_hdfs_path}/click_30_days"
hdfs_1day_path="${datasage_hdfs_path}/clickdata/${yesterday}/*"
hdfs_hour_path="${datasage_hdfs_path}/clickdata/${today}/"

#MYSQL OPER
getconversion=$SEM_HOME/sql/conversion.sql
getclickconversion=$SEM_HOME/sql/conversionclick.sql
pvuv_dir=${datasage_hdfs_path}/pageviewdetail*/treasure*
click_dir=${datasage_hdfs_path}/clickdetail*/treasure*
order_dir=${datasage_hdfs_path}/orderdetail*/treasure*
ui_dir=${datasage_hdfs_path}/uidetail*/treasure*

read_regex(){
   write_log "get conversion from 73 DB" $SEM_HOME/log/log.txt
   #从73数据库的conversion表中获取转换规则
   mysql -h$REG_MYSQL_HOST \
         -u$REG_MYSQL_USER \
         -p$REG_MYSQL_PWD \
    --port=$REG_MYSQL_PORT \
         --default-character-set=utf8 \
         < $getconversion \
         >$SEM_HOME/dynamic/conversion.tsv
   mysql -h$REG_MYSQL_HOST \
         -u$REG_MYSQL_USER \
         -p$REG_MYSQL_PWD \
    --port=$REG_MYSQL_PORT \
         --default-character-set=utf8 \
         < $getclickconversion \
         >$SEM_HOME/dynamic/conversionclick.tsv
   if [[ $? -ne 0  ]];then
   write_log "get conversion from 73 DB failed,please check DB." $SEM_HOME/log/log.txt
   fi
   #将从数据中取到的结果的标题删除
   sed -e '1d' $SEM_HOME/dynamic/conversion.tsv > $SEM_HOME/dynamic/conversion_tmp.tsv
   sed -e '1d' $SEM_HOME/dynamic/conversionclick.tsv > $SEM_HOME/dynamic/conversionclick_tmp.tsv
   #上传转换规则到HDFS
   
   hadoop dfs -rmr ${datasage_hdfs_path}/conversion.tsv
   hadoop dfs -rmr ${datasage_hdfs_path}/conversionclick.tsv
   hadoop dfs -put $SEM_HOME/dynamic/conversion_tmp.tsv  ${datasage_hdfs_path}/conversion.tsv
   hadoop dfs -put $SEM_HOME/dynamic/conversionclick_tmp.tsv ${datasage_hdfs_path}/conversionclick.tsv
}

read_regex

#获取conversion url
sh $SEM_HOME/common/urlbak.sh
write_log "get regex url" $SEM_HOME/log/log.txt
getConversionDay pvurl pv $SEM_HOME
getConversionDay uvurl uv $SEM_HOME
getConversionDay orderurl order $SEM_HOME
getConversionDay clickurl click $SEM_HOME

#--------------合成转化-----------
write_log "start to process data from HDFS pv" $SEM_HOME/log/log.txt
nohup $SEM_HOME/service/conv.sh $SEM_HOME $PVUV_DIR $hdfs_29day_path $hdfs_1day_path $hdfs_hour_path $today ${datasage_hdfs_path}/conversion.tsv pv &

write_log "start to process data from HDFS uv" $SEM_HOME/log/log.txt
nohup $SEM_HOME/service/conv.sh $SEM_HOME $PVUV_DIR $hdfs_29day_path $hdfs_1day_path $hdfs_hour_path $today ${datasage_hdfs_path}/conversion.tsv uv &

write_log "start to process data from HDFS cl" $SEM_HOME/log/log.txt
nohup $SEM_HOME/service/conv.sh $SEM_HOME $CLICK_DIR $hdfs_29day_path $hdfs_1day_path $hdfs_hour_path $today ${datasage_hdfs_path}/conversionclick.tsv click &

write_log "start to process data from HDFS cv" $SEM_HOME/log/log.txt
nohup $SEM_HOME/service/conv.sh $SEM_HOME $ORDER_DIR $hdfs_29day_path $hdfs_1day_path $hdfs_hour_path $today ${datasage_hdfs_path}/conversion.tsv order &
#
#_____________Upload To Ftp__________
wait
upload_process $today $hour pv
upload_process $today $hour uv
upload_process $today $hour click
upload_process $today $hour order
echo "****general service finished****"
