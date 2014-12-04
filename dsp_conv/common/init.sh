#!/bin/bash
init(){
 TIME_STAMP=$2
 SEM_HOME=$1
 HOUR=$(date +%H)
 YESTERDAY_STAMP=$(perl -e "print $TIME_STAMP-24*3600")
 BEFORE_YESTERDAY_STAMP=$(perl -e "print $TIME_STAMP-2*24*3600")

 TODAY=$(date -d @$TIME_STAMP +"%Y%m%d")
 YESTERDAY=$(date -d @$YESTERDAY_STAMP +"%Y%m%d")
 N_YESTERDAY=$(date -d $YESTERDAY +"%Y-%m-%d")
 BEFORE_YESTERDAY=$(date -d @$BEFORE_YESTERDAY_STAMP +"%Y%m%d")

 INTERNAL_START_TIME=$BEFORE_YESTERDAY"235959L"
 INTERNAL_END_TIME=$TODAY"000000L"
 INTERNAL_END_YESTERDAY_TIME=$YESTERDAY"000000L"

 #初始化Internal日志路径
 PVUV_DIR="/user/log/trc/{$YESTERDAY,$BEFORE_YESTERDAY}/{atac.conv,atac.conv_x}.*"
 CLICK_DIR="/user/log/trc/{$YESTERDAY,$BEFORE_YESTERDAY}/CL.*"
 ORDER_DIR="/user/log/trc/{$YESTERDAY,$BEFORE_YESTERDAY}/CV.*"
 UI_DIR="/user/log/trc/{$YESTERDAY,$BEFORE_YESTERDAY}/UI.*"
 #初始化EXTERNAL日志路径
 GEX_DIR="/user/log/trc/{$YESTERDAY,$BEFORE_YESTERDAY}/track.1.0.*"
 PEX_DIR="/user/log/trc/{$YESTERDAY,$BEFORE_YESTERDAY}/track.1.4.*"
 RF_DIR="/user/log/trc/{$YESTERDAY,$BEFORE_YESTERDAY}/RF.*"

 TRACKING_HDFS_PATH="/datapath/sem/tracking"
 DATA_BAK="$TRACKING_HDFS_PATH/datacenterserverbak/"
 ORDER_OFFSET="100L"
 EXTERNAL_DATA_BAK_30=$DATA_BAK'externaldatabak30'
 #lp分发有可能没有日志，上传空文件保证pig可以load到文件
 cd $SEM_HOME/dynamic
 touch CL.00.0000.da-bp-empty.tsv
 touch atac.conv.00.3016.da-bp-de17.tsv
 touch atac.conv_x.00.2213.da-bp-de20.tsv
 touch UI.00.0000.da-bp-empty.tsv
 touch CV.00.0000.da-bp-empty.tsv
 hadoop fs -put CL.00.0000.da-bp-empty.tsv /user/log/trc/$YESTERDAY/ 2>/dev/null
 hadoop fs -put atac.conv_x.00.2213.da-bp-de20.tsv /user/log/trc/$YESTERDAY/ 2>/dev/null
 hadoop fs -put atac.conv.00.3016.da-bp-de17.tsv /user/log/trc/$YESTERDAY/ 2>/dev/null
 hadoop fs -put UI.00.0000.da-bp-empty.tsv /user/log/trc/$YESTERDAY/ 2>/dev/null
 hadoop fs -put CV.00.0000.da-bp-empty.tsv /user/log/trc/$YESTERDAY/ 2>/dev/null
 rm CL.00.0000.da-bp-empty.tsv
 rm UI.00.0000.da-bp-empty.tsv
 rm CV.00.0000.da-bp-empty.tsv
 rm atac.conv_x.00.2213.da-bp-de20.tsv
 rm atac.conv.00.3016.da-bp-de17.tsv
}
