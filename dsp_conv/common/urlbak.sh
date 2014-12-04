#!/bin/bash
. /etc/profile
DATASAGE_HOME=$(cd "$(dirname "$0")/.."; pwd)
. $DATASAGE_HOME/conf/data.conf

cd  $DATASAGE_HOME/dynamic/geturl
if [[ $? -eq "1" ]];then
	mkdir  $DATASAGE_HOME/dynamic/geturl
fi

#获取转换的正则表达式
cd $DATASAGE_HOME

mysql -h $REG_MYSQL_HOST -u $REG_MYSQL_USER -p$REG_MYSQL_PWD < $DATASAGE_HOME/sql/getClick.sql|sed -n '2,$p'>$DATASAGE_HOME/dynamic/geturl/cl.csv
mysql -h $REG_MYSQL_HOST -u $REG_MYSQL_USER -p$REG_MYSQL_PWD < $DATASAGE_HOME/sql/getPv.sql|sed -n '2,$p'>$DATASAGE_HOME/dynamic/geturl/pv.csv
mysql -h $REG_MYSQL_HOST -u $REG_MYSQL_USER -p$REG_MYSQL_PWD < $DATASAGE_HOME/sql/getUv.sql|sed -n '2,$p'>$DATASAGE_HOME/dynamic/geturl/uv.csv
mysql -h $REG_MYSQL_HOST -u $REG_MYSQL_USER -p$REG_MYSQL_PWD < $DATASAGE_HOME/sql/getOrder.sql|sed -n '2,$p'>$DATASAGE_HOME/dynamic/geturl/cv.csv

cd $DATASAGE_HOME/dynamic/
rm -r clickurl
rm -r pvurl
rm -r orderurl
rm -r uvurl

mkdir clickurl
mkdir pvurl
mkdir orderurl
mkdir uvurl

cp  $DATASAGE_HOME/dynamic/geturl/cl.csv   $DATASAGE_HOME/dynamic/clickurl/result.csv
cp  $DATASAGE_HOME/dynamic/geturl/pv.csv  $DATASAGE_HOME/dynamic/pvurl/result.csv
cp  $DATASAGE_HOME/dynamic/geturl/cv.csv   $DATASAGE_HOME/dynamic/orderurl/result.csv
cp  $DATASAGE_HOME/dynamic/geturl/uv.csv  $DATASAGE_HOME/dynamic/uvurl/result.csv


cd  $DATASAGE_HOME/dynamic/bakpvurl
if [ $? -eq "1" ]
then
	mkdir  $DATASAGE_HOME/dynamic/bakpvurl
fi
cd $DATASAGE_HOME/dynamic/bakorderurl
if [ $? -eq "1" ]
then
	mkdir  $DATASAGE_HOME/dynamic/bakorderurl
fi
cd $DATASAGE_HOME/dynamic/bakclickurl
if [ $? -eq "1" ]
then
	mkdir  $DATASAGE_HOME/dynamic/bakclickurl
fi
cd $DATASAGE_HOME/dynamic/bakuvurl
if [ $? -eq "1" ]
then
	mkdir  $DATASAGE_HOME/dynamic/bakuvurl
fi

cd $DATASAGE_HOME/dynamic/pvurl

rowcount=`cat result.csv | wc -l`
if [ "$rowcount" -lt "2" ]
then
	cp $DATASAGE_HOME/dynamic/bakpvurl/result.csv  $DATASAGE_HOME/dynamic/pvurl/result.csv
else
	cp $DATASAGE_HOME/dynamic/pvurl/result.csv  $DATASAGE_HOME/dynamic/bakpvurl/result.csv  
fi

cd $DATASAGE_HOME/dynamic/uvurl

rowcount=`cat result.csv | wc -l`
if [ "$rowcount" -lt "2" ]
then
	cp $DATASAGE_HOME/dynamic/bakpvurl/result.csv  $DATASAGE_HOME/dynamic/pvurl/result.csv
else
	cp $DATASAGE_HOME/dynamic/pvurl/result.csv  $DATASAGE_HOME/dynamic/bakpvurl/result.csv  
fi

cd $DATASAGE_HOME/dynamic/orderurl

rowcounts=`cat result.csv | wc -l`
if [ "$rowcounts" -lt "2" ]
then
	cp $DATASAGE_HOME/dynamic/bakorderurl/result.csv  $DATASAGE_HOME/dynamic/orderurl/result.csv
else
	cp $DATASAGE_HOME/dynamic/orderurl/result.csv  $DATASAGE_HOME/dynamic/bakorderurl/result.csv
fi

cd $DATASAGE_HOME/dynamic/clickurl

rowcounts=`cat result.csv | wc -l`
if [ "$rowcounts" -lt "2" ]
then
	cp $DATASAGE_HOME/dynamic/bakclickurl/result.csv  $DATASAGE_HOME/dynamic/clickurl/result.csv
else
	cp $DATASAGE_HOME/dynamic/clickurl/result.csv   $DATASAGE_HOME/dynamic/bakclickurl/result.csv
fi

#:<<BLOCK 
#cd /work/sql/pig/datacenter_new/userinfourl

#rowcounts=`cat result.csv | wc -l`
#if [ "$rowcounts" -lt "1" ]
#then
#	cp /work/sql/pig/datacenter_new/bakuserinfourl/result.csv  /work/sql/pig/datacenter_new/userinfourl/result.csv
#else
#	cp /work/sql/pig/datacenter_new/userinfourl/result.csv  /work/sql/pig/datacenter_new/bakuserinfourl/result.csv
#fi
#BLOCK
