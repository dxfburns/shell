#!/bin/bash
function getConversionDay(){
	ctype=$1
	local HOME=$3
	fileName=$HOME/dynamic/$2"_converdays.txt"
	days=`awk -F ' ' '{print $2}' $HOME/dynamic/$ctype/result.csv`
	rm -f $fileName
	touch $fileName
	for list in $days
	do
		commandstr=`cat $fileName | grep -c $list`
		if [ $commandstr -lt '1' ]
		then
			echo $list >> $fileName
		fi
	done
}
function getRegURL(){
	DAY=$1;
	REGTYPE=$2
	local HOME=$3
	URLMATCHES=1
	while read j;
	do
        CONVERURL=`echo $j | awk -F ' ' '{print $1}'`
        CONVERDAY=`echo $j | awk -F ' ' '{print $2}'`
       if [[ $CONVERDAY == $DAY ]]; then
	   if [[ $URLMATCHES == '1' ]]; then 
	            REGURL=$CONVERURL
	       else
	            REGURL=$REGURL","$CONVERURL
           fi  
       	((URLMATCHES++))           
        fi  
	done < $HOME/dynamic/$REGTYPE"url"/result.csv
	echo $REGURL;
}

function getALLRegURL(){
	REGTYPE=$1
	local HOME=$2
	URLMATCHES=1
	while read j;
	do
        CONVERURL=`echo $j | awk -F ' ' '{print $1}'`
        CONVERDAY=`echo $j | awk -F ' ' '{print $2}'`
       
	   if [[ $URLMATCHES == '1' ]]; then 
	            REGURL=$CONVERURL
	       else
	            REGURL=$REGURL","$CONVERURL
           fi  
       	((URLMATCHES++))           
       
	done < $HOME/dynamic/$REGTYPE"url"/result.csv
	echo $REGURL;
}

#按adid分割文件的函数
function splitFileByAdid(){
   adidFilename=$1
   originalfilename=$2
   fileType=$3
   prj_home_path=$4
   local yesterday=$5
   #按照adid进行将原文件进行分割
   awk -F"\t" '{if(NR==FNR)array[$1]}{if(NR>FNR){if(($13!=NULL)&&($13 in array)) print >"./result/"$13;else print >"./result/"0}}' $adidFilename $originalfilename 
   
   cd $prj_home_path/dynamic/result/

   mkdir -p $prj_home_path/dynamic/$fileType/$yesterday
   mv ./* $prj_home_path/dynamic/$fileType/$yesterday
   
   cd $prj_home_path/dynamic/$fileType/$yesterday
   
   #打包/$fileType/$yesterday路径下所有文件按照adid生成的文件
   gzip -n *

   #生成md5值
   for file in `ls`
   do
        md5sum $file| awk '{print $1}' >> $file".md5"
   done
cd $prj_home_path/dynamic
}
function write_log(){
	echo "[$(date +'%Y-%m-%d %H:%M:%S')][$0]:$GREEN$1$NORMAL" |tee -a $2
}
