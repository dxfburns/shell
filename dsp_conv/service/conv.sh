#!/bin/bash
function conv_process_service(){
	SEM_HOME=$1
	SERV_DIR=$2
	hdfs_29day_path=$3
	hdfs_1day_path=$4
	hdfs_hour=$5
	today=$6
	conv_tmp_dir=$7
	conv_type=$8

	hdfs_out_path="/datapath/adop/dsp/conversion/${conv_type}_out/"

	source $SEM_HOME/common/common.sh	
	
	pig_param_path=$SEM_HOME/dynamic/pig_${conv_type}_param30.pig

	echo "----- getALLRegURL -----"
	EACHREG="'"`getALLRegURL $conv_type $SEM_HOME`"'"        
		cd $SEM_HOME/dynamic        
		rm -rf $pig_param_path      
		touch $pig_param_path      
		echo eachreg=$EACHREG>>$pig_param_path      
		sed -i 's/\$/\\\\$/g' $pig_param_path      

	#while read i;do
		
		

		#获取到所有的url过滤条件

		#pig_param_path=$SEM_HOME/dynamic/pig_${conv_type}_param${i}.pig
       
		

		#if [[ $i -eq '1' ]];then
		#	PIGNAME=$SEM_HOME/pig/conv_${conv_type}_1_days.pig
			
		#	pig -m $pig_param_path -p indir="$SERV_DIR" -p convday="$i" -p jobname="dsp_tracking_${conv_type}_$i" \
		#	-p hdfs_1day_path=$hdfs_1day_path \
		#	-p hdfs_hour=$hdfs_hour \
		#	-p conv_tmp_dir=$conv_tmp_dir \
		#	-p output=$hdfs_out_path \
		#	$PIGNAME	
		#else
		#	y=`expr $i + 1`
		#	EXSTARTTIME=$(date -d $today" -$y day" +%Y%m%d"235959L")
		#	echo "EXSTARTTIME=$EXSTARTTIME"
		#	PIGNAME=$SEM_HOME/pig/conv_${conv_type}_30_days.pig

			#pig -m $pig_param_path -p indir="$SERV_DIR" -p convday="$i" -p jobname="dsp_tracking_${conv_type}_$i" \
                        #-p hdfs_1day_path=$hdfs_1day_path \
			#-p hdfs_29day_path=$hdfs_29day_path \
                        #-p hdfs_hour=$hdfs_hour \
                        #-p conv_tmp_dir=$conv_tmp_dir \
			#-p output=$hdfs_out_path \
			#-p exstarttime=$EXSTARTTIME \
                        #$PIGNAME			
		#fi
		
	#done < $SEM_HOME/dynamic/${conv_type}_converdays.txt
		
	hadoop dfs -rmr $hdfs_out_path		
	
	PIGNAME=$SEM_HOME/pig/conv_${conv_type}_1_days.pig
			
	
	pig -m $pig_param_path -p indir="$SERV_DIR" -p convday="30" -p jobname="dsp_tracking_${conv_type}_30" \
		-p hdfs_1day_path=$hdfs_1day_path \
		-p hdfs_hour=$hdfs_hour \
		-p conv_tmp_dir=$conv_tmp_dir \
		-p output=$hdfs_out_path \
		$PIGNAME
}

conv_process_service $1 $2 $3 $4 $5 $6 $7 $8
