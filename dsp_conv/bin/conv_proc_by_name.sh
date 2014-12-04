#!/bin/bash
source /etc/profile

report_home=$(cd "$(dirname "$0")/.."; pwd)
#echo "report_path: $report_home"

hdfs_conv_path=/datapath/adop/dsp/conversion
local_path=$report_home/dynamic/clickData30

. $report_home/bin/ftp_proc_by_name.sh

function download_process(){
today=$1
hour=$2

echo "Get file with $1 $2..."
echo "-------------------------------------------------------------"

if [ ! -d $local_path ]; then
        mkdir -p $local_path
fi

file_name="click_1_${hour}"
zip_file_name="${file_name}.gz"
ftp_download $zip_file_name $local_path $today
#>/dev/null 2>&1

if ls $local_path/${zip_file_name} >/dev/null 2>&1;then
	gzip -df $local_path/${zip_file_name}
fi

echo "-------------------------------------------------------------"
echo "Begin to upload file ${local_path}/${file_name} to HDFS"

hdfs_click_file=$hdfs_conv_path/clickdata/${today}/${file_name}
hadoop dfs -rm -skipTrash ${hdfs_click_file}>/dev/null 2>&1 
hadoop dfs -put ${local_path}/${file_name} ${hdfs_click_file}

echo "End uploading."

}

function upload_process(){
today=$1
hour=$2
conv_type=$3

#echo $conv_type

hdfs_conv_file=${hdfs_conv_path}/${conv_type}_out/*
local_file=${local_path}/${conv_type}_1_${hour}

echo "hdfs_conv_file : ${hdfs_conv_file}"
echo "local file : ${local_file}"

hadoop dfs -cat ${hdfs_conv_file}>${local_file}

if [[ $conv_type == "click" ]];then
	conv_type="cl"
elif [[ $conv_type == "order" ]];then
	conv_type="cv"
fi

zip_file_name="${conv_type}_1_${hour}.gz"
gzip -c ${local_file} > ${local_path}/${zip_file_name}

echo $conv_type

ftp_upload ${zip_file_name} $local_path $today $conv_type

rm -f ${local_path}/${zip_file_name}
rm -f ${local_file}
}
