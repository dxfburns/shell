#!/bin/bash
        
#################FTP configuration#######################
ftp_read_server=172.16.60.191
ftp_read_username=dcftp
ftp_read_password=dcMktp@1F9T1P
ftp_path=afs_reader/dsp/click
#########################################################

function ftp_download(){
echo "FTP param:$1,$2,$3"

FILE_NAME=$1
LOCAL_PATH=$2
TODAY=$3

echo "FTP path: $ftp_path/$TODAY/"

echo 'BEGIN FTP GET'

ftp -n << EOF
open $ftp_read_server
user $ftp_read_username $ftp_read_password
cd ${ftp_path}
cd ${TODAY}
binary
lcd ${LOCAL_PATH}/
prompt
get ${FILE_NAME}
close
bye
EOF

echo 'END FTP GET'
}

#################FTP configuration#######################
ftp_server=172.18.30.81
ftp_username=FTPWJH
ftp_password=123456
ftp_base_path=dsp/conv
#########################################################

function ftp_upload(){
echo 'BEGIN FTP PUT'


FILE_NAME=$1
LOCAL_PATH=$2
TODAY=$3

ftp -n <<!

open $ftp_server
user $ftp_username $ftp_password
cd $ftp_base_path
mkdir $TODAY
cd $TODAY
binary
lcd $LOCAL_PATH/
prompt
put $FILE_NAME
close
bye
!

echo 'END FTP'
}

