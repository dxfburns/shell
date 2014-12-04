#!/usr/bin/env bash
#
# Copyright (C) 2013 by Lele Long <schemacs@gmail.com>
# This file is free software, distributed under the GPL License.
#
# <BRIEF DESCRIPTION HERE>
#

#http://www.thegeekstuff.com/2010/07/execute-shell-script/
#http://stackoverflow.com/questions/2683279/how-to-detect-if-a-script-is-being-sourced
#[[ $_ != $0 ]] && echo "Script is being sourced" || echo "Script is a subshell"


# DIR="$( cd "$(dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [[ ! -z "$TERM" && "$TERM" != "dumb" ]]; then
    NORMAL=$(tput sgr0)
    GREEN=$(tput setaf 2; tput bold)
    YELLOW=$(tput setaf 3)
    RED=$(tput setaf 1)
else
    NORMAL=""
    GREEN=""
    YELLOW=""
    RED=""
fi


# do not name to `info`, same command exists already
info() {
  echo -e "$GREEN${*}$NORMAL" 1>&2
}


warn() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')][$0]:$GREEN$1$NORMAL" |tee -a $2
}


err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S.%N%z')]: $@" >&2
}


debug() {
  if [[ "$DEBUG" == "true" ]]; then
    info "[$(date +'%Y-%m-%dT%H:%M:%S.%N%z')]: $@" 1>&2
  fi
}


fatal() {
  echo -e "$RED${*}$NORMAL" 1>&2
  exit 1
}


dump_var() {
  for arg in "$@"; do
    debug "$arg=[${!arg}]"
  done
}


require_bin() {
  which "$1"  > /dev/null 2>&1
  if [[ $? -eq 0 ]]; then
    return 0
  fi
  return 1
}


dfs_assert_exist() {
  for path in "$@"; do
    if ! hadoop dfs -test -e "$path" >/dev/null ; then
      fatal "dfs path not found: $path"
    fi
  done
}


assert_exist() {
  for path in "$@"; do
    if [[ ! -e "$path" ]]; then
      fatal "path not found: $path"
    fi
  done
}


get_callee() {
    local caller="$(ps --no-headers -o pid,cmd -p $$ | awk '{print $NF'})"
    echo "$caller"
}


profiling(){
  if [[ $# -eq 3 ]]; then
    #module=${1:0:4}
    local module=$1
    local start_time=$2
    local end_time=$3
  elif [[ $# -eq 2 ]]; then
    local module='Mars'
    local start_time=$1
    local end_time=$2
  else
    err "profiling argument error"
    return
  fi

  local bs=$(date -d "$start_time" +%s)
  local es=$(date -d "$end_time" +%s)
  ((diff=es-bs))
  debug "$module: $start_time ~ $end_time: ${diff}s"
}

get_sql_conf(){
    cur_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    python "$cur_dir/render.py" "$@"
}


mysql2dfs(){
  local dfs_filepath=$1
  local mysql_bin_cmd=$2
  local sql="$3"

  hadoop dfs -test -e "$dfs_filepath" && hadoop dfs -rmr "$dfs_filepath"
  $mysql_bin_cmd -N -e "$sql"  | hadoop dfs -put /dev/fd/0 "$dfs_filepath"

  #verify_dfs_data $dfs_filepath 'fileempty' 1 1
}

#  define to get file from ftp and put it to hadoop
ftp2dfs(){
    dfs_filepath=$1
    ftp_server=$2
    ftp_user="$3"
    ftp_pass="$4"
    ftp_filepath=$5
    tmp_filepath=$(mktemp --suffix=dmp_ftp_tmp)
    #dump_var "dfs_filepath" "ftp_server" "ftp_filepath"

    #TODO http://www.krazyworks.com/using-ftp-with-pipes/
    hadoop dfs -rmr "$dfs_filepath" 2>/dev/null
    #TODO empty password
    ftp -ivn <<EOF
             open "$ftp_server"
             user "$ftp_user" "$ftp_pass"
             get "$ftp_filepath" "$tmp_filepath"
             bye
EOF
    hadoop dfs -put "$tmp_filepath" "$dfs_filepath"
    rm "$tmp_filepath"
    #verify_dfs_data "$dfs_filepath" 'fileempty' 1 1
}


is_debug() {
    #if [[ "${@:-1}" == '-d' ]];
    if [[ "${!#}" == '-d' ]];
    then
        return 1
    else
        return 0
    fi
}
#echo $(is_debug "$@")

validate_exist(){
  if [[ ! -e "$1" ]]; then
    echo "$1 do not exist"
    exit 1
  fi
}

urlencode(){
  value="$@"
  if which perl >/dev/null; then
    #echo "using perl"
    #value=$(perl -MURI::Escape -e 'print uri_escape($ARGV[0]);' "$@")
    value=$(perl -MURI::Escape -e "print uri_escape('$@');")
  elif which php >/dev/null; then
    #echo "using php"
    value=$(php -r "echo urlencode(\"$@\");";)
  fi
  echo "$value"
}

#http://ethertubes.com/bash-snippet-url-encoding/
urlencode_new() {
  tab="$(echo -en "\x9")"
  i="$@"
  i=${i//%/%25}  ; i=${i//' '/%20} ; i=${i//$tab/%09}
  i=${i//!/%21}  ; i=${i//\"/%22}  ; i=${i//#/%23}
  i=${i//\$/%24} ; i=${i//\&/%26}  ; #TODO i=${i//\'/%27}
  i=${i//(/%28}  ; i=${i//)/%29}   ; i=${i//\*/%2a}
  i=${i//+/%2b}  ; i=${i//,/%2c}   ; i=${i//-/%2d}
  i=${i//\./%2e} ; i=${i//\//%2f}  ; i=${i//:/%3a}
  i=${i//;/%3b}  ; i=${i//</%3c}   ; i=${i//=/%3d}
  i=${i//>/%3e}  ; i=${i//\?/%3f}  ; i=${i//@/%40}
  i=${i//\[/%5b} ; i=${i//\\/%5c}  ; i=${i//\]/%5d}
  i=${i//\^/%5e} ; i=${i//_/%5f}   ; i=${i//\`/%60}
  i=${i//\{/%7b} ; i=${i//|/%7c}   ; i=${i//\}/%7d}
  i=${i//\~/%7e}
  echo "$i"
  i=""
}

#TODO * expanded to files in cwd
#echo $(urlencode 'a + % * # @ !')
#echo $(urlencode 'a + % # @ !')


is_bash(){
  #bin=$(basename "$SHELL")
  bin=$(lsof -p $$ 2>/dev/null| awk '(NR==2) {print $1}')
  if [[ "$bin" == "bash" ]]; then
    echo "1"
  else
    echo "0"
  fi
}

#DATE_FMT="+%Y%m%d"
DATE_FMT="+%Y-%m-%d"
DATE_DIR_FMT="+%Y/%m/%d"
DATE_SHORT_FMT="+%Y%m%d"


validate_date() {
  if [[ $# -eq 0 ]]; then
    err "no date to validate"
    return 1
  fi
  if ! date -d "$1 0 days ago" "${2:-$DATE_FMT}" >/dev/null; then
    err "invalid date: $1"
    return 1
  fi
  return 0
}


get_date(){
  if [[ $# -eq 1 ]]; then
    date --date="$1 0 days ago" "${DATE_FMT}"
  elif [[ $# -eq 2 ]]; then
    date --date="$1 0 days ago" "${2:-$DATE_FMT}"
  fi
}


get_date_dir() {
  if [[ $# -eq 1 ]]; then
    get_date "$1" "${DATE_DIR_FMT}"
  fi
}


get_short_date() {
  if [[ $# -eq 0 ]]; then
    date --date="1 days ago" "${DATE_SHORT_FMT}"
  elif [[ $# -eq 1 ]]; then
    date --date="$1 0 days ago" "${DATE_SHORT_FMT}"
  fi
}


today() {
  date "${1:-$DATE_FMT}"
}


yesterday() {
  date --date "1 day ago" "${1:-$DATE_FMT}"
}

this_hour() {
  date --date "0 hour ago" "${1:-${DATE_FMT}_%H}"
}

last_hour() {
  date --date "1 hour ago" "${1:-${DATE_FMT}_%H}"
}


days_ago() {
  date --date "${1:-0} day ago" "${2:-$DATE_FMT}"
}

hours_ago() {
  date --date "${1:-0} hour ago" "${2:-${DATE_FMT}_%H}"
}


run_cmd(){
    cmd="$1"

    debug "$cmd run start"
    bash "$cmd" &

    wait
    debug "$cmd run end"
}


run_module(){
  if [[ $# -eq 2 ]]; then
    local module="$1"
    local cmd="$2"
  elif [[ $# -eq 1 ]]; then
    local module="Duke"
    local cmd="$1"
  else
    return
  fi

  dump_var "cmd"
  local  begin_time=$(date +"%Y-%m-%d %H:%M:%S")
  "$cmd"
  local  end_time=$(date +"%Y-%m-%d %H:%M:%S")
  profiling "$module" "$begin_time" "$end_time"
}


time_range_dirs() {
    # http://stackoverflow.com/questions/3515481/pig-latin-load-multiple-files-from-a-date-range-part-of-the-directory-structur/3716648#3716648
    # http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html#globStatus%28org.apache.hadoop.fs.Path%29
    local next_hour="$1"
    local end_hour="$2"
    local interval="${3:-3600}"
    local formatter="${4:-"%Y%m%d/%H"}"
    local separator="${5:-","}"
    local next_hour_timestamp="$(date -d "$next_hour" "+%s")"
    local end_hour_timestamp="$(date -d "$end_hour" "+%s")"
    dirs_=""
    #dump_var "base_path" "start" "end" "next_hour_timestamp" "end_hour_timestamp"

    if [[ $end_hour_timestamp -lt 0 ]]; then
        warn "You have beat the relative theory!"
        return
    fi
    local formatted="$(time_range_format "$next_hour" "$end_hour" "$interval" "$formatter" "$separator")"
    local separator_count="$(echo "$formatted" | grep -o -F "$separator" | wc -l)"
    # NOTE separator count, not hour count
    if [[ $separator_count -ge 1 ]]; then
        echo "{${formatted}}"
    else
        echo "${formatted}"
    fi
}

time_range_format() {
    # take care of space in for loop, pls
    #for this_hour in $(time_range_format "$next_hour" "$end_hour" 3600 "%Y%m%dT%H:00:00"); do
    #    info bash clk.sh "$(date -d "$this_hour" +"%Y%m%d %H:00:00")"
    #done

    local next_time="$1"
    local end_time="$2"
    local interval="${3:-3600}"
    local formatter="${4:-%Y%m%d/%H}"
    local separator="${5:- }"
    local next_time_timestamp="$(date -d "$next_time" "+%s")"
    local end_time_timestamp="$(date -d "$end_time" "+%s")"
    dirs_=""

    local count=0
    while [[ $next_time_timestamp -lt $end_time_timestamp ]]; do
        this_dir="$(date -d "@$next_time_timestamp" "+$formatter")"
        if [[ $count -eq 0 ]]; then
            dirs_="$this_dir"
        else
            dirs_="$dirs_${separator}$this_dir"
        fi
        ((count++))
        next_time_timestamp=$((next_time_timestamp + interval))
    done
    echo "$dirs_"
}


hourly_range_dirs() {
    local next_hour="$1"
    local end_hour="$2"
    local interval="${3:-3600}"
    local formatter="${4:-"%Y%m%d/%H"}"
    local separator="${5:-","}"
    time_range_dirs "$next_hour" "$end_hour" "$interval" "$formatter" "$separator"
}


time_range_file_exists() {
    local next_time="$1"
    local end_time="$2"
    local interval="${3:-3600}"
    local formatter="${4:-%Y%m%d/%H}"
    local next_time_timestamp="$(date -d "$next_time" "+%s")"
    local end_time_timestamp="$(date -d "$end_time" "+%s")"

    while [[ $next_time_timestamp -lt $end_time_timestamp ]]; do
        this_file="$(date -d "@$next_time_timestamp" "+$formatter")"
        if [[ ! -e "$file" ]]; then
            warn "$file NOT EXISTS"
        elif [[ ! -s "$file" ]]; then
            warn "$file IS EMPTY"
        fi
        next_time_timestamp=$((next_time_timestamp + interval))
    done
}


get_ans() {
    #TODO see also: input_with_timeout(py)
    # ENTER is treated as NO
    local timeout="$1"
    # default_ans only take effect when timeout is used
    local default_ans="$2"
    local prompt="$3"
    local answer=""
    if [[ ! -z "$prompt" ]]; then
        echo -n "$prompt"
    fi
    if [[ -z "$timeout" ]]; then
        read answer
    else
        read -t "$timeout" answer
        if [[ $? -gt 128 ]]; then
            if [[ ! -z "$default_ans" ]]; then
                answer=$default_ans
            fi
        fi
    fi
    shopt -q nocasematch
    nocasematch_bak=$?
    shopt -u nocasematch
    if [[ "$answer" == "Yes" || "$answer" == "Y" || "$answer" == "yes, i do" ]]; then
        echo -n "Y"
    else
        echo -n "N"
    fi
    if [[ $nocasematch_bak -eq 0 ]]; then
        shopt -s nocasematch
    else
        shopt -u nocasematch
    fi
}


safe_rm() {
    local file=$1
    local prefix="${2:-/not_exist}"
    # normalize file path
    file="$(dirname "$file")/$(basename "$file")"
    if [[ "$file" =~ $prefix ]]; then
        echo rm "$file"
    else
        warn "$file is not prefixed with $prefix"
    fi
}


ini_get_val() {
    local path=$1
    local section=$2
    local key=$3
    local val_def=${4:-}
    cur_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    python -c "import sys; sys.path.append('$cur_dir'); import utils; print utils.ini_get_val('$path', '$section', '$key', '$val_def');"
}


ini_get_sections() {
    local path=$1
    local section=$2
    cur_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    echo -e "import sys; sys.path.append('$cur_dir'); import utils;\nfor section in utils.ini_get_sections('$path'): print section,;" | python
}


# http://mivok.net/2009/09/20/bashfunctionoverrist.html
save_function() {
    local orig_func=$(declare -f $1)
    local new_func="$2${orig_func#$1}"
    eval "$new_func"
}


random_line() {
    filename=$1
    line_cnt=$(wc -l "$filename" | grep -o -E '[0-9]+')
    rand_lineno=$((RANDOM % line_cnt + 1))
    sed -n "$rand_lineno {p}" "$filename"
}


wait4bgjobs() {
    local max_job_size=$1
    local enable_echo=${2:-}
    job_size=$(jobs -pr|wc -l)
    if [[ ! -z "$enable_echo" ]]; then
        echo "job_size $job_size"
    fi
    while [[ $job_size -ge $max_job_size ]]; do
        if [[ ! -z "$enable_echo" ]]; then
            echo "job_size $job_size"
        fi
        sleep 1
        job_size=$(jobs -pr|wc -l)
    done
}
