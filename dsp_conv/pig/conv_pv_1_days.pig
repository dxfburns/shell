SET default_parallel 5; 
SET job.name '$jobname'; 
REGISTER /work/hadoopneed/semudf/MiscSemBank.jar; 
define ipqueryforgeoid com.adsage.pig.bank.udf.keywordreport.IPQueryForGeoID(); 
define ipquery com.adsage.pig.bank.udf.keywordreport.IPQuery(); 
define geturldomain com.adsage.pig.bank.udf.GetURLDomain(); 
define datecale com.adsage.pig.bank.udf.keywordreport.DateCale(); 
define transcode com.adsage.pig.bank.udf.keywordreport.BaiduTransReplace(); 
define urlextract com.adsage.pig.bank.udf.keywordreport.UrlExtract(); 
define wordsplit com.adsage.pig.bank.udf.Split(); 
define istrac com.adsage.pig.bank.udf.Pubsagestrint(); 
define pingantran com.adsage.pig.bank.udf.Pingantran(); 
define urlmatchreg com.adsage.pig.bank.udf.UrlMatchReg(); 
define longToDate com.adsage.pig.bank.udf.ConvertLongToDate();
DEFINE urlMatcheRegFilter com.adsage.pig.bank.udf.UrlMatchesFilter(); 

IN_DATA = load '$indir' using PigStorage('\t', '-noschema') as (
	conv_date:long,
	ip:chararray,
	ua:chararray,      
	url:chararray,      
	uid:chararray,      
	sid:chararray,      
	RequestFileName:chararray,      
	adid:chararray); 

EX_ONEDAY = load '$hdfs_1day_path' using PigStorage('\t','-noschema') as (
	visit_date:long,
	ip:chararray,
	ua:chararray,
	ref:chararray,
	uid:chararray,
	adid:chararray,
	iid:chararray,
	lp:chararray,
	location:chararray,
	keyword:chararray);

EX_HOUR = load '$hdfs_hour' using PigStorage('\t','-noschema') as (
	visit_date:long,
	ip:chararray,
	ua:chararray,
	ref:chararray,
	uid:chararray,
	adid:chararray,
	iid:chararray,
	lp:chararray,
	location:chararray,
	keyword:chararray);

COVN_TMP_DATA = load '$conv_tmp_dir' using PigStorage('\t', '-noschema') as (
	conversion_id:chararray,
	advertiser_id:chararray,
	conversion_name:chararray,
	description:chararray,
	url:chararray,
	calcindex:chararray,
	convday:int,
	revenue:chararray,
	status:int,
	lastmodifiledtime:chararray,
	addtime:chararray,
	level:int,
	original_url:chararray,
	match_type:int);

IN_DATA_URLFILTER = FILTER IN_DATA by url is not null; 
IN_DATA_REGFILTER = FILTER IN_DATA_URLFILTER by urlMatcheRegFilter(adid, url, '$eachreg'); 
IN_DATA_FILTER = foreach IN_DATA_REGFILTER generate conv_date,ip,ua,url,uid,adid,flatten(urlmatchreg(adid, url, '$eachreg')) as conv_url:chararray; 

COVN_TMP = FILTER COVN_TMP_DATA by calcindex == 'PV';
IN_CONV_JOIN = join IN_DATA_FILTER by (conv_url), COVN_TMP by (url);

EX_DATA = union EX_ONEDAY,EX_HOUR;

EX_IN_JOIN = join EX_DATA by (adid,uid), IN_CONV_JOIN by (adid,uid);

EX_IN_JOIN_FILTER = filter EX_IN_JOIN by visit_date < conv_date;

EX_IN_GROUP = group EX_IN_JOIN_FILTER by (IN_DATA_FILTER::adid,IN_DATA_FILTER::uid,conv_date);

EX_MAX_DATE = foreach EX_IN_GROUP generate flatten(group) as (adid:chararray, uid:chararray, conv_date:long), MAX(EX_IN_JOIN_FILTER.visit_date) as maxexdate;

EX_IN_GROUP_JOIN = join EX_MAX_DATE by (adid, uid, conv_date), IN_CONV_JOIN by (IN_DATA_FILTER::adid, IN_DATA_FILTER::uid, IN_DATA_FILTER::conv_date);

EX_IN_RES = join EX_IN_GROUP_JOIN by (IN_CONV_JOIN::IN_DATA_FILTER::adid, IN_CONV_JOIN::IN_DATA_FILTER::uid, maxexdate), EX_DATA by (adid, uid, visit_date); 

RES_FIN = foreach EX_IN_RES generate
        IN_CONV_JOIN::IN_DATA_FILTER::url AS url,
        IN_CONV_JOIN::COVN_TMP::conversion_id AS conversion_id,
        IN_CONV_JOIN::COVN_TMP::conversion_name AS conversion_name, 
        IN_CONV_JOIN::IN_DATA_FILTER::conv_date AS conv_date,
        EX_DATA::visit_date AS visit_date,
        IN_CONV_JOIN::COVN_TMP::original_url AS original_url,
        EX_DATA::ref as refer,
        EX_DATA::keyword as keyword,
        EX_DATA::iid as iid,
        EX_DATA::uid as uid,
        EX_DATA::lp as lp,
        IN_CONV_JOIN::IN_DATA_FILTER::ip as ip,
        EX_DATA::adid as adid,
        EX_DATA::ua as ua,
        flatten(ipqueryforgeoid(IN_CONV_JOIN::IN_DATA_FILTER::ip)) as (countryid:chararray,provinceid:chararray,cityid:chararray,operator:chararray);

DATA_GEO = load '/datapath/AFS/georelation.tsv' using PigStorage('\t','-noschema') as (country:chararray,province:chararray,city:chararray,country_id:chararray,province_id:chararray,city_id:chararray);

GEO_JOIN = join RES_FIN by (countryid,provinceid,cityid) LEFT OUTER, DATA_GEO by (country_id,province_id,city_id);

RESULT = foreach GEO_JOIN generate 
        longToDate(conv_date) as conv_date:chararray,
        url as original_url,
        longToDate(visit_date) as visit_date:chararray,
        original_url as conv_url,
        conversion_name,
        conversion_id,
        refer,
        keyword,
        iid,
        lp,
        ip,
        CONCAT('(',CONCAT(country,CONCAT(',',CONCAT(province,CONCAT(',',CONCAT(city,')')))))) as location,
        adid,
        uid,
        ua;

store RESULT into '$output';

