SELECT cc.`conversion_id`,cc.`advertiser_id`,cc.`conversion_name`,cc.`description`,cc.`url`,RIGHT(cc.`calcindex`,LENGTH(cc.`calcindex`)-3),cc.`convday`,cc.`revenue`,cc.`status`,cc.`lastmodifiledtime`,cc.`addtime`,cc.`level`,cc.`originalurl`,cc.`match_type` FROM `TrackingCodeLogData`.`conversion` cc WHERE cc.`status`=1 AND cc.`calcindex` LIKE 'CL%';
