--拉取数据
create table if not exists lzy_ant_tianchi_ccf_sl_shop_info as select * from odps_tc_257100_f673506e024.ant_tianchi_ccf_sl_shop_info;
create table if not exists lzy_ant_tianchi_ccf_sl_user_shop_behavior as select * from odps_tc_257100_f673506e024.ant_tianchi_ccf_sl_user_shop_behavior;
create table if not exists lzy_ant_tianchi_ccf_sl_test as select * from odps_tc_257100_f673506e024.ant_tianchi_ccf_sl_test;

--增加训练数据row_id, 0~10913284
drop table if exists lzy_new_user_shop_behavior;
PAI -name AppendId -project algo_public  
-DIDColName=row_id 
-DoutputTableName=lzy_new_user_shop_behavior
-DinputTableName=lzy_ant_tianchi_ccf_sl_user_shop_behavior
-DselectedColNames=user_id,shop_id,time_stamp,longitude,latitude,wifi_infos;

--增加训练数据user_shop_behavior的shop相关字段,时间字段
drop table if exists lzy_new_user_shop_behavior_;
create table lzy_new_user_shop_behavior_ as 
select a.row_id,a.user_id,a.shop_id,a.time_stamp,a.longitude,a.latitude,b.mall_id,b.category_id,b.price,b.shop_longitude,b.shop_latitude,a.wifi_infos
from lzy_new_user_shop_behavior as a left outer join 
(select shop_id,category_id,price,mall_id,longitude as shop_longitude,latitude as shop_latitude from lzy_ant_tianchi_ccf_sl_shop_info) as b 
on a.shop_id = b.shop_id;

--切分时间字段
drop table if exists lzy_new_user_shop_behavior;
create table lzy_new_user_shop_behavior as 
select *,cast(split_part(time_stamp,'-',2) as bigint) as month,
cast(split_part(split_part(time_stamp,'-',3),' ',1) as bigint) as day, 
cast(split_part(split_part(time_stamp,' ',2),':',1) as bigint) as hour,
cast(split_part(split_part(time_stamp,' ',2),':',2) as bigint) as minite
from lzy_new_user_shop_behavior_;
drop table if exists lzy_new_user_shop_behavior_;

--测试数据,及时间字段
drop table if exists lzy_new_test;
create table lzy_new_test as 
select cast(row_id as bigint) as row_id,user_id,mall_id,time_stamp,longitude,latitude,wifi_infos from lzy_ant_tianchi_ccf_sl_test;

drop table if exists lzy_new_test_;
create table lzy_new_test_ as 
select *,cast(split_part(time_stamp,'-',2) as bigint) as month,
cast(split_part(split_part(time_stamp,'-',3),' ',1) as bigint) as day, 
cast(split_part(split_part(time_stamp,' ',2),':',1) as bigint) as hour,
cast(split_part(split_part(time_stamp,' ',2),':',2) as bigint) as minite
from lzy_new_test;
drop table if exists lzy_new_test;
create table lzy_new_test as select * from lzy_new_test_;
drop table if exists lzy_new_test_;

--切分wifi_infos
drop table if exists lzy_new_wifi_train;
create table lzy_new_wifi_train as 
select lzy_split_wifiinfos(row_id,wifi_infos) as (row_id,bssid,rssi,connect) from lzy_new_user_shop_behavior;
drop table if exists lzy_new_wifi_test;
create table lzy_new_wifi_test as 
select lzy_split_wifiinfos(row_id,wifi_infos) as (row_id,bssid,rssi,connect) from lzy_new_test;

--填充wifi表的null及connet值,hive不支持update
drop table if exists lzy_new_wifi_train_;
create table lzy_new_wifi_train_ as 
select row_id,bssid,
(case rssi
  when 'null' then null
  else rssi
  end
)as rssi,
(case connect
  when 'false' then 0
  when 'true' then 1
  else 0
  end
)as connect
from lzy_new_wifi_train;

drop table if exists lzy_new_wifi_test_;
create table lzy_new_wifi_test_ as 
select row_id,bssid,
(case rssi
  when 'null' then null
  else rssi
  end
)as rssi,
(case connect
  when 'false' then 0
  when 'true' then 1
  else 0
  end
)as connect
from lzy_new_wifi_test;

--训练集wifi添加shop_id相关字段,时间字段,测试集wifi添加mall_id字段,时间字段
drop table if exists lzy_new_wifi_train;
create table lzy_new_wifi_train as 
select a.row_id,a.bssid,a.rssi,a.connect,b.shop_id,b.mall_id,b.category_id,b.price,b.month,b.day,b.hour,b.minite
from lzy_new_wifi_train_ as a left outer join (select row_id,shop_id,mall_id,category_id,price,month,day,hour,minite from lzy_new_user_shop_behavior) as b 
on a.row_id = b.row_id;

drop table if exists lzy_new_wifi_test;
create table lzy_new_wifi_test as 
select a.row_id,a.bssid,a.rssi,a.connect,b.mall_id,b.month,b.day,b.hour,b.minite
from lzy_new_wifi_test_ as a left outer join (select row_id,mall_id,month,day,hour,minite from lzy_new_test) as b 
on a.row_id = b.row_id;


--表字段类型转换string|bigint|double|date|bool,也可以用sql: cast(col as bigint) as col
drop table if exists lzy_new_wifi_train_;
PAI -name type_transform -project algo_public 
-DinputTable="lzy_new_wifi_train"
-DselectedCols="rssi" 
-Dpre_type="string"
-Dnew_type="bigint" 
-DoutputTable="lzy_new_wifi_train_"
-Dlifecycle="28";
drop table if exists lzy_new_wifi_train;
create table lzy_new_wifi_train as select * from lzy_new_wifi_train_;
drop table if exists lzy_new_wifi_train_;

drop table if exists lzy_new_wifi_test_;
PAI -name type_transform -project algo_public 
-DinputTable="lzy_new_wifi_test"
-DselectedCols="rssi" 
-Dpre_type="string"
-Dnew_type="bigint" 
-DoutputTable="lzy_new_wifi_test_"
-Dlifecycle="28";
drop table if exists lzy_new_wifi_test;
create table lzy_new_wifi_test as select * from lzy_new_wifi_test_;
drop table if exists lzy_new_wifi_test_;

--rowidwifi排序,rssi_rank为考虑重复rssi的排名,top_rank为不考虑重复rssi的排名
drop table if exists lzy_new_wifi_train_;
create table lzy_new_wifi_train_ as 
select *, row_number() over(partition by row_id order by rssi desc) as top_rank,rank() over(partition by row_id order by rssi desc) as rssi_rank from lzy_new_wifi_train;
drop table if exists lzy_new_wifi_train;
create table lzy_new_wifi_train as select * from lzy_new_wifi_train_;
drop table if exists lzy_new_wifi_train_;

drop table if exists lzy_new_wifi_test_;
create table lzy_new_wifi_test_ as 
select *, row_number() over(partition by row_id order by rssi desc) as top_rank,rank() over(partition by row_id order by rssi desc) as rssi_rank from lzy_new_wifi_test;
drop table if exists lzy_new_wifi_test;
create table lzy_new_wifi_test as select * from lzy_new_wifi_test_;
drop table if exists lzy_new_wifi_test_;


--拆分区间
--7.1~8.17做8.18~8.31统计区间，7.15~8.31做9.1~9.14测试集的统计区间
--8.18~8.31正样本2799866,9.1~9.14记录数2402119
--区间

drop table if exists lzy_new_tongji_offline;
create table lzy_new_tongji_offline as 
select * from lzy_new_user_shop_behavior where (month==8 and day>=18 and day<=31);
drop table if exists lzy_new_tongji_wifi_offline;
create table lzy_new_tongji_wifi_offline as 
select * from lzy_new_wifi_train where row_id in (select row_id from lzy_new_tongji_offline);

drop table if exists lzy_new_tongji_online;
create table lzy_new_tongji_online as 
select row_id,user_id,mall_id,time_stamp,longitude,latitude,wifi_infos,month,day,hour,minite from lzy_new_test;
drop table if exists lzy_new_tongji_wifi_online;
create table lzy_new_tongji_wifi_online as 
select * from lzy_new_wifi_test;

--统计区间
drop table if exists lzy_new_tongji_offlinewin;
create table lzy_new_tongji_offlinewin as 
select * from lzy_new_user_shop_behavior where month==7 or (month==8 and day<=17);
drop table if exists lzy_new_tongji_wifi_offlinewin;
create table lzy_new_tongji_wifi_offlinewin as 
select * from lzy_new_wifi_train where row_id in (select row_id from lzy_new_tongji_offlinewin);

drop table if exists lzy_new_tongji_onlinewin;
create table lzy_new_tongji_onlinewin as 
select * from lzy_new_user_shop_behavior where (month==7 and day>=15) or month==8 ;
drop table if exists lzy_new_tongji_wifi_onlinewin;
create table lzy_new_tongji_wifi_onlinewin as 
select * from lzy_new_wifi_train where row_id in (select row_id from lzy_new_tongji_onlinewin);

---isnull和notnull
drop table if exists lzy_new_isnull_offline;
create table lzy_new_isnull_offline as 
select distinct row_id from lzy_new_tongji_wifi_71_831 where rssi is null;
drop table if exists lzy_new_notnull_offline;
create table lzy_new_notnull_offline as 
select distinct row_id from lzy_new_tongji_wifi_71_831 where rssi is not null;

drop table if exists lzy_new_isnull_online;
create table lzy_new_isnull_online as 
select distinct row_id from lzy_new_wifi_test where rssi is null;
drop table if exists lzy_new_notnull_online;
create table lzy_new_notnull_online as 
select distinct row_id from lzy_new_wifi_test where rssi is not null;

--店铺在统计区间的交易经纬度中值
drop table if exists lzy_new_shopcenter_offlinewin;
create table lzy_new_shopcenter_offlinewin as 
select mall_id,shop_id,median(longitude) as shop_median_longitude,median(latitude) as shop_median_latitude from lzy_new_tongji_offlinewin group by mall_id,shop_id;

drop table if exists lzy_new_shopcenter_onlinewin;
create table lzy_new_shopcenter_onlinewin as 
select mall_id,shop_id,median(longitude) as shop_median_longitude,median(latitude) as shop_median_latitude from lzy_new_tongji_onlinewin group by mall_id,shop_id;

--bssid在shop里出现了多少次，shop总bssid数，bssid在mall的总出现数,及rate_inshop,rate_inbssid,
--7.1~8.17 
drop table if exists lzy_new_shop_wifi_offlinewin;
create table lzy_new_shop_wifi_offlinewin as 
select mall_id,bssid,shop_id,count(1) as bssid_count_inshop from lzy_new_tongji_wifi_offlinewin group by mall_id,bssid,shop_id;

drop table if exists lzy_new_shop_wifi_offlinewin_;
create table lzy_new_shop_wifi_offlinewin_ as 
select a.*,b.shop_wificount_sum from lzy_new_shop_wifi_offlinewin as a
left outer join (select mall_id,shop_id,sum(bssid_count_inshop) as shop_wificount_sum from lzy_new_shop_wifi_offlinewin group by mall_id,shop_id) as b
on a.mall_id=b.mall_id and a.shop_id=b.shop_id;

drop table if exists lzy_new_shop_wifi_offlinewin;
create table lzy_new_shop_wifi_offlinewin as 
select a.*,b.bssid_count_sum,bssid_count_inshop/shop_wificount_sum as bssid_count_rate_inshop,bssid_count_inshop/bssid_count_sum as bssid_count_rate_inbssid from lzy_new_shop_wifi_offlinewin_ as a
left outer join (select mall_id,bssid,sum(bssid_count_inshop) as bssid_count_sum from lzy_new_shop_wifi_offlinewin_ group by mall_id,bssid) as b
on a.mall_id=b.mall_id and a.bssid=b.bssid;
--7.15~8.31
drop table if exists lzy_new_shop_wifi_onlinewin;
create table lzy_new_shop_wifi_onlinewin as 
select mall_id,bssid,shop_id,count(1) as bssid_count_inshop from lzy_new_tongji_wifi_onlinewin group by mall_id,bssid,shop_id;

drop table if exists lzy_new_shop_wifi_onlinewin_;
create table lzy_new_shop_wifi_onlinewin_ as 
select a.*,b.shop_wificount_sum from lzy_new_shop_wifi_onlinewin as a
left outer join (select mall_id,shop_id,sum(bssid_count_inshop) as shop_wificount_sum from lzy_new_shop_wifi_onlinewin group by mall_id,shop_id) as b
on a.mall_id=b.mall_id and a.shop_id=b.shop_id;

drop table if exists lzy_new_shop_wifi_onlinewin;
create table lzy_new_shop_wifi_onlinewin as 
select a.*,b.bssid_count_sum,bssid_count_inshop/shop_wificount_sum as bssid_count_rate_inshop,bssid_count_inshop/bssid_count_sum as bssid_count_rate_inbssid from lzy_new_shop_wifi_onlinewin_ as a
left outer join (select mall_id,bssid,sum(bssid_count_inshop) as bssid_count_sum from lzy_new_shop_wifi_onlinewin_ group by mall_id,bssid) as b
on a.mall_id=b.mall_id and a.bssid=b.bssid;

--店铺wifi在统计区间的rssi中值,最强值
--7.1~8.17
drop table if exists lzy_new_shop_wifi_offlinewin_;
create table lzy_new_shop_wifi_offlinewin_ as 
select a.*,b.bssid_median_rssi_inshop,b.bssid_max_rssi_inshop from lzy_new_shop_wifi_offlinewin as a left outer join 
(select bssid,shop_id,median(rssi) as bssid_median_rssi_inshop ,max(rssi) as bssid_max_rssi_inshop from lzy_new_tongji_wifi_offlinewin where row_id in (select row_id from lzy_new_notnull_offline) group by bssid,shop_id) as b
on a.bssid=b.bssid and a.shop_id=b.shop_id;

drop table if exists lzy_new_shop_wifi_offlinewin;
create table lzy_new_shop_wifi_offlinewin as select * from lzy_new_shop_wifi_offlinewin_;
drop table if exists lzy_new_shop_wifi_offlinewin_;
--7.15~8.31
drop table if exists lzy_new_shop_wifi_onlinewin_;
create table lzy_new_shop_wifi_onlinewin_ as 
select a.*,b.bssid_median_rssi_inshop,b.bssid_max_rssi_inshop from lzy_new_shop_wifi_onlinewin as a left outer join 
(select bssid,shop_id,median(rssi) as bssid_median_rssi_inshop ,max(rssi) as bssid_max_rssi_inshop from lzy_new_tongji_wifi_onlinewin where row_id in (select row_id from lzy_new_notnull_online) group by bssid,shop_id) as b
on a.bssid=b.bssid and a.shop_id=b.shop_id;

drop table if exists lzy_new_shop_wifi_onlinewin;
create table lzy_new_shop_wifi_onlinewin as select * from lzy_new_shop_wifi_onlinewin_;
drop table if exists lzy_new_shop_wifi_onlinewin_;

-----------------------------------------------------------------规则
--row_id的wifi在统计区间交互过的shop
--8.18~8.31正样本 2799866,row所有wifi交互过的该mall的shop 45887886,倍数16.38 覆盖率0.9426
--9.1~9.14倍数18.4
drop table if exists lzy_new_wifi_appear_shop_before_offline;
create table lzy_new_wifi_appear_shop_before_offline as 
select c.row_id,c.shop_id,c.pred_shop,sum(c.bssid_count_inshop) as bssid_count_inshop from
(select a.row_id,a.shop_id,b.pred_shop,b.bssid_count_inshop
  from (select row_id,mall_id,shop_id,bssid from lzy_new_tongji_wifi_offline) as a 
  inner join (select mall_id,shop_id as pred_shop,bssid,bssid_count_inshop from lzy_new_shop_wifi_offlinewin) as b 
  on a.mall_id = b.mall_id and a.bssid = b.bssid) as c 
group by c.row_id,c.shop_id,c.pred_shop;

drop table if exists lzy_new_wifi_appear_shop_before_online;
create table lzy_new_wifi_appear_shop_before_online as 
select c.row_id,c.pred_shop,sum(c.bssid_count_inshop) as bssid_count_inshop from
(select a.row_id,b.pred_shop,b.bssid_count_inshop
  from (select row_id,mall_id,bssid from lzy_new_tongji_wifi_online) as a 
  inner join (select mall_id,shop_id as pred_shop,bssid,bssid_count_inshop from lzy_new_shop_wifi_onlinewin) as b 
  on a.mall_id = b.mall_id and a.bssid = b.bssid) as c 
group by c.row_id,c.pred_shop;

----row的topKwifi在统计区间筛选记录里交互过的shop，筛选条件是只保留topN
----3,3，8.4~8.17选到0.9637 rowid,倍数7.3651,覆盖率0.93996
----5,3,0.96649,8.4,0.9422
----3,5,0.9656,8.806,0.94373
--drop table if exists lzy_new_topwifi_appear_selected_records_shop_before_818_831;
--create table lzy_new_topwifi_appear_selected_records_shop_before_818_831 as 
--select distinct a.row_id,a.shop_id,b.pred_shop from 
--(select row_id,mall_id,shop_id,bssid from lzy_new_tongji_wifi_818_831 where rssi_rank<=3) as a 
--inner join (select distinct  mall_id,shop_id as pred_shop,bssid from lzy_new_tongji_wifi_71_817 where rssi_rank<=3) as b 
--on a.mall_id = b.mall_id and a.bssid = b.bssid;


--用户在统计区间到过的该mall的shop
drop table if exists lzy_new_userbeen_shop_before_offline;
create table lzy_new_userbeen_shop_before_offline as 
select a.row_id,a.shop_id,b.pred_shop from
(select row_id,shop_id,user_id,mall_id from lzy_new_tongji_offline) as a inner join 
(select user_id,shop_id as pred_shop,mall_id from lzy_new_tongji_offlinewin) as b on a.user_id=b.user_id and a.mall_id=b.mall_id;

drop table if exists lzy_new_userbeen_shop_before_online;
create table lzy_new_userbeen_shop_before_online as 
select a.row_id,b.pred_shop from
(select row_id,user_id,mall_id from lzy_new_tongji_online) as a inner join 
(select user_id,shop_id as pred_shop,mall_id from lzy_new_tongji_onlinewin) as b on a.user_id=b.user_id and a.mall_id=b.mall_id;


--row到该mall所有shop消费中心的距离
drop table if exists lzy_new_user_distance_to_shop_before_offline;
create table lzy_new_user_distance_to_shop_before_offline as 
select a.*,b.pred_shop,b.shop_median_longitude,b.shop_median_latitude, 
    round(abs(a.longitude-b.shop_median_longitude),6) as longitude_diff, 
  round(abs(a.latitude-b.shop_median_latitude),6) as latitude_diff,
  round(sqrt(pow((a.longitude-b.shop_median_longitude)*111.3195,2)+pow((a.latitude-b.shop_median_latitude)*111.3195,2))*1000,2) as distance_diff from
(select  row_id,mall_id,shop_id,longitude,latitude from lzy_new_tongji_offline ) as a inner join 
(select mall_id,shop_id as pred_shop, shop_median_longitude,shop_median_latitude from lzy_new_shopcenter_offlinewin ) as b on a.mall_id=b.mall_id;

drop table if exists lzy_new_user_distance_to_shop_before_online;
create table lzy_new_user_distance_to_shop_before_online as 
select a.*,b.pred_shop,b.shop_median_longitude,b.shop_median_latitude, 
    round(abs(a.longitude-b.shop_median_longitude),6) as longitude_diff, 
  round(abs(a.latitude-b.shop_median_latitude),6) as latitude_diff,
  round(sqrt(pow((a.longitude-b.shop_median_longitude)*111.3195,2)+pow((a.latitude-b.shop_median_latitude)*111.3195,2))*1000,2) as distance_diff from
(select  row_id,mall_id,longitude,latitude from lzy_new_tongji_online ) as a inner join 
(select mall_id,shop_id as pred_shop, shop_median_longitude,shop_median_latitude from lzy_new_shopcenter_onlinewin ) as b on a.mall_id=b.mall_id;

--用户到店铺pos经纬度距离

---------------------------------------------------------------------------构建样本
--构造候选样本：交互规则+用户规则+距离规则 k个
--格式：k,倍数，覆盖率, 比k-1提升的覆盖率
--7,19.3985,0.96288
--8,19.8863,0.96374 0.00086
--9,20.3936,0.9644 0.00066
--10,20.92709,0.9651 0.0007
--11,21.47859,0.96570 0.0005
--12,22.0530,0.9662 0.0004
--13,22.6441,0.9666 0.0004
--14,23.2535,0.9670 0.0004
--构造训练样本
drop table if exists lzy_new_samples_10_offline;
create table lzy_new_samples_10_offline as 
select row_id,pred_shop as shop_id,cast(shop_id=pred_shop as bigint) as label from
(select row_id,shop_id,pred_shop from lzy_new_userbeen_shop_before_offline 
union select row_id,shop_id,pred_shop from (select *, row_number() over(partition by row_id order by distance_diff) as rank from lzy_new_user_distance_to_shop_before_offline) as a 
where a.rank<=10
union select row_id,shop_id,pred_shop from lzy_new_wifi_appear_shop_before_offline
union select row_id,shop_id,shop_id as pred_shop from lzy_new_tongji_offline) as t; ---验证集也补全了正样本，验证偏高


--构造测试样本
drop table if exists lzy_new_samples_10_online;
create table lzy_new_samples_10_online as 
select row_id,pred_shop as shop_id from
(select row_id,pred_shop from lzy_new_userbeen_shop_before_online 
union select row_id,pred_shop from (select *, row_number() over(partition by row_id order by distance_diff) as rank from lzy_new_user_distance_to_shop_before_online) as a 
where a.rank<=10
union select row_id,pred_shop from lzy_new_wifi_appear_shop_before_online) as t; 

--new构造样本
drop table if exists lzy_new_samples_tmpp_;
create table lzy_new_samples_tmpp_ as 
select a.row_id,a.pred_shop,b.shop_id from 
(select cast(row_id as bigint) as row_id,shop_id as pred_shop from ly_m_18_31_train_hx_rs_12) as a 
left outer join 
(select row_id,shop_id from lzy_new_user_shop_behavior) as b
on a.row_id=b.row_id;

drop table if exists lzy_new_samples_add_offline;
create table lzy_new_samples_add_offline as 
select row_id,pred_shop as shop_id,cast(shop_id=pred_shop as bigint) as label from
(select row_id,shop_id,pred_shop from lzy_new_userbeen_shop_before_offline 
union select row_id,shop_id,pred_shop from (select *, row_number() over(partition by row_id order by distance_diff) as rank from lzy_new_user_distance_to_shop_before_offline) as a 
where a.rank<=10
union select row_id,shop_id,pred_shop from lzy_new_wifi_appear_shop_before_offline
union select row_id,shop_id,pred_shop from lzy_new_samples_tmpp_
union select row_id,shop_id,shop_id as pred_shop from lzy_new_tongji_offline
) as t;
drop table if exists lzy_new_samples_tmpp_;

drop table if exists lzy_new_samples_add_online;
create table lzy_new_samples_add_online as 
select row_id,pred_shop as shop_id from
(select row_id,pred_shop from lzy_new_userbeen_shop_before_online 
union select row_id,pred_shop from (select *, row_number() over(partition by row_id order by distance_diff) as rank from lzy_new_user_distance_to_shop_before_online) as a 
where a.rank<=10
union select row_id,pred_shop from lzy_new_wifi_appear_shop_before_online
union select cast(row_id as bigint) as row_id,shop_id as pred_shop from ly_m_91_14_test_hx_rs_12) as t;

-----------------------------------------------------------------------------------------
--tongji加上topk_bssid,topk_rssi字段
--top1
drop table if exists lzy_new_tongji_offline_;
create table lzy_new_tongji_offline_ as 
select a.*,b.row_top1_bssid,b.row_top1_rssi from
(select * from lzy_new_tongji_offline) as a left outer join 
(select row_id,bssid as row_top1_bssid,rssi as row_top1_rssi,rssi_rank from lzy_new_tongji_wifi_offline where top_rank=1) as b
on a.row_id=b.row_id;
drop table if exists lzy_new_tongji_online_;
create table lzy_new_tongji_online_ as 
select a.*,b.row_top1_bssid,b.row_top1_rssi from
(select * from lzy_new_tongji_online) as a left outer join 
(select row_id,bssid as row_top1_bssid,rssi as row_top1_rssi from lzy_new_tongji_wifi_online where top_rank=1) as b
on a.row_id=b.row_id;
--top2
drop table if exists lzy_new_tongji_offline;
create table lzy_new_tongji_offline as 
select a.*,b.row_top2_bssid,b.row_top2_rssi from
(select * from lzy_new_tongji_offline_) as a left outer join 
(select row_id,bssid as row_top2_bssid,rssi as row_top2_rssi from lzy_new_tongji_wifi_offline where top_rank=2) as b
on a.row_id=b.row_id;
drop table if exists lzy_new_tongji_online;
create table lzy_new_tongji_online as 
select a.*,b.row_top2_bssid,b.row_top2_rssi from
(select * from lzy_new_tongji_online_) as a left outer join 
(select row_id,bssid as row_top2_bssid,rssi as row_top2_rssi from lzy_new_tongji_wifi_online where top_rank=2) as b
on a.row_id=b.row_id;
--top3
drop table if exists lzy_new_tongji_offline_;
create table lzy_new_tongji_offline_ as 
select a.*,b.row_top3_bssid,b.row_top3_rssi from
(select * from lzy_new_tongji_offline) as a left outer join 
(select row_id,bssid as row_top3_bssid,rssi as row_top3_rssi from lzy_new_tongji_wifi_offline where top_rank=3) as b
on a.row_id=b.row_id;
drop table if exists lzy_new_tongji_online_;
create table lzy_new_tongji_online_ as 
select a.*,b.row_top3_bssid,b.row_top3_rssi from
(select * from lzy_new_tongji_online) as a left outer join 
(select row_id,bssid as row_top3_bssid,rssi as row_top3_rssi from lzy_new_tongji_wifi_online where top_rank=3) as b
on a.row_id=b.row_id;
--top4
drop table if exists lzy_new_tongji_offline;
create table lzy_new_tongji_offline as 
select a.*,b.row_top4_bssid,b.row_top4_rssi from
(select * from lzy_new_tongji_offline_) as a left outer join 
(select row_id,bssid as row_top4_bssid,rssi as row_top4_rssi from lzy_new_tongji_wifi_offline where top_rank=4) as b
on a.row_id=b.row_id;
drop table if exists lzy_new_tongji_online;
create table lzy_new_tongji_online as 
select a.*,b.row_top4_bssid,b.row_top4_rssi from
(select * from lzy_new_tongji_online_) as a left outer join 
(select row_id,bssid as row_top4_bssid,rssi as row_top4_rssi from lzy_new_tongji_wifi_online where top_rank=4) as b
on a.row_id=b.row_id;
--top5
drop table if exists lzy_new_tongji_offline_;
create table lzy_new_tongji_offline_ as 
select a.*,b.row_top5_bssid,b.row_top5_rssi from
(select * from lzy_new_tongji_offline) as a left outer join 
(select row_id,bssid as row_top5_bssid,rssi as row_top5_rssi from lzy_new_tongji_wifi_offline where top_rank=5) as b
on a.row_id=b.row_id;
drop table if exists lzy_new_tongji_online_;
create table lzy_new_tongji_online_ as 
select a.*,b.row_top5_bssid,b.row_top5_rssi from
(select * from lzy_new_tongji_online) as a left outer join 
(select row_id,bssid as row_top5_bssid,rssi as row_top5_rssi from lzy_new_tongji_wifi_online where top_rank=5) as b
on a.row_id=b.row_id;
--top6
drop table if exists lzy_new_tongji_offline;
create table lzy_new_tongji_offline as 
select a.*,b.row_top6_bssid,b.row_top6_rssi from
(select * from lzy_new_tongji_offline_) as a left outer join 
(select row_id,bssid as row_top6_bssid,rssi as row_top6_rssi from lzy_new_tongji_wifi_offline where top_rank=6) as b
on a.row_id=b.row_id;
drop table if exists lzy_new_tongji_online;
create table lzy_new_tongji_online as 
select a.*,b.row_top6_bssid,b.row_top6_rssi from
(select * from lzy_new_tongji_online_) as a left outer join 
(select row_id,bssid as row_top6_bssid,rssi as row_top6_rssi from lzy_new_tongji_wifi_online where top_rank=6) as b
on a.row_id=b.row_id;
--top7
drop table if exists lzy_new_tongji_offline_;
create table lzy_new_tongji_offline_ as 
select a.*,b.row_top7_bssid,b.row_top7_rssi from
(select * from lzy_new_tongji_offline) as a left outer join 
(select row_id,bssid as row_top7_bssid,rssi as row_top7_rssi from lzy_new_tongji_wifi_offline where top_rank=7) as b
on a.row_id=b.row_id;
drop table if exists lzy_new_tongji_online_;
create table lzy_new_tongji_online_ as 
select a.*,b.row_top7_bssid,b.row_top7_rssi from
(select * from lzy_new_tongji_online) as a left outer join 
(select row_id,bssid as row_top7_bssid,rssi as row_top7_rssi from lzy_new_tongji_wifi_online where top_rank=7) as b
on a.row_id=b.row_id;
--top8
drop table if exists lzy_new_tongji_offline;
create table lzy_new_tongji_offline as 
select a.*,b.row_top8_bssid,b.row_top8_rssi from
(select * from lzy_new_tongji_offline_) as a left outer join 
(select row_id,bssid as row_top8_bssid,rssi as row_top8_rssi from lzy_new_tongji_wifi_offline where top_rank=8) as b
on a.row_id=b.row_id;
drop table if exists lzy_new_tongji_online;
create table lzy_new_tongji_online as 
select a.*,b.row_top8_bssid,b.row_top8_rssi from
(select * from lzy_new_tongji_online_) as a left outer join 
(select row_id,bssid as row_top8_bssid,rssi as row_top8_rssi from lzy_new_tongji_wifi_online where top_rank=8) as b
on a.row_id=b.row_id;
--top9
drop table if exists lzy_new_tongji_offline_;
create table lzy_new_tongji_offline_ as 
select a.*,b.row_top9_bssid,b.row_top9_rssi from
(select * from lzy_new_tongji_offline) as a left outer join 
(select row_id,bssid as row_top9_bssid,rssi as row_top9_rssi from lzy_new_tongji_wifi_offline where top_rank=9) as b
on a.row_id=b.row_id;
drop table if exists lzy_new_tongji_online_;
create table lzy_new_tongji_online_ as 
select a.*,b.row_top9_bssid,b.row_top9_rssi from
(select * from lzy_new_tongji_online) as a left outer join 
(select row_id,bssid as row_top9_bssid,rssi as row_top9_rssi from lzy_new_tongji_wifi_online where top_rank=9) as b
on a.row_id=b.row_id;
--top10
drop table if exists lzy_new_tongji_offline;
create table lzy_new_tongji_offline as 
select a.*,b.row_top10_bssid,b.row_top10_rssi from
(select * from lzy_new_tongji_offline_) as a left outer join 
(select row_id,bssid as row_top10_bssid,rssi as row_top10_rssi from lzy_new_tongji_wifi_offline where top_rank=10) as b
on a.row_id=b.row_id;
drop table if exists lzy_new_tongji_online;
create table lzy_new_tongji_online as 
select a.*,b.row_top10_bssid,b.row_top10_rssi from
(select * from lzy_new_tongji_online_) as a left outer join 
(select row_id,bssid as row_top10_bssid,rssi as row_top10_rssi from lzy_new_tongji_wifi_online where top_rank=10) as b
on a.row_id=b.row_id;
drop table if exists lzy_new_tongji_offline_;
drop table if exists lzy_new_tongji_online_;

--加上基础字段和topkwifi字段
--train
drop table if exists lzy_new_offline_data;
create table lzy_new_offline_data as
select a.label,a.shop_id,c.*,b.category_id,b.price,b.mall_id from 
lzy_new_samples_add_offline as a 
left outer join 
(select shop_id,category_id,price,mall_id from lzy_ant_tianchi_ccf_sl_shop_info) as b 
on a.shop_id=b.shop_id
left outer join
(select row_id,user_id,longitude,latitude,month,day,hour,minite,
 row_top1_bssid,row_top1_rssi,row_top2_bssid,row_top2_rssi,row_top3_bssid,row_top3_rssi,row_top4_bssid,row_top4_rssi,row_top5_bssid,row_top5_rssi,
 row_top6_bssid,row_top6_rssi,row_top7_bssid,row_top7_rssi,row_top8_bssid,row_top8_rssi,row_top9_bssid,row_top9_rssi,row_top10_bssid,row_top10_rssi
 from lzy_new_tongji_offline) as c
on a.row_id=c.row_id;
--test
drop table if exists lzy_new_tongji_online_data;
create table lzy_new_tongji_online_data as
select a.shop_id,c.*,b.category_id,b.price,b.mall_id from 
lzy_new_samples_add_online as a 
left outer join 
(select shop_id,category_id,price,mall_id from lzy_ant_tianchi_ccf_sl_shop_info) as b 
on a.shop_id=b.shop_id
left outer join
(select row_id,user_id,longitude,latitude,month,day,hour,minite,
 row_top1_bssid,row_top1_rssi,row_top2_bssid,row_top2_rssi,row_top3_bssid,row_top3_rssi,row_top4_bssid,row_top4_rssi,row_top5_bssid,row_top5_rssi,
 row_top6_bssid,row_top6_rssi,row_top7_bssid,row_top7_rssi,row_top8_bssid,row_top8_rssi,row_top9_bssid,row_top9_rssi,row_top10_bssid,row_top10_rssi
 from lzy_new_tongji_online) as c
on a.row_id=c.row_id;
------------------------------------------------------------------------建特征表

--建立指纹库
create table if not exists lzy_new_fingerbase_offlinewin as
select lzy_get_fingerbase(mall_id,shop_id,wifi_list) as (mall_id,shop_id,key_values) 
from (select mall_id,shop_id,wm_concat("#", wifi_infos) as wifi_list from lzy_new_tongji_offlinewin group by mall_id,shop_id) as t;

create table if not exists lzy_new_fingerbase_onlinewin as 
select lzy_get_fingerbase(mall_id,shop_id,wifi_list) as (mall_id,shop_id,key_values) 
from (select mall_id,shop_id,wm_concat("#", wifi_infos) as wifi_list from lzy_new_tongji_onlinewin group by mall_id,shop_id) as t;
--指纹得分特征
--train
drop table if exists lzy_new_feat_fingerscore_offline_;
create table lzy_new_feat_fingerscore_offline_ as
select c.*,d.key_values from
( 
 select a.*,b.wifi_infos from (select row_id,shop_id from lzy_new_samples_add_offline ) as a left outer join
 (select row_id,wifi_infos from lzy_new_tongji_offline) as b on a.row_id=b.row_id
 ) as c
left outer join (select shop_id,key_values from lzy_new_fingerbase_offlinewin) as d 
on c.shop_id = d.shop_id;

drop table if exists lzy_new_feat_fingerscore_offline;
create table lzy_new_feat_fingerscore_offline as
select lzy_get_fingerscore(row_id,shop_id,wifi_infos,key_values) as (row_id,shop_id,finger_score)
from lzy_new_feat_fingerscore_offline_;
drop table if exists lzy_new_feat_fingerscore_offline_;
--test
drop table if exists lzy_new_feat_fingerscore_online_;
create table lzy_new_feat_fingerscore_online_ as
select c.*,d.key_values from
( 
 select a.*,b.wifi_infos from (select row_id,shop_id from lzy_new_samples_add_online ) as a left outer join
 (select row_id,wifi_infos from lzy_new_tongji_online) as b on a.row_id=b.row_id
 ) as c
left outer join (select shop_id,key_values from lzy_new_fingerbase_onlinewin) as d 
on c.shop_id = d.shop_id;

drop table if exists lzy_new_feat_fingerscore_online;
create table lzy_new_feat_fingerscore_online as
select lzy_get_fingerscore(row_id,shop_id,wifi_infos,key_values) as (row_id,shop_id,finger_score)
from lzy_new_feat_fingerscore_online_;
drop table if exists lzy_new_feat_fingerscore_online_;

--在筛选数据中(筛选条件是只保留记录top3wifi)，bssid在shop里出现了多少次,及rate_inshop,rate_inbssid,
--7.1~8.17 
drop table if exists lzy_new_shop_wifi_selected_offlinewin;
create table lzy_new_shop_wifi_selected_offlinewin as 
select mall_id,bssid,shop_id,count(1) as bssid_count_inshop from lzy_new_tongji_wifi_offlinewin where rssi_rank<=3 and row_id in (select row_id from lzy_new_notnull_offline) group by mall_id,bssid,shop_id;

drop table if exists lzy_new_shop_wifi_selected_offlinewin_;
create table lzy_new_shop_wifi_selected_offlinewin_ as 
select a.*,b.shop_wificount_sum from lzy_new_shop_wifi_selected_offlinewin as a
left outer join (select mall_id,shop_id,sum(bssid_count_inshop) as shop_wificount_sum from lzy_new_shop_wifi_selected_offlinewin group by mall_id,shop_id) as b
on a.mall_id=b.mall_id and a.shop_id=b.shop_id;

drop table if exists lzy_new_shop_wifi_selected_offlinewin;
create table lzy_new_shop_wifi_selected_offlinewin as 
select a.*,b.bssid_count_sum,bssid_count_inshop/shop_wificount_sum as bssid_count_rate_inshop,bssid_count_inshop/bssid_count_sum as bssid_count_rate_inbssid from lzy_new_shop_wifi_selected_offlinewin_ as a
left outer join (select mall_id,bssid,sum(bssid_count_inshop) as bssid_count_sum from lzy_new_shop_wifi_selected_offlinewin_ group by mall_id,bssid) as b
on a.mall_id=b.mall_id and a.bssid=b.bssid;
drop table if exists lzy_new_shop_wifi_selected_offlinewin_;
--7.15~8.31
drop table if exists lzy_new_shop_wifi_selected_onlinewin;
create table lzy_new_shop_wifi_selected_onlinewin as 
select mall_id,bssid,shop_id,count(1) as bssid_count_inshop from lzy_new_tongji_wifi_onlinewin where rssi_rank<=3 and row_id in (select row_id from lzy_new_notnull_online) group by mall_id,bssid,shop_id;

drop table if exists lzy_new_shop_wifi_selected_onlinewin_;
create table lzy_new_shop_wifi_selected_onlinewin_ as 
select a.*,b.shop_wificount_sum from lzy_new_shop_wifi_selected_onlinewin as a
left outer join (select mall_id,shop_id,sum(bssid_count_inshop) as shop_wificount_sum from lzy_new_shop_wifi_selected_onlinewin group by mall_id,shop_id) as b
on a.mall_id=b.mall_id and a.shop_id=b.shop_id;

drop table if exists lzy_new_shop_wifi_selected_onlinewin;
create table lzy_new_shop_wifi_selected_onlinewin as 
select a.*,b.bssid_count_sum,bssid_count_inshop/shop_wificount_sum as bssid_count_rate_inshop,bssid_count_inshop/bssid_count_sum as bssid_count_rate_inbssid from lzy_new_shop_wifi_selected_onlinewin_ as a
left outer join (select mall_id,bssid,sum(bssid_count_inshop) as bssid_count_sum from lzy_new_shop_wifi_selected_onlinewin_ group by mall_id,bssid) as b
on a.mall_id=b.mall_id and a.bssid=b.bssid;
drop table if exists lzy_new_shop_wifi_selected_onlinewin_;


--bssid在shop里rssi_rank出现了多少次,及rate_inshop,rate_inbssid,
--7.1~8.17 
drop table if exists lzy_new_feat_top_also_top_offline;
create table lzy_new_feat_top_also_top_offline as 
select t.mall_id,t.bssid,t.shop_id,t.top_rank,count(1) as top_also_top_count_inshop from
(
  select a.mall_id,a.bssid,a.shop_id,a.top_rank from 
  (
    select distinct m.shop_id,n.bssid,n.top_rank,n.mall_id from 
    (select * from lzy_new_samples_add_offline) as m left outer join (select row_id,bssid,top_rank,mall_id from lzy_new_tongji_wifi_offline where row_id in (select row_id from lzy_new_notnull_offline)) as n on m.row_id=n.row_id
  ) as a 
  inner join 
  (select bssid,shop_id,top_rank from lzy_new_tongji_wifi_offlinewin where row_id in (select row_id from lzy_new_notnull_offline)) as b
  on a.bssid=b.bssid and a.shop_id=b.shop_id and a.top_rank=b.top_rank
) as t
group by t.mall_id,t.bssid,t.shop_id,t.top_rank;

drop table if exists lzy_new_feat_top_also_top_offline_;
create table lzy_new_feat_top_also_top_offline_ as 
select a.*,b.shop_wificount_sum from lzy_new_feat_top_also_top_offline as a
left outer join (
  select shop_id,count(1) as shop_wificount_sum from lzy_new_tongji_wifi_offlinewin where row_id in (select row_id from lzy_new_notnull_offline) group by shop_id 
) as b
on a.shop_id=b.shop_id;

drop table if exists lzy_new_feat_top_also_top_offline;
create table lzy_new_feat_top_also_top_offline as 
select a.*,bssid_count_sum,top_also_top_count_inshop/shop_wificount_sum as bssid_top_also_top_rate_inshop,top_also_top_count_inshop/bssid_count_sum as bssid_top_also_top_rate_inbssid from lzy_new_feat_top_also_top_offline_ as a
left outer join (
  select mall_id,bssid,count(1) as bssid_count_sum from lzy_new_tongji_wifi_offlinewin where row_id in (select row_id from lzy_new_notnull_offline) group by mall_id,bssid 
) as b
on a.mall_id=b.mall_id and a.bssid=b.bssid;
drop table if exists lzy_new_feat_top_also_top_offline_;
--7.15~8.31
drop table if exists lzy_new_feat_top_also_top_online;
create table lzy_new_feat_top_also_top_online as 
select t.mall_id,t.bssid,t.shop_id,t.top_rank,count(1) as top_also_top_count_inshop from
(
  select a.mall_id,a.bssid,a.shop_id,a.top_rank from 
  (
    select distinct m.shop_id,n.bssid,n.top_rank,n.mall_id from 
    (select * from lzy_new_samples_add_online) as m left outer join (select row_id,bssid,top_rank,mall_id from lzy_new_tongji_wifi_online where row_id in (select row_id from lzy_new_notnull_online)) as n on m.row_id=n.row_id
  ) as a 
  inner join 
  (select bssid,shop_id,top_rank from lzy_new_tongji_wifi_onlinewin where row_id in (select row_id from lzy_new_notnull_online)) as b
  on a.bssid=b.bssid and a.shop_id=b.shop_id and a.top_rank=b.top_rank
) as t
group by t.mall_id,t.bssid,t.shop_id,t.top_rank;

drop table if exists lzy_new_feat_top_also_top_online_;
create table lzy_new_feat_top_also_top_online_ as 
select a.*,b.shop_wificount_sum from lzy_new_feat_top_also_top_online as a
left outer join (
  select shop_id,count(1) as shop_wificount_sum from lzy_new_tongji_wifi_onlinewin where row_id in (select row_id from lzy_new_notnull_online) group by shop_id 
) as b
on a.shop_id=b.shop_id;

drop table if exists lzy_new_feat_top_also_top_online;
create table lzy_new_feat_top_also_top_online as 
select a.*,bssid_count_sum,top_also_top_count_inshop/shop_wificount_sum as bssid_top_also_top_rate_inshop,top_also_top_count_inshop/bssid_count_sum as bssid_top_also_top_rate_inbssid from lzy_new_feat_top_also_top_online_ as a
left outer join (
  select mall_id,bssid,count(1) as bssid_count_sum from lzy_new_tongji_wifi_onlinewin where row_id in (select row_id from lzy_new_notnull_online) group by mall_id,bssid 
) as b
on a.mall_id=b.mall_id and a.bssid=b.bssid;
drop table if exists lzy_new_feat_top_also_top_online_;

--bssid在shop里rssi偏差小于D出现了多少次,及rate_inshop,rate_inbssid
--7.1~8.17 
drop table if exists lzy_new_feat_rssidiff_less8_offline;
create table lzy_new_feat_rssidiff_less8_offline as 
select t.mall_id,t.bssid,t.shop_id,t.rssi,count(1) as bssid_rssidiff_less8_count from
(
  select a.mall_id,a.bssid,a.shop_id,a.rssi from 
  (
    select distinct m.shop_id,n.bssid,n.rssi,n.mall_id from 
    (select * from lzy_new_samples_add_offline) as m left outer join (select row_id,bssid,rssi,mall_id from lzy_new_tongji_wifi_offline) as n on m.row_id=n.row_id
  ) as a 
  inner join 
  (select bssid,shop_id,rssi as his_rssi from lzy_new_tongji_wifi_offlinewin) as b
  on a.bssid=b.bssid and a.shop_id=b.shop_id 
  where abs(a.rssi-b.his_rssi)<=8
) as t
group by t.mall_id,t.bssid,t.shop_id,t.rssi;

drop table if exists lzy_new_feat_rssidiff_less8_offline_;
create table lzy_new_feat_rssidiff_less8_offline_ as 
select a.*,b.shop_wificount_sum from lzy_new_feat_rssidiff_less8_offline as a
left outer join (
  select shop_id,count(1) as shop_wificount_sum from lzy_new_tongji_wifi_offlinewin where row_id in (select row_id from lzy_new_notnull_offline) group by shop_id 
) as b
on a.shop_id=b.shop_id;

drop table if exists lzy_new_feat_rssidiff_less8_offline;
create table lzy_new_feat_rssidiff_less8_offline as 
select a.*,bssid_count_sum,bssid_rssidiff_less8_count/shop_wificount_sum as bssid_rssidiff_less8_rate_inshop,bssid_rssidiff_less8_count/bssid_count_sum as bssid_rssidiff_less8_rate_inbssid from lzy_new_feat_rssidiff_less8_offline_ as a
left outer join (
  select mall_id,bssid,count(1) as bssid_count_sum from lzy_new_tongji_wifi_offlinewin where row_id in (select row_id from lzy_new_notnull_offline) group by mall_id,bssid 
) as b
on a.mall_id=b.mall_id and a.bssid=b.bssid;
drop table if exists lzy_new_feat_rssidiff_less8_offline_;
--7.15~8.31
drop table if exists lzy_new_feat_rssidiff_less8_online;
create table lzy_new_feat_rssidiff_less8_online as 
select t.mall_id,t.bssid,t.shop_id,t.rssi,count(1) as bssid_rssidiff_less8_count from
(
  select a.mall_id,a.bssid,a.shop_id,a.rssi from 
  (
    select distinct m.shop_id,n.bssid,n.rssi,n.mall_id from 
    (select * from lzy_new_samples_add_online) as m left outer join (select row_id,bssid,rssi,mall_id from lzy_new_tongji_wifi_online) as n on m.row_id=n.row_id
  ) as a 
  inner join 
  (select bssid,shop_id,rssi as his_rssi from lzy_new_tongji_wifi_onlinewin) as b
  on a.bssid=b.bssid and a.shop_id=b.shop_id 
  where abs(a.rssi-b.his_rssi)<=8
) as t
group by t.mall_id,t.bssid,t.shop_id,t.rssi;

drop table if exists lzy_new_feat_rssidiff_less8_online_;
create table lzy_new_feat_rssidiff_less8_online_ as 
select a.*,b.shop_wificount_sum from lzy_new_feat_rssidiff_less8_online as a
left outer join (
  select shop_id,count(1) as shop_wificount_sum from lzy_new_tongji_wifi_onlinewin where row_id in (select row_id from lzy_new_notnull_online) group by shop_id 
) as b
on a.shop_id=b.shop_id;

drop table if exists lzy_new_feat_rssidiff_less8_online;
create table lzy_new_feat_rssidiff_less8_online as 
select a.*,bssid_count_sum,bssid_rssidiff_less8_count/shop_wificount_sum as bssid_rssidiff_less8_rate_inshop,bssid_rssidiff_less8_count/bssid_count_sum as bssid_rssidiff_less8_rate_inbssid from lzy_new_feat_rssidiff_less8_online_ as a
left outer join (
  select mall_id,bssid,count(1) as bssid_count_sum from lzy_new_tongji_wifi_onlinewin where row_id in (select row_id from lzy_new_notnull_online) group by mall_id,bssid 
) as b
on a.mall_id=b.mall_id and a.bssid=b.bssid;
drop table if exists lzy_new_feat_rssidiff_less8_online_;


------对rate_inbssid做平滑或筛选
--lzy_new_shop_wifi
drop table if exists lzy_new_shop_wifi_offlinewin_;
create table lzy_new_shop_wifi_offlinewin_ as 
select mall_id,bssid,shop_id,bssid_count_inshop,shop_wificount_sum,bssid_count_sum,bssid_count_rate_inshop,bssid_count_rate_inbssid, bssid_count_rate_inbssid as bssid_count_smoothrate_inbssid,bssid_median_rssi_inshop,bssid_max_rssi_inshop 
from lzy_new_shop_wifi_offlinewin where bssid_count_sum>=5
union all 
select mall_id,bssid,shop_id,bssid_count_inshop,shop_wificount_sum,bssid_count_sum,bssid_count_rate_inshop,bssid_count_rate_inbssid, 0 as bssid_count_smoothrate_inbssid,bssid_median_rssi_inshop,bssid_max_rssi_inshop 
from lzy_new_shop_wifi_offlinewin where bssid_count_sum<5;
drop table if exists lzy_new_shop_wifi_offlinewin;
create table lzy_new_shop_wifi_offlinewin as select * from lzy_new_shop_wifi_offlinewin_;
drop table if exists lzy_new_shop_wifi_offlinewin_;

drop table if exists lzy_new_shop_wifi_onlinewin_;
create table lzy_new_shop_wifi_onlinewin_ as 
select mall_id,bssid,shop_id,bssid_count_inshop,shop_wificount_sum,bssid_count_sum,bssid_count_rate_inshop,bssid_count_rate_inbssid, bssid_count_rate_inbssid as bssid_count_smoothrate_inbssid,bssid_median_rssi_inshop,bssid_max_rssi_inshop 
from lzy_new_shop_wifi_onlinewin where bssid_count_sum>=5
union all 
select mall_id,bssid,shop_id,bssid_count_inshop,shop_wificount_sum,bssid_count_sum,bssid_count_rate_inshop,bssid_count_rate_inbssid, 0 as bssid_count_smoothrate_inbssid,bssid_median_rssi_inshop,bssid_max_rssi_inshop 
from lzy_new_shop_wifi_onlinewin where bssid_count_sum<5;
drop table if exists lzy_new_shop_wifi_onlinewin;
create table lzy_new_shop_wifi_onlinewin as select * from lzy_new_shop_wifi_onlinewin_;
drop table if exists lzy_new_shop_wifi_onlinewin_;
--lzy_new_shop_wifi_selected
drop table if exists lzy_new_shop_wifi_selected_offlinewin_;
create table lzy_new_shop_wifi_selected_offlinewin_ as 
select mall_id,bssid,shop_id,bssid_count_inshop,shop_wificount_sum,bssid_count_sum,bssid_count_rate_inshop,bssid_count_rate_inbssid, bssid_count_rate_inbssid as bssid_count_smoothrate_inbssid
from lzy_new_shop_wifi_selected_offlinewin where bssid_count_sum>=3
union all 
select mall_id,bssid,shop_id,bssid_count_inshop,shop_wificount_sum,bssid_count_sum,bssid_count_rate_inshop,bssid_count_rate_inbssid, 0 as bssid_count_smoothrate_inbssid
from lzy_new_shop_wifi_selected_offlinewin where bssid_count_sum<3;
drop table if exists lzy_new_shop_wifi_selected_offlinewin;
create table lzy_new_shop_wifi_selected_offlinewin as select * from lzy_new_shop_wifi_selected_offlinewin_;
drop table if exists lzy_new_shop_wifi_selected_offlinewin_;

drop table if exists lzy_new_shop_wifi_selected_onlinewin_;
create table lzy_new_shop_wifi_selected_onlinewin_ as 
select mall_id,bssid,shop_id,bssid_count_inshop,shop_wificount_sum,bssid_count_sum,bssid_count_rate_inshop,bssid_count_rate_inbssid, bssid_count_rate_inbssid as bssid_count_smoothrate_inbssid
from lzy_new_shop_wifi_selected_onlinewin where bssid_count_sum>=3
union all 
select mall_id,bssid,shop_id,bssid_count_inshop,shop_wificount_sum,bssid_count_sum,bssid_count_rate_inshop,bssid_count_rate_inbssid, 0 as bssid_count_smoothrate_inbssid
from lzy_new_shop_wifi_selected_onlinewin where bssid_count_sum<3;
drop table if exists lzy_new_shop_wifi_selected_onlinewin;
create table lzy_new_shop_wifi_selected_onlinewin as select * from lzy_new_shop_wifi_selected_onlinewin_;
drop table if exists lzy_new_shop_wifi_selected_onlinewin_;
--lzy_new_feat_top_also_top
drop table if exists lzy_new_feat_top_also_top_offline_;
create table lzy_new_feat_top_also_top_offline_ as 
select mall_id,bssid,shop_id,top_rank,top_also_top_count_inshop,shop_wificount_sum,bssid_count_sum,bssid_top_also_top_rate_inshop,bssid_top_also_top_rate_inbssid,bssid_top_also_top_rate_inbssid as bssid_top_also_top_smoothrate_inbssid
from lzy_new_feat_top_also_top_offline where bssid_count_sum>=3
union all 
select mall_id,bssid,shop_id,top_rank,top_also_top_count_inshop,shop_wificount_sum,bssid_count_sum,bssid_top_also_top_rate_inshop,bssid_top_also_top_rate_inbssid,0 as bssid_top_also_top_smoothrate_inbssid
from lzy_new_feat_top_also_top_offline where bssid_count_sum<3;
drop table if exists lzy_new_feat_top_also_top_offline;
create table lzy_new_feat_top_also_top_offline as select * from lzy_new_feat_top_also_top_offline_;
drop table if exists lzy_new_feat_top_also_top_offline_;

drop table if exists lzy_new_feat_top_also_top_online_;
create table lzy_new_feat_top_also_top_online_ as 
select mall_id,bssid,shop_id,top_rank,top_also_top_count_inshop,shop_wificount_sum,bssid_count_sum,bssid_top_also_top_rate_inshop,bssid_top_also_top_rate_inbssid,bssid_top_also_top_rate_inbssid as bssid_top_also_top_smoothrate_inbssid
from lzy_new_feat_top_also_top_online where bssid_count_sum>=3
union all 
select mall_id,bssid,shop_id,top_rank,top_also_top_count_inshop,shop_wificount_sum,bssid_count_sum,bssid_top_also_top_rate_inshop,bssid_top_also_top_rate_inbssid,0 as bssid_top_also_top_smoothrate_inbssid
from lzy_new_feat_top_also_top_online where bssid_count_sum<3;
drop table if exists lzy_new_feat_top_also_top_online;
create table lzy_new_feat_top_also_top_online as select * from lzy_new_feat_top_also_top_online_;
drop table if exists lzy_new_feat_top_also_top_online_;
--lzy_new_feat_rssidiff_less8
drop table if exists lzy_new_feat_rssidiff_less8_offline_;
create table lzy_new_feat_rssidiff_less8_offline_ as 
select mall_id,bssid,shop_id,rssi,bssid_rssidiff_less8_count,shop_wificount_sum,bssid_count_sum,bssid_rssidiff_less8_rate_inshop,bssid_rssidiff_less8_rate_inbssid,bssid_rssidiff_less8_rate_inbssid as bssid_rssidiff_less8_smoothrate_inbssid
from lzy_new_feat_rssidiff_less8_offline where bssid_count_sum>=3
union all 
select mall_id,bssid,shop_id,rssi,bssid_rssidiff_less8_count,shop_wificount_sum,bssid_count_sum,bssid_rssidiff_less8_rate_inshop,bssid_rssidiff_less8_rate_inbssid,0 as bssid_rssidiff_less8_smoothrate_inbssid
from lzy_new_feat_rssidiff_less8_offline where bssid_count_sum<3;
drop table if exists lzy_new_feat_rssidiff_less8_offline;
create table lzy_new_feat_rssidiff_less8_offline as select * from lzy_new_feat_rssidiff_less8_offline_;
drop table if exists lzy_new_feat_rssidiff_less8_offline_;

drop table if exists lzy_new_feat_rssidiff_less8_online_;
create table lzy_new_feat_rssidiff_less8_online_ as 
select mall_id,bssid,shop_id,rssi,bssid_rssidiff_less8_count,shop_wificount_sum,bssid_count_sum,bssid_rssidiff_less8_rate_inshop,bssid_rssidiff_less8_rate_inbssid,bssid_rssidiff_less8_rate_inbssid as bssid_rssidiff_less8_smoothrate_inbssid
from lzy_new_feat_rssidiff_less8_online where bssid_count_sum>=3
union all 
select mall_id,bssid,shop_id,rssi,bssid_rssidiff_less8_count,shop_wificount_sum,bssid_count_sum,bssid_rssidiff_less8_rate_inshop,bssid_rssidiff_less8_rate_inbssid,0 as bssid_rssidiff_less8_smoothrate_inbssid
from lzy_new_feat_rssidiff_less8_online where bssid_count_sum<3;
drop table if exists lzy_new_feat_rssidiff_less8_online;
create table lzy_new_feat_rssidiff_less8_online as select * from lzy_new_feat_rssidiff_less8_online_;
drop table if exists lzy_new_feat_rssidiff_less8_online_;

--wifi在该商场几个店铺出现过,及在商场的占比
drop table if exists lzy_new_feat_wifi_unique_shop_offline;
create table lzy_new_feat_wifi_unique_shop_offline as 
select a.mall_id,bssid,bssid_unique_shop_count,bssid_unique_shop_count/mall_shop_count as bssid_unique_shop_rate from
(select mall_id,bssid,count(1) as bssid_unique_shop_count from
  (select distinct mall_id,bssid,shop_id from lzy_new_tongji_wifi_offlinewin) as t 
 group by mall_id,bssid
) as a
left outer join 
(select mall_id,count(1) as mall_shop_count from lzy_ant_tianchi_ccf_sl_shop_info group by mall_id) as b
on a.mall_id=b.mall_id;

drop table if exists lzy_new_feat_wifi_unique_shop_online;
create table lzy_new_feat_wifi_unique_shop_online as 
select a.mall_id,bssid,bssid_unique_shop_count,bssid_unique_shop_count/mall_shop_count as bssid_unique_shop_rate from
(select mall_id,bssid,count(1) as bssid_unique_shop_count from
  (select distinct mall_id,bssid,shop_id from lzy_new_tongji_wifi_onlinewin) as t 
 group by mall_id,bssid
) as a
left outer join 
(select mall_id,count(1) as mall_shop_count from lzy_ant_tianchi_ccf_sl_shop_info group by mall_id) as b
on a.mall_id=b.mall_id;


--wifi_loss
--train
drop table if exists lzy_new_feat_wifi_loss_offline_;
create table lzy_new_feat_wifi_loss_offline_ as 
select t.*,c.bssid_median_rssi_inshop,abs(t.rssi-c.bssid_median_rssi_inshop) as wifi_loss from 
(select a.*,b.bssid,b.rssi from (select * from lzy_new_samples_add_offline) as a left outer join (select row_id,bssid,rssi from lzy_new_tongji_wifi_offline) as b on a.row_id=b.row_id) as t
inner join 
(select shop_id,bssid,bssid_median_rssi_inshop from lzy_new_shop_wifi_offlinewin) as c
on t.shop_id=c.shop_id and t.bssid=c.bssid;

drop table if exists lzy_new_feat_wifi_loss_offline;
create table lzy_new_feat_wifi_loss_offline as 
select row_id,shop_id,avg(wifi_loss) as wifi_loss from lzy_new_feat_wifi_loss_offline_ where wifi_loss is not null group by row_id,shop_id;
drop table if exists lzy_new_feat_wifi_loss_offline_;
--test
drop table if exists lzy_new_feat_wifi_loss_online_;
create table lzy_new_feat_wifi_loss_online_ as 
select t.*,c.bssid_median_rssi_inshop,abs(t.rssi-c.bssid_median_rssi_inshop) as wifi_loss from 
(select a.*,b.bssid,b.rssi from (select * from lzy_new_samples_add_online) as a left outer join (select row_id,bssid,rssi from lzy_new_tongji_wifi_online) as b on a.row_id=b.row_id) as t
inner join 
(select shop_id,bssid,bssid_median_rssi_inshop from lzy_new_shop_wifi_onlinewin) as c
on t.shop_id=c.shop_id and t.bssid=c.bssid;

drop table if exists lzy_new_feat_wifi_loss_online;
create table lzy_new_feat_wifi_loss_online as 
select row_id,shop_id,avg(wifi_loss) as wifi_loss from lzy_new_feat_wifi_loss_online_ where wifi_loss is not null group by row_id,shop_id;
drop table if exists lzy_new_feat_wifi_loss_online_;


--row所有wifi有几个在shop记录里
--train
drop table if exists lzy_new_feat_num_wifi_inshop_offline;
create table lzy_new_feat_num_wifi_inshop_offline as 
select t.row_id,t.shop_id,count(1) as num_rowwifi_in_shopwifi from 
  (select a.*,b.bssid from (select * from lzy_new_samples_add_offline) as a left outer join (select row_id,bssid from lzy_new_tongji_wifi_offline) as b on a.row_id=b.row_id) as t
  inner join 
  (select distinct shop_id,bssid from lzy_new_tongji_wifi_offlinewin) as c
  on t.shop_id=c.shop_id and t.bssid=c.bssid
group by t.row_id,t.shop_id;
--test
drop table if exists lzy_new_feat_num_wifi_inshop_online;
create table lzy_new_feat_num_wifi_inshop_online as 
select t.row_id,t.shop_id,count(1) as num_rowwifi_in_shopwifi from 
  (select a.*,b.bssid from (select * from lzy_new_samples_add_online) as a left outer join (select row_id,bssid from lzy_new_tongji_wifi_online) as b on a.row_id=b.row_id) as t
  inner join 
  (select distinct shop_id,bssid from lzy_new_tongji_wifi_onlinewin) as c
  on t.shop_id=c.shop_id and t.bssid=c.bssid
group by t.row_id,t.shop_id;


--shop出现过几个wifi
--train
drop table if exists lzy_new_feat_shop_unique_wifi_offline;
create table lzy_new_feat_shop_unique_wifi_offline as 
select a.mall_id,shop_id,shop_unique_bssid_count,shop_unique_bssid_count/mall_bssid_unique as shop_unique_bssid_rate from
(select mall_id,shop_id,count(1) as shop_unique_bssid_count from
  (select distinct mall_id,bssid,shop_id from lzy_new_tongji_wifi_offlinewin) as t 
 group by mall_id,shop_id
) as a
left outer join 
(select mall_id,count(distinct bssid) as mall_bssid_unique from lzy_new_tongji_wifi_offlinewin group by mall_id) as b
on a.mall_id=b.mall_id;
--test
drop table if exists lzy_new_feat_shop_unique_wifi_online;
create table lzy_new_feat_shop_unique_wifi_online as 
select a.mall_id,shop_id,shop_unique_bssid_count,shop_unique_bssid_count/mall_bssid_unique as shop_unique_bssid_rate from
(select mall_id,shop_id,count(1) as shop_unique_bssid_count from
  (select distinct mall_id,bssid,shop_id from lzy_new_tongji_wifi_onlinewin) as t 
 group by mall_id,shop_id
) as a
left outer join 
(select mall_id,count(distinct bssid) as mall_bssid_unique from lzy_new_tongji_wifi_onlinewin group by mall_id) as b
on a.mall_id=b.mall_id;

--row所有wifi的rssi比shop历史记录该bssid最强值强的num
--train
drop table if exists lzy_new_feat_larger_than_max_offline;
create table lzy_new_feat_larger_than_max_offline as 
select t.row_id,t.shop_id,count(1) as larger_than_max_rssi_num from
(
  select a.row_id,a.shop_id,a.bssid from 
  (
    select m.row_id,m.shop_id,n.bssid,n.rssi from 
    (select * from lzy_new_samples_add_offline) as m left outer join (select row_id,bssid,rssi from lzy_new_tongji_wifi_offline) as n on m.row_id=n.row_id
  ) as a 
  inner join 
  (select bssid,shop_id,max(rssi) as max_rssi from lzy_new_tongji_wifi_offlinewin group by bssid,shop_id) as b
  on a.bssid=b.bssid and a.shop_id=b.shop_id 
  where a.rssi>=b.max_rssi
) as t
group by t.row_id,t.shop_id;
--test
drop table if exists lzy_new_feat_larger_than_max_online;
create table lzy_new_feat_larger_than_max_online as 
select t.row_id,t.shop_id,count(1) as larger_than_max_rssi_num from
(
  select a.row_id,a.shop_id,a.bssid from 
  (
    select m.row_id,m.shop_id,n.bssid,n.rssi from 
    (select * from lzy_new_samples_add_online) as m left outer join (select row_id,bssid,rssi from lzy_new_tongji_wifi_online) as n on m.row_id=n.row_id
  ) as a 
  inner join 
  (select bssid,shop_id,max(rssi) as max_rssi from lzy_new_tongji_wifi_onlinewin group by bssid,shop_id) as b
  on a.bssid=b.bssid and a.shop_id=b.shop_id 
  where a.rssi>=b.max_rssi
) as t
group by t.row_id,t.shop_id;

--在筛选数据中(筛选条件是只保留记录top3wifi)，bssid在shop里的强度中值
--train
drop table if exists lzy_new_feat_median_rssi_selected_offline;
create table lzy_new_feat_median_rssi_selected_offline as 
select bssid,shop_id,median(rssi) as bssid_median_rssi_inselect_inshop ,max(rssi) as bssid_max_rssi_inselect_inshop 
from lzy_new_tongji_wifi_offlinewin where rssi_rank <=3 and row_id in (select row_id from lzy_new_notnull_offline)
group by bssid,shop_id;
--test
drop table if exists lzy_new_feat_median_rssi_selected_online;
create table lzy_new_feat_median_rssi_selected_online as 
select bssid,shop_id,median(rssi) as bssid_median_rssi_inselect_inshop ,max(rssi) as bssid_max_rssi_inselect_inshop 
from lzy_new_tongji_wifi_onlinewin where rssi_rank <=3 and row_id in (select row_id from lzy_new_notnull_online)
group by bssid,shop_id;

--wifi_loss_inselect
--train
drop table if exists lzy_new_feat_wifi_loss_inselect_offline_;
create table lzy_new_feat_wifi_loss_inselect_offline_ as 
select t.*,abs(t.rssi-c.bssid_median_rssi_inselect_inshop) as wifi_loss_inselect from 
(select a.*,b.bssid,b.rssi from (select * from lzy_new_samples_add_offline) as a left outer join (select row_id,bssid,rssi from lzy_new_tongji_wifi_offline where rssi_rank<=3) as b on a.row_id=b.row_id) as t
inner join 
(select shop_id,bssid,bssid_median_rssi_inselect_inshop from lzy_new_feat_median_rssi_selected_offline) as c
on t.shop_id=c.shop_id and t.bssid=c.bssid;

drop table if exists lzy_new_feat_wifi_loss_inselect_offline;
create table lzy_new_feat_wifi_loss_inselect_offline as 
select row_id,shop_id,avg(wifi_loss_inselect) as wifi_loss_inselect from lzy_new_feat_wifi_loss_inselect_offline_ where wifi_loss_inselect is not null group by row_id,shop_id;
drop table if exists lzy_new_feat_wifi_loss_inselect_offline_;
--test
drop table if exists lzy_new_feat_wifi_loss_inselect_online_;
create table lzy_new_feat_wifi_loss_inselect_online_ as 
select t.*,abs(t.rssi-c.bssid_median_rssi_inselect_inshop) as wifi_loss_inselect from 
(select a.*,b.bssid,b.rssi from (select * from lzy_new_samples_add_online) as a left outer join (select row_id,bssid,rssi from lzy_new_tongji_wifi_online where rssi_rank<=3) as b on a.row_id=b.row_id) as t
inner join 
(select shop_id,bssid,bssid_median_rssi_inselect_inshop from lzy_new_feat_median_rssi_selected_online) as c
on t.shop_id=c.shop_id and t.bssid=c.bssid;

drop table if exists lzy_new_feat_wifi_loss_inselect_online;
create table lzy_new_feat_wifi_loss_inselect_online as 
select row_id,shop_id,avg(wifi_loss_inselect) as wifi_loss_inselect from lzy_new_feat_wifi_loss_inselect_online_ where wifi_loss_inselect is not null group by row_id,shop_id;
drop table if exists lzy_new_feat_wifi_loss_inselect_online_;

--各price、category的wifi出现数
--train
drop table if exists lzy_new_feat_p_wificount_sum_offline;
create table lzy_new_feat_p_wificount_sum_offline as 
select a.mall_id,a.price,a.price_wificount_sum,(a.price_wificount_sum/b.mall_wifi_count) as price_wificount_rate from
(select mall_id,price,count(1) as price_wificount_sum from lzy_new_tongji_wifi_offlinewin group by mall_id,price) as a
left outer join 
(select mall_id,count(bssid) as mall_wifi_count from lzy_new_tongji_wifi_offlinewin group by mall_id) as b
on a.mall_id=b.mall_id;

drop table if exists lzy_new_feat_c_wificount_sum_offline;
create table lzy_new_feat_c_wificount_sum_offline as 
select a.mall_id,a.category_id,a.category_wificount_sum,(a.category_wificount_sum/b.mall_wifi_count) as category_wificount_rate from
(select mall_id,category_id,count(1) as category_wificount_sum from lzy_new_tongji_wifi_offlinewin group by mall_id,category_id) as a
left outer join 
(select mall_id,count(bssid) as mall_wifi_count from lzy_new_tongji_wifi_offlinewin group by mall_id) as b
on a.mall_id=b.mall_id;
--test
drop table if exists lzy_new_feat_p_wificount_sum_online;
create table lzy_new_feat_p_wificount_sum_online as 
select a.mall_id,a.price,a.price_wificount_sum,(a.price_wificount_sum/b.mall_wifi_count) as price_wificount_rate from
(select mall_id,price,count(1) as price_wificount_sum from lzy_new_tongji_wifi_onlinewin group by mall_id,price) as a
left outer join 
(select mall_id,count(bssid) as mall_wifi_count from lzy_new_tongji_wifi_onlinewin group by mall_id) as b
on a.mall_id=b.mall_id;

drop table if exists lzy_new_feat_c_wificount_sum_online;
create table lzy_new_feat_c_wificount_sum_online as 
select a.mall_id,a.category_id,a.category_wificount_sum,(a.category_wificount_sum/b.mall_wifi_count) as category_wificount_rate from
(select mall_id,category_id,count(1) as category_wificount_sum from lzy_new_tongji_wifi_onlinewin group by mall_id,category_id) as a
left outer join 
(select mall_id,count(bssid) as mall_wifi_count from lzy_new_tongji_wifi_onlinewin group by mall_id) as b
on a.mall_id=b.mall_id;

--wifi_rank_loss
--train
drop table if exists lzy_new_feat_wifi_r_loss_offline_;
create table lzy_new_feat_wifi_r_loss_offline_ as 
select t.*,c.bssid_mean_rank_inshop,abs(t.rssi_rank-c.bssid_mean_rank_inshop) as wifi_rank_loss from 
(select a.*,b.bssid,b.rssi_rank from (select * from lzy_new_samples_add_offline) as a left outer join (select row_id,bssid,rssi_rank from lzy_new_tongji_wifi_offline where row_id in (select row_id from lzy_new_notnull_offline)) as b on a.row_id=b.row_id) as t
inner join 
(select shop_id,bssid,avg(rssi_rank) as bssid_mean_rank_inshop from lzy_new_tongji_wifi_offlinewin  where row_id in (select row_id from lzy_new_notnull_offline) group by shop_id,bssid) as c
on t.shop_id=c.shop_id and t.bssid=c.bssid;

drop table if exists lzy_new_feat_wifi_r_loss_offline;
create table lzy_new_feat_wifi_r_loss_offline as 
select row_id,shop_id,avg(wifi_rank_loss) as wifi_rank_loss from lzy_new_feat_wifi_r_loss_offline_ where wifi_rank_loss is not null group by row_id,shop_id;
drop table if exists lzy_new_feat_wifi_r_loss_offline_;
--test
drop table if exists lzy_new_feat_wifi_r_loss_online_;
create table lzy_new_feat_wifi_r_loss_online_ as 
select t.*,c.bssid_mean_rank_inshop,abs(t.rssi_rank-c.bssid_mean_rank_inshop) as wifi_rank_loss from 
(select a.*,b.bssid,b.rssi_rank from (select * from lzy_new_samples_add_online) as a left outer join (select row_id,bssid,rssi_rank from lzy_new_tongji_wifi_online where row_id in (select row_id from lzy_new_notnull_online)) as b on a.row_id=b.row_id) as t
inner join 
(select shop_id,bssid,avg(rssi_rank) as bssid_mean_rank_inshop from lzy_new_tongji_wifi_onlinewin  where row_id in (select row_id from lzy_new_notnull_online) group by shop_id,bssid) as c
on t.shop_id=c.shop_id and t.bssid=c.bssid;

drop table if exists lzy_new_feat_wifi_r_loss_online;
create table lzy_new_feat_wifi_r_loss_online as 
select row_id,shop_id,avg(wifi_rank_loss) as wifi_rank_loss from lzy_new_feat_wifi_r_loss_online_ where wifi_rank_loss is not null group by row_id,shop_id;
drop table if exists lzy_new_feat_wifi_r_loss_online_;

--wifi_price_loss
--train
drop table if exists lzy_new_feat_wifi_p_loss_offline_;
create table lzy_new_feat_wifi_p_loss_offline_ as 
select t.*,c.bssid_mean_price,abs(t.price-c.bssid_mean_price) as wifi_price_loss from 
(select a.*,b.bssid,p.mall_id,p.price from (select * from lzy_new_samples_add_offline) as a left outer join (select row_id,bssid from lzy_new_tongji_wifi_offline) as b on a.row_id=b.row_id left outer join (select mall_id,shop_id,price from lzy_ant_tianchi_ccf_sl_shop_info) as p on a.shop_id=p.shop_id) as t
inner join 
(select mall_id,bssid,avg(price) as bssid_mean_price from lzy_new_tongji_wifi_offlinewin group by mall_id,bssid) as c
on t.mall_id=c.mall_id and t.bssid=c.bssid;

drop table if exists lzy_new_feat_wifi_p_loss_offline;
create table lzy_new_feat_wifi_p_loss_offline as 
select row_id,shop_id,avg(wifi_price_loss) as wifi_price_loss from lzy_new_feat_wifi_p_loss_offline_ group by row_id,shop_id;
drop table if exists lzy_new_feat_wifi_p_loss_offline_;
--test
drop table if exists lzy_new_feat_wifi_p_loss_online_;
create table lzy_new_feat_wifi_p_loss_online_ as 
select t.*,c.bssid_mean_price,abs(t.price-c.bssid_mean_price) as wifi_price_loss from 
(select a.*,b.bssid,p.mall_id,p.price from (select * from lzy_new_samples_add_online) as a left outer join (select row_id,bssid from lzy_new_tongji_wifi_online) as b on a.row_id=b.row_id left outer join (select mall_id,shop_id,price from lzy_ant_tianchi_ccf_sl_shop_info) as p on a.shop_id=p.shop_id) as t
inner join 
(select mall_id,bssid,avg(price) as bssid_mean_price from lzy_new_tongji_wifi_onlinewin group by mall_id,bssid) as c
on t.mall_id=c.mall_id and t.bssid=c.bssid;

drop table if exists lzy_new_feat_wifi_p_loss_online;
create table lzy_new_feat_wifi_p_loss_online as 
select row_id,shop_id,avg(wifi_price_loss) as wifi_price_loss from lzy_new_feat_wifi_p_loss_online_ group by row_id,shop_id;
drop table if exists lzy_new_feat_wifi_p_loss_online_;

--bssid在shop的强度中值rank
drop table if exists lzy_new_feat_rank_inshop_offline;
create table lzy_new_feat_rank_inshop_offline as 
select mall_id,bssid,shop_id,rank() over(partition by shop_id order by bssid_median_rssi_inshop desc) as bssid_rank_inshop from lzy_new_shop_wifi_offlinewin where row_id in (select row_id from lzy_new_notnull_offline); 

drop table if exists lzy_new_feat_rank_inshop_online;
create table lzy_new_feat_rank_inshop_online as 
select mall_id,bssid,shop_id,rank() over(partition by shop_id order by bssid_median_rssi_inshop desc) as bssid_rank_inshop from lzy_new_shop_wifi_onlinewin where row_id in (select row_id from lzy_new_notnull_online); 


--店铺的rssi贡献率
--train
drop table if exists lzy_new_feat_contri_offline;
create table lzy_new_feat_contri_offline as 
select mall_id,shop_id,bssid,sum(-1/rssi) as contri_inshop from lzy_new_tongji_wifi_offlinewin where rssi <0 group by mall_id,shop_id,bssid;

drop table if exists lzy_new_feat_contri_offline_;
create table lzy_new_feat_contri_offline_ as 
select a.*,(contri_inshop/shop_contri_sum) as contri_rate_inshop,(contri_inshop/bssid_contri_sum) as contri_rate_inbssid from
(select * from lzy_new_feat_contri_offline) as a
left outer join 
(select shop_id,sum(contri_inshop) as shop_contri_sum from lzy_new_feat_contri_offline group by shop_id) as b
on a.shop_id=b.shop_id
left outer join
(select mall_id,bssid,sum(contri_inshop) as bssid_contri_sum from lzy_new_feat_contri_offline group by mall_id,bssid) as c
on a.mall_id=c.mall_id and a.bssid=c.bssid;

drop table if exists lzy_new_feat_contri_offline;
create table lzy_new_feat_contri_offline as 
select t.row_id,t.shop_id,sum(contri_inshop) as contri_inshop,sum(contri_rate_inshop) as contri_rate_inshop,sum(contri_rate_inbssid) as  contri_rate_inbssid from
  (select a.*,b.bssid from (select * from lzy_new_samples_add_offline) as a left outer join (select row_id,bssid from lzy_new_tongji_wifi_offline) as b on a.row_id=b.row_id) as t
  inner join 
  (select shop_id,bssid,contri_inshop,contri_rate_inshop,contri_rate_inbssid from lzy_new_feat_contri_offline_) as c
  on t.shop_id=c.shop_id and t.bssid=c.bssid
group by t.row_id,t.shop_id;
drop table if exists lzy_new_feat_contri_offline_;
--test
drop table if exists lzy_new_feat_contri_online;
create table lzy_new_feat_contri_online as 
select mall_id,shop_id,bssid,sum(-1/rssi) as contri_inshop from lzy_new_tongji_wifi_onlinewin where rssi <0 group by mall_id,shop_id,bssid;

drop table if exists lzy_new_feat_contri_online_;
create table lzy_new_feat_contri_online_ as 
select a.*,(contri_inshop/shop_contri_sum) as contri_rate_inshop,(contri_inshop/bssid_contri_sum) as contri_rate_inbssid from
(select * from lzy_new_feat_contri_online) as a
left outer join 
(select shop_id,sum(contri_inshop) as shop_contri_sum from lzy_new_feat_contri_online group by shop_id) as b
on a.shop_id=b.shop_id
left outer join
(select mall_id,bssid,sum(contri_inshop) as bssid_contri_sum from lzy_new_feat_contri_online group by mall_id,bssid) as c
on a.mall_id=c.mall_id and a.bssid=c.bssid;

drop table if exists lzy_new_feat_contri_online;
create table lzy_new_feat_contri_online as 
select t.row_id,t.shop_id,sum(contri_inshop) as contri_inshop,sum(contri_rate_inshop) as contri_rate_inshop,sum(contri_rate_inbssid) as  contri_rate_inbssid from
  (select a.*,b.bssid from (select * from lzy_new_samples_add_online) as a left outer join (select row_id,bssid from lzy_new_tongji_wifi_online) as b on a.row_id=b.row_id) as t
  inner join 
  (select shop_id,bssid,contri_inshop,contri_rate_inshop,contri_rate_inbssid from lzy_new_feat_contri_online_) as c
  on t.shop_id=c.shop_id and t.bssid=c.bssid
group by t.row_id,t.shop_id;
drop table if exists lzy_new_feat_contri_online_;


---other-train
--cross1 user和shop交叉，稀疏
drop table if exists lzy_new_feat_cross1_offline_;
create table lzy_new_feat_cross1_offline_ as 
select a.*,b.user_count from 
(select user_id,shop_id,count(1) as user_shop_count from lzy_new_tongji_offlinewin group by user_id,shop_id) as a
left outer join
(select user_id,count(1) as user_count from lzy_new_tongji_offlinewin group by user_id) as b
on a.user_id=b.user_id;

drop table if exists lzy_new_feat_cross1_offline;
create table lzy_new_feat_cross1_offline as 
select a.*,b.user_shop_count,b.user_count from
(
  select m.row_id,m.shop_id,n.user_id from 
  (select * from lzy_new_samples_add_offline) as m left outer join (select row_id,user_id from lzy_new_tongji_offline) as n on m.row_id=n.row_id
) as a 
left outer join 
lzy_new_feat_cross1_offline_ as b
on a.shop_id=b.shop_id and a.user_id=b.user_id;
drop table if exists lzy_new_feat_cross1_offline_;

drop table if exists lzy_new_feat_cross1_offline_;
create table lzy_new_feat_cross1_offline_ as 
select row_id,shop_id,user_id,user_shop_count,user_count,user_shop_smoothrate from 
(select row_id,shop_id,user_id,user_shop_count,user_count,0 as user_shop_smoothrate from lzy_new_feat_cross1_offline where user_count is null or user_count<=3
union 
select row_id,shop_id,user_id,user_shop_count,user_count,cast(user_shop_count as double)/user_count as user_shop_smoothrate from lzy_new_feat_cross1_offline where user_count is not null and user_count>3
) as t;

drop table if exists lzy_new_feat_cross1_offline;
create table lzy_new_feat_cross1_offline as select * from lzy_new_feat_cross1_offline_;
drop table if exists lzy_new_feat_cross1_offline_;
--cross2 shop和hour交叉
drop table if exists lzy_new_feat_cross2_offline_;
create table lzy_new_feat_cross2_offline_ as 
select a.*,shop_count,hour_count,(shop_hour_count/shop_count) as shop_hour_rate_inshop,(shop_hour_count/hour_count) as shop_hour_rate_inhour from 
(select mall_id,shop_id,hour,count(1) as shop_hour_count from lzy_new_tongji_offlinewin group by mall_id,shop_id,hour) as a
left outer join
(select shop_id,count(1) as shop_count from lzy_new_tongji_offlinewin group by shop_id) as b
on a.shop_id=b.shop_id
left outer join
(select mall_id,hour,count(1) as hour_count from lzy_new_tongji_offlinewin group by mall_id,hour) as c
on a.mall_id=c.mall_id and a.hour=c.hour;

drop table if exists lzy_new_feat_cross2_offline;
create table lzy_new_feat_cross2_offline as 
select a.*,b.shop_hour_count,b.shop_hour_rate_inshop,b.shop_hour_rate_inhour from
(
  select m.row_id,m.shop_id,n.hour from 
  (select * from lzy_new_samples_add_offline) as m left outer join (select row_id,hour from lzy_new_tongji_offline) as n on m.row_id=n.row_id
) as a 
left outer join 
lzy_new_feat_cross2_offline_ as b
on a.shop_id=b.shop_id and a.hour=b.hour;
drop table if exists lzy_new_feat_cross2_offline_;


--cross3 category和hour交叉
drop table if exists lzy_new_feat_cross3_offline_;
create table lzy_new_feat_cross3_offline_ as 
select a.*,category_count,hour_count,(category_hour_count/category_count) as category_hour_rate_incategory,(category_hour_count/hour_count) as category_hour_rate_inhour from 
(select mall_id,category_id,hour,count(1) as category_hour_count from lzy_new_tongji_offlinewin group by mall_id,category_id,hour) as a
left outer join
(select category_id,count(1) as category_count from lzy_new_tongji_offlinewin group by category_id) as b
on a.category_id=b.category_id
left outer join
(select mall_id,hour,count(1) as hour_count from lzy_new_tongji_offlinewin group by mall_id,hour) as c
on a.mall_id=c.mall_id and a.hour=c.hour;

drop table if exists lzy_new_feat_tmp3_offline_;
create table lzy_new_feat_tmp3_offline_ as 
select m.row_id,m.shop_id,hour from 
(select * from lzy_new_samples_add_offline) as m 
left outer join 
(select row_id,hour from lzy_new_tongji_offline) as n 
on m.row_id=n.row_id;

drop table if exists lzy_new_feat_tmp3_offline;
create table lzy_new_feat_tmp3_offline as 
select a.*,category_id,mall_id from 
(select * from lzy_new_feat_tmp3_offline_) as a 
left outer join 
(select shop_id,category_id,mall_id from lzy_ant_tianchi_ccf_sl_shop_info) as p 
on a.shop_id=p.shop_id;
drop table if exists lzy_new_feat_tmp3_offline_;

drop table if exists lzy_new_feat_cross3_offline;
create table lzy_new_feat_cross3_offline as 
select a.*,b.category_hour_count,b.category_hour_rate_incategory,b.category_hour_rate_inhour from
lzy_new_feat_tmp3_offline as a 
left outer join 
lzy_new_feat_cross3_offline_ as b
on a.mall_id=b.mall_id and a.category_id=b.category_id and a.hour=b.hour;
drop table if exists lzy_new_feat_cross3_offline_;
drop table if exists lzy_new_feat_tmp3_offline;

--other1 店铺的再次光顾率
drop table if exists lzy_new_feat_other1_offline_;
create table lzy_new_feat_other1_offline_ as 
select shop_id,avg(user_shop_again_rate) as user_shop_again_rate from
(select user_id,shop_id,(case when user_shop_count>1 then 1 else 0 end) as user_shop_again_rate from
 (select user_id,shop_id,count(1) as user_shop_count from lzy_new_tongji_offlinewin group by user_id,shop_id) as a) as b
group by shop_id;

drop table if exists lzy_new_feat_other1_offline;
create table lzy_new_feat_other1_offline as 
select a.*,b.user_shop_again_rate from
(select * from lzy_new_samples_add_offline) as a 
left outer join 
lzy_new_feat_other1_offline_ as b
on a.shop_id=b.shop_id;
drop table if exists lzy_new_feat_other1_offline_;

--other2 店铺在时间点类型(是否饭点)的记录数、比例
drop table if exists lzy_new_feat_other2_offline_;
create table lzy_new_feat_other2_offline_ as 
select a.shop_id,a.is_eattime,count_at_timetype_inshop,(count_at_timetype_inshop/count_inshop_sum) as rate_at_timetype_inshop,(count_at_timetype_inshop/count_ineattime_sum) as rate_at_timetype_intime from
(
  select shop_id,is_eattime,count(1) as count_at_timetype_inshop from
  (
  select shop_id,
  (case when (hour>=12 and hour<=13) or (hour>=18 and hour<=20) then 1 else 0 end) as is_eattime
  from lzy_new_tongji_offlinewin
  ) as p
  group by shop_id,is_eattime
) as a
left outer join 
(
  select shop_id,count(1) as count_inshop_sum from
  (
  select shop_id,
  (case when (hour>=12 and hour<=13) or (hour>=18 and hour<=20) then 1 else 0 end) as is_eattime
  from lzy_new_tongji_offlinewin
  ) as q
  group by shop_id
) as b
on a.shop_id=b.shop_id
left outer join 
(
  select is_eattime,count(1) as count_ineattime_sum from
  (
  select shop_id,
  (case when (hour>=12 and hour<=13) or (hour>=18 and hour<=20) then 1 else 0 end) as is_eattime
  from lzy_new_tongji_offlinewin
  ) as m
  group by is_eattime
) as c
on a.is_eattime=c.is_eattime;


drop table if exists lzy_new_feat_other2_offline;
create table lzy_new_feat_other2_offline as 
select a.*,b.count_at_timetype_inshop,b.rate_at_timetype_inshop,b.rate_at_timetype_intime from
(
  select m.row_id,m.shop_id,(case when (hour>=12 and hour<=13) or (hour>=18 and hour<=20) then 1 else 0 end) as is_eattime from 
  (select * from lzy_new_samples_add_offline) as m left outer join (select row_id,hour from lzy_new_tongji_offline) as n on m.row_id=n.row_id
) as a 
left outer join 
lzy_new_feat_other2_offline_ as b
on a.shop_id=b.shop_id and a.is_eattime=b.is_eattime;
drop table if exists lzy_new_feat_other2_offline_;

--other3 店铺在日期点类型(是否周末)的记录数、比例
drop table if exists lzy_new_feat_other3_offline;
create table lzy_new_feat_other3_offline as 
select shop_id,is_hotday,count(1) as count_at_daytype_inshop from
(
  select *,
  (case when ((month-7)*31+day)%7>=1 and ((month-7)*31+day)%7<=2 then 1 else 0 end ) as is_hotday
  from lzy_new_tongji_offlinewin
) as a
group by shop_id,is_hotday;

drop table if exists lzy_new_feat_other3_offline_;
create table lzy_new_feat_other3_offline_ as 
select a.shop_id,a.is_hotday,count_at_daytype_inshop,(count_at_daytype_inshop/count_inshop_sum) as rate_at_daytype_inshop,(count_at_daytype_inshop/count_indaytype_sum) as rate_at_daytype_inday from
(
  select shop_id,is_hotday,count(1) as count_at_daytype_inshop from
  (
  select shop_id,
  (case when ((month-7)*31+day)%7>=1 and ((month-7)*31+day)%7<=2 then 1 else 0 end ) as is_hotday
  from lzy_new_tongji_offlinewin
  ) as p
  group by shop_id,is_hotday
) as a
left outer join 
(
  select shop_id,count(1) as count_inshop_sum from
  (
  select shop_id,
  (case when ((month-7)*31+day)%7>=1 and ((month-7)*31+day)%7<=2 then 1 else 0 end ) as is_hotday
  from lzy_new_tongji_offlinewin
  ) as q
  group by shop_id
) as b
on a.shop_id=b.shop_id
left outer join 
(
  select is_hotday,count(1) as count_indaytype_sum from
  (
  select shop_id,
  (case when ((month-7)*31+day)%7>=1 and ((month-7)*31+day)%7<=2 then 1 else 0 end ) as is_hotday
  from lzy_new_tongji_offlinewin
  ) as m
  group by is_hotday
) as c
on a.is_hotday=c.is_hotday;


drop table if exists lzy_new_feat_other3_offline;
create table lzy_new_feat_other3_offline as 
select a.*,b.count_at_daytype_inshop,b.rate_at_daytype_inshop,b.rate_at_daytype_inday from
(
  select m.row_id,m.shop_id,(case when ((month-7)*31+day)%7>=1 and ((month-7)*31+day)%7<=2 then 1 else 0 end ) as is_hotday from 
  (select * from lzy_new_samples_add_offline) as m left outer join (select row_id,month,day from lzy_new_tongji_offline) as n on m.row_id=n.row_id
) as a 
left outer join 
lzy_new_feat_other3_offline_ as b
on a.shop_id=b.shop_id and a.is_hotday=b.is_hotday;
drop table if exists lzy_new_feat_other3_offline_;


--other4 用户平均消费水平与店铺消费水平差
drop table if exists lzy_new_feat_other4_offline_;
create table lzy_new_feat_other4_offline_ as 
select user_id,avg(price) as user_mean_price from lzy_new_tongji_offlinewin group by user_id;

drop table if exists lzy_new_feat_tmp_offline_;
create table lzy_new_feat_tmp_offline_ as 
select m.row_id,m.shop_id,user_id from 
(select * from lzy_new_samples_add_offline) as m 
left outer join 
(select row_id,user_id from lzy_new_tongji_offline) as n 
on m.row_id=n.row_id;

drop table if exists lzy_new_feat_tmp_offline;
create table lzy_new_feat_tmp_offline as 
select a.*,price from 
(select * from lzy_new_feat_tmp_offline_) as a 
left outer join 
(select shop_id,price from lzy_ant_tianchi_ccf_sl_shop_info) as p 
on a.shop_id=p.shop_id;
drop table if exists lzy_new_feat_tmp_offline_;

drop table if exists lzy_new_feat_other4_offline;
create table lzy_new_feat_other4_offline as 
select a.*,user_mean_price,abs(price-user_mean_price) as price_diff_user_mean_price from
lzy_new_feat_tmp_offline as a 
left outer join 
lzy_new_feat_other4_offline_ as b
on a.user_id=b.user_id;
drop table if exists lzy_new_feat_other4_offline_;
drop table if exists lzy_new_feat_tmp_offline;
--test
--cross1 user和shop交叉，由于稀疏，考虑LabelEncode
drop table if exists lzy_new_feat_cross1_online_;
create table lzy_new_feat_cross1_online_ as 
select a.*,b.user_count from 
(select user_id,shop_id,count(1) as user_shop_count from lzy_new_tongji_onlinewin group by user_id,shop_id) as a
left outer join
(select user_id,count(1) as user_count from lzy_new_tongji_onlinewin group by user_id) as b
on a.user_id=b.user_id;

drop table if exists lzy_new_feat_cross1_online;
create table lzy_new_feat_cross1_online as 
select a.*,b.user_shop_count,b.user_count from
(
  select m.row_id,m.shop_id,n.user_id from 
  (select * from lzy_new_samples_add_online) as m left outer join (select row_id,user_id from lzy_new_tongji_online) as n on m.row_id=n.row_id
) as a 
left outer join 
lzy_new_feat_cross1_online_ as b
on a.shop_id=b.shop_id and a.user_id=b.user_id;
drop table if exists lzy_new_feat_cross1_online_;

drop table if exists lzy_new_feat_cross1_online_;
create table lzy_new_feat_cross1_online_ as 
select row_id,shop_id,user_id,user_shop_count,user_count,user_shop_smoothrate from 
(select row_id,shop_id,user_id,user_shop_count,user_count,0 as user_shop_smoothrate from lzy_new_feat_cross1_online where user_count is null or user_count<=3
union 
select row_id,shop_id,user_id,user_shop_count,user_count,cast(user_shop_count as double)/user_count as user_shop_smoothrate from lzy_new_feat_cross1_online where user_count is not null and user_count>3
) as t;

drop table if exists lzy_new_feat_cross1_online;
create table lzy_new_feat_cross1_online as select * from lzy_new_feat_cross1_online_;
drop table if exists lzy_new_feat_cross1_online_;
--cross2 shop和hour交叉
drop table if exists lzy_new_feat_cross2_online_;
create table lzy_new_feat_cross2_online_ as 
select a.*,shop_count,hour_count,(shop_hour_count/shop_count) as shop_hour_rate_inshop,(shop_hour_count/hour_count) as shop_hour_rate_inhour from 
(select mall_id,shop_id,hour,count(1) as shop_hour_count from lzy_new_tongji_onlinewin group by mall_id,shop_id,hour) as a
left outer join
(select shop_id,count(1) as shop_count from lzy_new_tongji_onlinewin group by shop_id) as b
on a.shop_id=b.shop_id
left outer join
(select mall_id,hour,count(1) as hour_count from lzy_new_tongji_onlinewin group by mall_id,hour) as c
on a.mall_id=c.mall_id and a.hour=c.hour;

drop table if exists lzy_new_feat_cross2_online;
create table lzy_new_feat_cross2_online as 
select a.*,b.shop_hour_count,b.shop_hour_rate_inshop,b.shop_hour_rate_inhour from
(
  select m.row_id,m.shop_id,n.hour from 
  (select * from lzy_new_samples_add_online) as m left outer join (select row_id,hour from lzy_new_tongji_online) as n on m.row_id=n.row_id
) as a 
left outer join 
lzy_new_feat_cross2_online_ as b
on a.shop_id=b.shop_id and a.hour=b.hour;
drop table if exists lzy_new_feat_cross2_online_;

--cross3 category和hour交叉
drop table if exists lzy_new_feat_cross3_online_;
create table lzy_new_feat_cross3_online_ as 
select a.*,category_count,hour_count,(category_hour_count/category_count) as category_hour_rate_incategory,(category_hour_count/hour_count) as category_hour_rate_inhour from 
(select mall_id,category_id,hour,count(1) as category_hour_count from lzy_new_tongji_onlinewin group by mall_id,category_id,hour) as a
left outer join
(select category_id,count(1) as category_count from lzy_new_tongji_onlinewin group by category_id) as b
on a.category_id=b.category_id
left outer join
(select mall_id,hour,count(1) as hour_count from lzy_new_tongji_onlinewin group by mall_id,hour) as c
on a.mall_id=c.mall_id and a.hour=c.hour;

drop table if exists lzy_new_feat_tmp3_online_;
create table lzy_new_feat_tmp3_online_ as 
select m.row_id,m.shop_id,hour from 
(select * from lzy_new_samples_add_online) as m 
left outer join 
(select row_id,hour from lzy_new_tongji_online) as n 
on m.row_id=n.row_id;

drop table if exists lzy_new_feat_tmp3_online;
create table lzy_new_feat_tmp3_online as 
select a.*,category_id,mall_id from 
(select * from lzy_new_feat_tmp3_online_) as a 
left outer join 
(select shop_id,category_id,mall_id from lzy_ant_tianchi_ccf_sl_shop_info) as p 
on a.shop_id=p.shop_id;
drop table if exists lzy_new_feat_tmp3_online_;

drop table if exists lzy_new_feat_cross3_online;
create table lzy_new_feat_cross3_online as 
select a.*,b.category_hour_count,b.category_hour_rate_incategory,b.category_hour_rate_inhour from
lzy_new_feat_tmp3_online as a 
left outer join 
lzy_new_feat_cross3_online_ as b
on a.mall_id=b.mall_id and a.category_id=b.category_id and a.hour=b.hour;
drop table if exists lzy_new_feat_cross3_online_;
drop table if exists lzy_new_feat_tmp3_online;

--other1 店铺的再次光顾率
drop table if exists lzy_new_feat_other1_online_;
create table lzy_new_feat_other1_online_ as 
select shop_id,avg(user_shop_again_rate) as user_shop_again_rate from
(select user_id,shop_id,(case when user_shop_count>1 then 1 else 0 end) as user_shop_again_rate from
 (select user_id,shop_id,count(1) as user_shop_count from lzy_new_tongji_onlinewin group by user_id,shop_id) as a) as b
group by shop_id;

drop table if exists lzy_new_feat_other1_online;
create table lzy_new_feat_other1_online as 
select a.*,b.user_shop_again_rate from
(select * from lzy_new_samples_add_online) as a 
left outer join 
lzy_new_feat_other1_online_ as b
on a.shop_id=b.shop_id;
drop table if exists lzy_new_feat_other1_online_;

--other2 店铺在时间点类型(是否饭点)的记录数、比例
drop table if exists lzy_new_feat_other2_online_;
create table lzy_new_feat_other2_online_ as 
select a.shop_id,a.is_eattime,count_at_timetype_inshop,(count_at_timetype_inshop/count_inshop_sum) as rate_at_timetype_inshop,(count_at_timetype_inshop/count_ineattime_sum) as rate_at_timetype_intime from
(
  select shop_id,is_eattime,count(1) as count_at_timetype_inshop from
  (
  select shop_id,
  (case when (hour>=12 and hour<=13) or (hour>=18 and hour<=20) then 1 else 0 end) as is_eattime
  from lzy_new_tongji_onlinewin
  ) as p
  group by shop_id,is_eattime
) as a
left outer join 
(
  select shop_id,count(1) as count_inshop_sum from
  (
  select shop_id,
  (case when (hour>=12 and hour<=13) or (hour>=18 and hour<=20) then 1 else 0 end) as is_eattime
  from lzy_new_tongji_onlinewin
  ) as q
  group by shop_id
) as b
on a.shop_id=b.shop_id
left outer join 
(
  select is_eattime,count(1) as count_ineattime_sum from
  (
  select shop_id,
  (case when (hour>=12 and hour<=13) or (hour>=18 and hour<=20) then 1 else 0 end) as is_eattime
  from lzy_new_tongji_onlinewin
  ) as m
  group by is_eattime
) as c
on a.is_eattime=c.is_eattime;


drop table if exists lzy_new_feat_other2_online;
create table lzy_new_feat_other2_online as 
select a.*,b.count_at_timetype_inshop,b.rate_at_timetype_inshop,b.rate_at_timetype_intime from
(
  select m.row_id,m.shop_id,(case when (hour>=12 and hour<=13) or (hour>=18 and hour<=20) then 1 else 0 end) as is_eattime from 
  (select * from lzy_new_samples_add_online) as m left outer join (select row_id,hour from lzy_new_tongji_online) as n on m.row_id=n.row_id
) as a 
left outer join 
lzy_new_feat_other2_online_ as b
on a.shop_id=b.shop_id and a.is_eattime=b.is_eattime;
drop table if exists lzy_new_feat_other2_online_;

--other3 店铺在日期点类型(是否周末)的记录数、比例
drop table if exists lzy_new_feat_other3_online;
create table lzy_new_feat_other3_online as 
select shop_id,is_hotday,count(1) as count_at_daytype_inshop from
(
  select *,
  (case when ((month-7)*31+day)%7>=1 and ((month-7)*31+day)%7<=2 then 1 else 0 end ) as is_hotday
  from lzy_new_tongji_onlinewin
) as a
group by shop_id,is_hotday;

drop table if exists lzy_new_feat_other3_online_;
create table lzy_new_feat_other3_online_ as 
select a.shop_id,a.is_hotday,count_at_daytype_inshop,(count_at_daytype_inshop/count_inshop_sum) as rate_at_daytype_inshop,(count_at_daytype_inshop/count_indaytype_sum) as rate_at_daytype_inday from
(
  select shop_id,is_hotday,count(1) as count_at_daytype_inshop from
  (
  select shop_id,
  (case when ((month-7)*31+day)%7>=1 and ((month-7)*31+day)%7<=2 then 1 else 0 end ) as is_hotday
  from lzy_new_tongji_onlinewin
  ) as p
  group by shop_id,is_hotday
) as a
left outer join 
(
  select shop_id,count(1) as count_inshop_sum from
  (
  select shop_id,
  (case when ((month-7)*31+day)%7>=1 and ((month-7)*31+day)%7<=2 then 1 else 0 end ) as is_hotday
  from lzy_new_tongji_onlinewin
  ) as q
  group by shop_id
) as b
on a.shop_id=b.shop_id
left outer join 
(
  select is_hotday,count(1) as count_indaytype_sum from
  (
  select shop_id,
  (case when ((month-7)*31+day)%7>=1 and ((month-7)*31+day)%7<=2 then 1 else 0 end ) as is_hotday
  from lzy_new_tongji_onlinewin
  ) as m
  group by is_hotday
) as c
on a.is_hotday=c.is_hotday;


drop table if exists lzy_new_feat_other3_online;
create table lzy_new_feat_other3_online as 
select a.*,b.count_at_daytype_inshop,b.rate_at_daytype_inshop,b.rate_at_daytype_inday from
(
  select m.row_id,m.shop_id,(case when ((month-7)*31+day)%7>=1 and ((month-7)*31+day)%7<=2 then 1 else 0 end ) as is_hotday from 
  (select * from lzy_new_samples_add_online) as m left outer join (select row_id,day,month from lzy_new_tongji_online) as n on m.row_id=n.row_id
) as a 
left outer join 
lzy_new_feat_other3_online_ as b
on a.shop_id=b.shop_id and a.is_hotday=b.is_hotday;
drop table if exists lzy_new_feat_other3_online_;


--other4 用户平均消费水平与店铺消费水平差
drop table if exists lzy_new_feat_other4_online_;
create table lzy_new_feat_other4_online_ as 
select user_id,avg(price) as user_mean_price from lzy_new_tongji_onlinewin group by user_id;

drop table if exists lzy_new_feat_tmp_online_;
create table lzy_new_feat_tmp_online_ as 
select m.row_id,m.shop_id,user_id from 
(select * from lzy_new_samples_add_online) as m 
left outer join 
(select row_id,user_id from lzy_new_tongji_online) as n 
on m.row_id=n.row_id;

drop table if exists lzy_new_feat_tmp_online;
create table lzy_new_feat_tmp_online as 
select a.*,price from 
(select * from lzy_new_feat_tmp_online_) as a 
left outer join 
(select shop_id,price from lzy_ant_tianchi_ccf_sl_shop_info) as p 
on a.shop_id=p.shop_id;
drop table if exists lzy_new_feat_tmp_online_;

drop table if exists lzy_new_feat_other4_online;
create table lzy_new_feat_other4_online as 
select a.*,user_mean_price,abs(price-user_mean_price) as price_diff_user_mean_price from
lzy_new_feat_tmp_online as a 
left outer join 
lzy_new_feat_other4_online_ as b
on a.user_id=b.user_id;
drop table if exists lzy_new_feat_other4_online_;
drop table if exists lzy_new_feat_tmp_online;


--connect
--7.1~8.17 
drop table if exists lzy_new_connect_offlinewin;
create table lzy_new_connect_offlinewin as 
select mall_id,bssid,shop_id,sum(connect) as connect_count_inshop from lzy_new_tongji_wifi_offlinewin group by mall_id,bssid,shop_id;

drop table if exists lzy_new_connect_offlinewin_;
create table lzy_new_connect_offlinewin_ as 
select a.*,b.shop_wificount_sum from lzy_new_connect_offlinewin as a
left outer join (select distinct mall_id,shop_id, shop_wificount_sum from lzy_new_shop_wifi_offlinewin) as b
on a.mall_id=b.mall_id and a.shop_id=b.shop_id;

drop table if exists lzy_new_connect_offlinewin;
create table lzy_new_connect_offlinewin as 
select a.*,b.bssid_count_sum,connect_count_inshop/shop_wificount_sum as connect_rate_inshop,connect_count_inshop/bssid_count_sum as connect_rate_inbssid from lzy_new_connect_offlinewin_ as a
left outer join (select distinct mall_id,bssid,bssid_count_sum from lzy_new_shop_wifi_offlinewin) as b
on a.mall_id=b.mall_id and a.bssid=b.bssid;
--平滑
drop table if exists lzy_new_connect_offlinewin_;
create table lzy_new_connect_offlinewin_ as 
select mall_id,bssid,shop_id,connect_count_inshop,shop_wificount_sum,bssid_count_sum,connect_rate_inshop,connect_rate_inbssid, connect_rate_inbssid as connect_smoothrate_inbssid
from lzy_new_connect_offlinewin where bssid_count_sum>=5
union all 
select mall_id,bssid,shop_id,connect_count_inshop,shop_wificount_sum,bssid_count_sum,connect_rate_inshop,connect_rate_inbssid, 0 as connect_smoothrate_inbssid
from lzy_new_connect_offlinewin where bssid_count_sum<5;
drop table if exists lzy_new_connect_offlinewin;
create table lzy_new_connect_offlinewin as select * from lzy_new_connect_offlinewin_;
drop table if exists lzy_new_connect_offlinewin_;
--feat_connect
drop table if exists lzy_new_feat_connect_offline;
create table lzy_new_feat_connect_offline as 
select row_id,shop_id,sum(connect_count_inshop) as connect_count_inshop,sum(connect_rate_inshop) as connect_rate_inshop,sum(connect_rate_inbssid) as connect_rate_inbssid,sum(connect_smoothrate_inbssid) as connect_smoothrate_inbssid from
( 
  select a.*,connect_count_inshop,connect_rate_inshop,connect_rate_inbssid,connect_smoothrate_inbssid from 
  (
    select m.row_id,m.shop_id,n.bssid,n.mall_id from 
    (select * from lzy_new_samples_add_offline) as m left outer join (select row_id,bssid,mall_id from lzy_new_tongji_wifi_offline) as n on m.row_id=n.row_id
  ) as a 
  inner join 
  (select bssid,shop_id,connect_count_inshop,connect_rate_inshop,connect_rate_inbssid,connect_smoothrate_inbssid from lzy_new_connect_offlinewin) as b
  on a.bssid=b.bssid and a.shop_id=b.shop_id
) as t
group by row_id,shop_id;
--7.15~8.31
drop table if exists lzy_new_connect_onlinewin;
create table lzy_new_connect_onlinewin as 
select mall_id,bssid,shop_id,sum(connect) as connect_count_inshop from lzy_new_tongji_wifi_onlinewin group by mall_id,bssid,shop_id;

drop table if exists lzy_new_connect_onlinewin_;
create table lzy_new_connect_onlinewin_ as 
select a.*,b.shop_wificount_sum from lzy_new_connect_onlinewin as a
left outer join (select distinct mall_id,shop_id, shop_wificount_sum from lzy_new_shop_wifi_onlinewin) as b
on a.mall_id=b.mall_id and a.shop_id=b.shop_id;

drop table if exists lzy_new_connect_onlinewin;
create table lzy_new_connect_onlinewin as 
select a.*,b.bssid_count_sum,connect_count_inshop/shop_wificount_sum as connect_rate_inshop,connect_count_inshop/bssid_count_sum as connect_rate_inbssid from lzy_new_connect_onlinewin_ as a
left outer join (select distinct mall_id,bssid,bssid_count_sum from lzy_new_shop_wifi_onlinewin) as b
on a.mall_id=b.mall_id and a.bssid=b.bssid;
--平滑
drop table if exists lzy_new_connect_onlinewin_;
create table lzy_new_connect_onlinewin_ as 
select mall_id,bssid,shop_id,connect_count_inshop,shop_wificount_sum,bssid_count_sum,connect_rate_inshop,connect_rate_inbssid, connect_rate_inbssid as connect_smoothrate_inbssid
from lzy_new_connect_onlinewin where bssid_count_sum>=5
union all 
select mall_id,bssid,shop_id,connect_count_inshop,shop_wificount_sum,bssid_count_sum,connect_rate_inshop,connect_rate_inbssid, 0 as connect_smoothrate_inbssid
from lzy_new_connect_onlinewin where bssid_count_sum<5;
drop table if exists lzy_new_connect_onlinewin;
create table lzy_new_connect_onlinewin as select * from lzy_new_connect_onlinewin_;
drop table if exists lzy_new_connect_onlinewin_;
--feat_connect
drop table if exists lzy_new_feat_connect_online;
create table lzy_new_feat_connect_online as 
select row_id,shop_id,sum(connect_count_inshop) as connect_count_inshop,sum(connect_rate_inshop) as connect_rate_inshop,sum(connect_rate_inbssid) as connect_rate_inbssid,sum(connect_smoothrate_inbssid) as connect_smoothrate_inbssid from
( 
  select a.*,connect_count_inshop,connect_rate_inshop,connect_rate_inbssid,connect_smoothrate_inbssid from 
  (
    select m.row_id,m.shop_id,n.bssid,n.mall_id from 
    (select * from lzy_new_samples_add_online) as m left outer join (select row_id,bssid,mall_id from lzy_new_tongji_wifi_online) as n on m.row_id=n.row_id
  ) as a 
  inner join 
  (select bssid,shop_id,connect_count_inshop,connect_rate_inshop,connect_rate_inbssid,connect_smoothrate_inbssid from lzy_new_connect_onlinewin) as b
  on a.bssid=b.bssid and a.shop_id=b.shop_id
) as t
group by row_id,shop_id;


--与店铺pos位置的距离
drop table if exists lzy_new_user_distance_to_shoppos_before_offline;
create table lzy_new_user_distance_to_shoppos_before_offline as 
select a.*,b.pred_shop,
    round(abs(a.longitude-b.shop_longitude),6) as pos_longitude_diff, 
  round(abs(a.latitude-b.shop_latitude),6) as pos_latitude_diff,
  round(sqrt(pow((a.longitude-b.shop_longitude)*111.3195,2)+pow((a.latitude-b.shop_latitude)*111.3195,2))*1000,2) as pos_distance_diff from
(select  row_id,mall_id,shop_id,longitude,latitude from lzy_new_tongji_offline ) as a inner join 
(select mall_id,shop_id as pred_shop, longitude as shop_longitude,latitude as shop_latitude from lzy_ant_tianchi_ccf_sl_shop_info) as b on a.mall_id=b.mall_id;

drop table if exists lzy_new_user_distance_to_shoppos_before_online;
create table lzy_new_user_distance_to_shoppos_before_online as 
select a.*,b.pred_shop,
    round(abs(a.longitude-b.shop_longitude),6) as pos_longitude_diff, 
  round(abs(a.latitude-b.shop_latitude),6) as pos_latitude_diff,
  round(sqrt(pow((a.longitude-b.shop_longitude)*111.3195,2)+pow((a.latitude-b.shop_latitude)*111.3195,2))*1000,2) as pos_distance_diff from
(select  row_id,mall_id,longitude,latitude from lzy_new_tongji_online ) as a inner join 
(select mall_id,shop_id as pred_shop, longitude as shop_longitude,latitude as shop_latitude from lzy_ant_tianchi_ccf_sl_shop_info) as b on a.mall_id=b.mall_id;


--wifi_std
drop table if exists lzy_new_shop_wifi_std_offlinewin;
create table lzy_new_shop_wifi_std_offlinewin as 
select bssid,shop_id,stddev(rssi) as bssid_rssi_std_inshop from lzy_new_tongji_wifi_offlinewin where row_id in (select row_id from lzy_new_notnull_offline) group by bssid,shop_id;

drop table if exists lzy_new_shop_wifi_std_onlinewin;
create table lzy_new_shop_wifi_std_onlinewin as 
select bssid,shop_id,stddev(rssi) as bssid_rssi_std_inshop from lzy_new_tongji_wifi_onlinewin where row_id in (select row_id from lzy_new_notnull_online) group by bssid,shop_id;

--wifi_rank_diff
drop table if exists lzy_new_feat_wifi_r_diff_offline;
create table lzy_new_feat_wifi_r_diff_offline as 
select t.*,t.rssi_rank-c.bssid_mean_rank_inshop as wifi_rank_diff from 
(select a.*,b.bssid,b.rssi_rank from (select * from lzy_new_samples_add_offline) as a left outer join (select row_id,bssid,rssi_rank from lzy_new_tongji_wifi_offline where row_id in (select row_id from lzy_new_notnull_offline)) as b on a.row_id=b.row_id) as t
inner join 
(select shop_id,bssid,avg(rssi_rank) as bssid_mean_rank_inshop from lzy_new_tongji_wifi_offlinewin  where row_id in (select row_id from lzy_new_notnull_offline) group by shop_id,bssid) as c
on t.shop_id=c.shop_id and t.bssid=c.bssid;

drop table if exists lzy_new_feat_wifi_r_diff_offline_;
create table lzy_new_feat_wifi_r_diff_offline_ as 
select row_id,shop_id,bssid,avg(wifi_rank_diff) as wifi_rank_diff from lzy_new_feat_wifi_r_diff_offline group by row_id,shop_id,bssid;
drop table if exists lzy_new_feat_wifi_r_diff_offline;
create table lzy_new_feat_wifi_r_diff_offline as select * from lzy_new_feat_wifi_r_diff_offline_;
drop table if exists lzy_new_feat_wifi_r_diff_offline_;
--test
drop table if exists lzy_new_feat_wifi_r_diff_online;
create table lzy_new_feat_wifi_r_diff_online as 
select t.*,t.rssi_rank-c.bssid_mean_rank_inshop as wifi_rank_diff from 
(select a.*,b.bssid,b.rssi_rank from (select * from lzy_new_samples_add_online) as a left outer join (select row_id,bssid,rssi_rank from lzy_new_tongji_wifi_online where row_id in (select row_id from lzy_new_notnull_online)) as b on a.row_id=b.row_id) as t
inner join 
(select shop_id,bssid,avg(rssi_rank) as bssid_mean_rank_inshop from lzy_new_tongji_wifi_onlinewin  where row_id in (select row_id from lzy_new_notnull_online) group by shop_id,bssid) as c
on t.shop_id=c.shop_id and t.bssid=c.bssid;

drop table if exists lzy_new_feat_wifi_r_diff_online_;
create table lzy_new_feat_wifi_r_diff_online_ as 
select row_id,shop_id,bssid,avg(wifi_rank_diff) as wifi_rank_diff from lzy_new_feat_wifi_r_diff_online group by row_id,shop_id,bssid;
drop table if exists lzy_new_feat_wifi_r_diff_online;
create table lzy_new_feat_wifi_r_diff_online as select * from lzy_new_feat_wifi_r_diff_online_;
drop table if exists lzy_new_feat_wifi_r_diff_online_;



drop table if exists lzy_new_offline_rowid;
create table lzy_new_offline_rowid as 
select row_id from lzy_new_tongji_offline;
