--单模型提交
drop table if exists lzy_new_pred_test_res_1;
create table lzy_new_pred_test_res_1 as 
select cast(row_id as string) as row_id,shop_id from
(select row_id,shop_id,row_number() over(partition by row_id order by prob desc) as rank_name from
  (select row_id,shop_id,1-prediction_score as prob from lzy_new_pred_test_prob_1  where prediction_result=0 
   union all 
   select row_id,shop_id,prediction_score as prob from lzy_new_pred_test_prob_1 where prediction_result=1) as c
) as t where t.rank_name=1;

drop table if exists ant_tianchi_ccf_sl_predict;
create table ant_tianchi_ccf_sl_predict as 
select * from lzy_new_pred_test_res_1;


--融合后提交
--bagging
drop table if exists lzy_new_esb_ps_wp1_bagging;
create table lzy_new_esb_ps_wp1_bagging as 
select cast(row_id as string) as row_id,shop_id,avg(prob) as prob from
(
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wp1_1
 union all 
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wp1_2
 union all 
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wp1_3
 union all 
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wp1_4
 union all 
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wp1_5
 union all
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wp1_1
 union all 
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wp1_2
 union all 
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wp1_3
 union all 
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wp1_4
 union all 
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wp1_5
 ) as c group by row_id,shop_id;

drop table if exists lzy_new_esb_ps_wp2_bagging;
create table lzy_new_esb_ps_wp2_bagging as 
select cast(row_id as string) as row_id,shop_id,avg(prob) as prob from
(
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wp2_1
 union all 
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wp2_2
 union all 
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wp2_3
 union all 
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wp2_4
 union all 
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wp2_5
 union all
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wp2_1
 union all 
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wp2_2
 union all 
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wp2_3
 union all 
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wp2_4
 union all 
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wp2_5
 ) as c group by row_id,shop_id;

--bagging
drop table if exists lzy_new_esb_ps_wpall_bagging;
create table lzy_new_esb_ps_wpall_bagging as 
select cast(row_id as string) as row_id,shop_id,avg(prob) as prob from
(
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wpall_1
 union all 
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wpall_2
 union all 
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wpall_3
 union all 
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wpall_4
 union all 
 select row_id,shop_id,prob from lzy_new_esb_nn_ps_wpall_5
 union all
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wpall_1
 union all 
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wpall_2
 union all 
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wpall_3
 union all 
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wpall_4
 union all 
 select row_id,shop_id,prob from lzy_new_esb_in_ps_wpall_5
 ) as c group by row_id,shop_id;

--final
drop table if exists ant_tianchi_ccf_sl_predict;
create table ant_tianchi_ccf_sl_predict as 
select cast(row_id as string) as row_id,shop_id from
(select row_id,shop_id,row_number() over(partition by row_id order by prob desc) as rank_name from
  (
  select row_id,shop_id,sum(prob) as prob from
  (
   --
   select row_id,shop_id,(10*prob) as prob from lzy_new_esb_ps_wp1_bagging
   union all 
   select row_id,shop_id,(6*prob) as prob from lzy_new_esb_xgb_wp1
   union all
   select row_id,shop_id,(4*prob) as prob from lzy_new_esb_in_gb_wp1_4
   union all
   select row_id,shop_id,(4*prob) as prob from lzy_new_esb_in_gb_wp1_5
   --
   union all 
   select row_id,shop_id,(18*prob) as prob from lzy_new_esb_ps_wpall_bagging
   --
   union all
   select row_id,shop_id,(12*prob) as prob from lzy_new_esb_ps_wp2_bagging
   ) as c
   group by row_id,shop_id
  ) as cc
) as t where t.rank_name=1;