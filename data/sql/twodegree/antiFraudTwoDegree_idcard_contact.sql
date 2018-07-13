use knowledge_graph;

--根据二度取关联数据，增加时间日期 , 通配符统一替换关联边
create table temp_degree2_relation_data_idcard_contact as 
SELECT a.order_id_src,
a.apply_date_src ,
a.cert_no_src,
a.order_id_dst2, 
a.apply_date_dst2,
a.cert_no_dst2
FROM fqz.fqz_relation_degree2  a 
join temp_contract_data b on a.order_id_src = b.order_id
where edg_type_src1 = 'IDCARD' and edg_type_src2 = 'CONTACT'
--and a.cert_no_src <> a.cert_no_dst2
GROUP BY 
a.order_id_src,
apply_date_src,
a.cert_no_src,
a.order_id_dst2, 
apply_date_dst2,
a.cert_no_dst2;

--添加源订单，根据时间范围扩展
create table temp_degree2_relation_data_src_idcard_contact as 
select  
tab.order_id_src,tab.apply_date_src,tab.cert_no_src,
tab.order_id_src as order_id_dst2,tab.apply_date_src as apply_date_dst2, tab.cert_no_src as cert_no_dst2  from 
(select a.order_id_src,a.apply_date_src,a.cert_no_src from temp_degree2_relation_data_idcard_contact a group by a.order_id_src,a.apply_date_src,a.cert_no_src) tab
union all 
select a.order_id_src,a.apply_date_src,a.cert_no_src,a.order_id_dst2,a.apply_date_dst2,a.cert_no_dst2
from temp_degree2_relation_data_idcard_contact a;

--关联订单属性  ，增加关联订单号、时间
create table temp_degree2_relation_data_attribute_idcard_contact as 
select 
a.order_id_src,
a.apply_date_src,
a.cert_no_src,
a.order_id_dst2,
a.apply_date_dst2,
--a.cert_no_dst2,
b.loan_pan as loan_pan_dst2,
b.cert_no as cert_no_dst2,
b.email as email_dst2,
b.mobile as mobile_dst2,
regexp_replace(b.comp_phone,'[^0-9]','') as comp_phone_dst2,
b.emergency_contact_mobile as emergency_contact_mobile_dst2,
b.contact_mobile as contact_mobile_dst2,
b.device_id as device_id_dst2,
b.product_name as product_name_dst2,
case when b.product_name = '易分期' then 1 else 0 end as yfq_dst2,
case when b.product_name = '替你还' then 1 else 0 end as tnh_dst2,
case when b.type = 'pass' then 1 else 0 end as pass_contract_dst2,
case when b.performance = 'q_refuse' then 1 else 0 end as q_refuse_dst2,
case when b.current_due_day <=0 then 1 else 0 end as current_overdue0_dst2,
case when b.current_due_day >3 then 1 else 0 end as current_overdue3_dst2,
case when b.current_due_day >30 then 1 else 0 end as current_overdue30_dst2,
case when b.history_due_day <=0 then 1 else 0 end as history_overdue0_dst2,
case when b.history_due_day >3 then 1 else 0 end as history_overdue3_dst2,
case when b.history_due_day >30 then 1 else 0 end as history_overdue30_dst2
from temp_degree2_relation_data_src_idcard_contact a 
join fqz.fqz_knowledge_graph_data_external b on a.order_id_dst2 = b.order_id;

--================================================================================================

--指标统计
--================================================================================================
--订单合同表现指标
FROM (select * from temp_degree2_relation_data_attribute_idcard_contact where order_id_src <> order_id_dst2 ) a
INSERT OVERWRITE TABLE degree2_features partition (title='order_cnt_idcard_contact')  --二度含自身订单数量，
SELECT a.order_id_src, count(distinct a.order_id_dst2) cnt group by  a.order_id_src 
INSERT OVERWRITE TABLE degree2_features partition (title='pass_contract_cnt_idcard_contact')   --二度含自身通过合同数量
SELECT a.order_id_src, sum(a.pass_contract_dst2) cnt  group by  a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='q_order_cnt_idcard_contact')   --二度含自身Q标订单数量
SELECT a.order_id_src, sum(a.q_refuse_dst2) cnt group by  a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='current_overdue0_contract_cnt_idcard_contact')   --二度含自身当前无逾期合同数量
select a.order_id_src, sum(a.current_overdue0_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='current_overdue3_contract_cnt_idcard_contact')   --二度含自身当前3+合同数量
select a.order_id_src, sum(a.current_overdue3_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='current_overdue30_contract_cnt_idcard_contact')   --二度含自身当前30+合同数量
select a.order_id_src, sum(a.current_overdue30_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='history_overdue0_contract_cnt_idcard_contact')   --二度含自身历史无逾期合同数量
select a.order_id_src, sum(a.history_overdue0_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='history_overdue3_contract_cnt_idcard_contact')   --二度含自身历史3+合同数量
select a.order_id_src, sum(a.history_overdue3_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='history_overdue30_contract_cnt_idcard_contact')  --二度含自身历史30+合同数量
select a.order_id_src, sum(a.history_overdue30_dst2) cnt group by a.order_id_src;

--关联边指标，区别于订单合同表现指标（包含原始订单）
FROM (select * from temp_degree2_relation_data_attribute_idcard_contact) a
INSERT OVERWRITE TABLE degree2_features partition (title='cid_cnt_idcard_contact')   --二度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt  group by  a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='mobile_cnt_idcard_contact')  --二度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src 
INSERT OVERWRITE TABLE degree2_features partition (title='bankcard_cnt_idcard_contact')  --二度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='imei_cnt_idcard_contact')  --二度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='email_cnt_idcard_contact')  --二度含自身Email数量
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='contact_mobile_cnt_idcard_contact')  --二度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='emergency_mobile_cnt_idcard_contact')  --二度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='company_phone_cnt_idcard_contact')  --二度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src
--申请产品指标
INSERT OVERWRITE TABLE degree2_features partition (title='product_cnt_idcard_contact')  --二度含自身总产品数
select a.order_id_src, count(distinct a.product_name_dst2) cnt group by a.order_id_src 
INSERT OVERWRITE TABLE degree2_features partition (title='yfq_cnt_idcard_contact')  --二度含自身yfq数量
select a.order_id_src, sum(a.yfq_dst2) cnt group by a.order_id_src 
INSERT OVERWRITE TABLE degree2_features partition (title='tnh_cnt_idcard_contact')  --二度含自身tnh数量
select a.order_id_src, sum(a.tnh_dst2) cnt group by a.order_id_src  ;

--关联边小图指标，按时间1\3\7\30切片
--===================================================================================================
FROM (select * from temp_degree2_relation_data_attribute_idcard_contact where datediff(apply_date_src,apply_date_dst2) <= 1) a
INSERT OVERWRITE TABLE degree2_features partition (title='cid_cnt1_idcard_contact')   --二度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt group by  a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='mobile_cnt1_idcard_contact')  --二度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src 
INSERT OVERWRITE TABLE degree2_features partition (title='bankcard_cnt1_idcard_contact')  --二度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='imei_cnt1_idcard_contact')  --二度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='email_cnt1_idcard_contact')  --二度含自身Email数量
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='contact_mobile_cnt1_idcard_contact')  --二度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='emergency_mobile_cnt1_idcard_contact')  --二度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='company_phone_cnt1_idcard_contact')  --二度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

FROM (select * from temp_degree2_relation_data_attribute_idcard_contact where datediff(apply_date_src,apply_date_dst2) <= 3) a
INSERT OVERWRITE TABLE degree2_features partition (title='cid_cnt3_idcard_contact')   --二度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt group by  a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='mobile_cnt3_idcard_contact')  --二度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src 
INSERT OVERWRITE TABLE degree2_features partition (title='bankcard_cnt3_idcard_contact')  --二度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='imei_cnt3_idcard_contact')  --二度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='email_cnt3_idcard_contact')  --二度含自身Email数量
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='contact_mobile_cnt3_idcard_contact')  --二度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='emergency_mobile_cnt3_idcard_contact')  --二度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='company_phone_cnt3_idcard_contact')  --二度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

FROM (select * from temp_degree2_relation_data_attribute_idcard_contact where datediff(apply_date_src,apply_date_dst2) <= 7) a
INSERT OVERWRITE TABLE degree2_features partition (title='cid_cnt7_idcard_contact')   --二度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt  group by  a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='mobile_cnt7_idcard_contact')  --二度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src 
INSERT OVERWRITE TABLE degree2_features partition (title='bankcard_cnt7_idcard_contact')  --二度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='imei_cnt7_idcard_contact')  --二度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='email_cnt7_idcard_contact')  --二度含自身Email数量
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='contact_mobile_cnt7_idcard_contact')  --二度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='emergency_mobile_cnt7_idcard_contact')  --二度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='company_phone_cnt7_idcard_contact')  --二度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

FROM (select * from temp_degree2_relation_data_attribute_idcard_contact where datediff(apply_date_src,apply_date_dst2) <= 30) a
INSERT OVERWRITE TABLE degree2_features partition (title='cid_cnt30_idcard_contact')   --二度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt group by  a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='mobile_cnt30_idcard_contact')  --二度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src 
INSERT OVERWRITE TABLE degree2_features partition (title='bankcard_cnt30_idcard_contact')  --二度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='imei_cnt30_idcard_contact')  --二度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='email_cnt30_idcard_contact')  --二度含自身Email数量
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='contact_mobile_cnt30_idcard_contact')  --二度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='emergency_mobile_cnt30_idcard_contact')  --二度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT OVERWRITE TABLE degree2_features partition (title='company_phone_cnt30_idcard_contact')  --二度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

--关联边命中黑指标
INSERT OVERWRITE TABLE degree2_features partition (title='black_cid_cnt_idcard_contact')   --二度含自身黑身份证数量
select a.order_id_src,count(distinct a.cert_no_dst2) as cnt from temp_degree2_relation_data_attribute_idcard_contact a 
join fqz_black_attribute_data b on a.cert_no_dst2 = b.CONTENT
where  b.type = 'black_cid' group by a.order_id_src;
INSERT OVERWRITE TABLE degree2_features partition (title='black_mobile_cnt_idcard_contact')   --二度含自身黑手机数量
select a.order_id_src,count(distinct a.mobile_dst2) as cnt from temp_degree2_relation_data_attribute_idcard_contact a 
join fqz_black_attribute_data b on a.mobile_dst2 = b.CONTENT
where  b.type = 'black_mobile' group by a.order_id_src;
INSERT OVERWRITE TABLE degree2_features partition (title='black_bankcard_cnt_idcard_contact')   --二度含自身黑银行卡数量
select a.order_id_src,count(distinct a.loan_pan_dst2) as cnt from temp_degree2_relation_data_attribute_idcard_contact a 
join fqz_black_attribute_data b on a.loan_pan_dst2 = b.CONTENT
where  b.type = 'black_bankcard' group by a.order_id_src;
INSERT OVERWRITE TABLE degree2_features partition (title='black_imei_cnt_idcard_contact')   --二度含自身黑IMEI数量
select a.order_id_src,count(distinct a.device_id_dst2) as cnt from temp_degree2_relation_data_attribute_idcard_contact a 
join fqz_black_attribute_data b on a.device_id_dst2 = b.CONTENT
where  b.type = 'black_imei' group by a.order_id_src;
INSERT OVERWRITE TABLE degree2_features partition (title='black_email_cnt_idcard_contact')   --二度含自身黑Email数量
select a.order_id_src,count(distinct a.email_dst2) as cnt from temp_degree2_relation_data_attribute_idcard_contact a 
join fqz_black_attribute_data b on a.email_dst2 = b.CONTENT
where  b.type = 'black_email'  group by a.order_id_src;
INSERT OVERWRITE TABLE degree2_features partition (title='black_company_phone_cnt_idcard_contact')   --二度含自身黑单电数量
select a.order_id_src,count(distinct a.comp_phone_dst2) as cnt from temp_degree2_relation_data_attribute_idcard_contact a 
join fqz_black_attribute_data b on a.comp_phone_dst2 = b.CONTENT
where  b.type =  'black_company_phone' group by a.order_id_src; --单电是否正则化处理  
INSERT OVERWRITE TABLE degree2_features partition (title='black_contract_cnt_idcard_contact')   --二度含自身黑合同数量
select a.order_id_src,count(distinct a.order_id_dst2) as cnt from temp_degree2_relation_data_attribute_idcard_contact a 
join fqz_black_attribute_data b on a.order_id_dst2 = b.CONTENT
where b.type = 'black_contract' group by a.order_id_src;