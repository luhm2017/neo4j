--基础数据添加：产品分类，金额，是否命中黑名单

--一度关联变量
create table degree1_features(order_id_src string,cnt int) PARTITIONED BY ( title string);
create table degree2_features(order_id_src string,cnt int) PARTITIONED BY ( title string);

--数据准备
--================================================
--提取合同数据
create table temp_contract_data as 
select a.order_id from  fqz.fqz_knowledge_graph_data_external a 
where a.type = 'pass';

--根据一度取关联数据，增加时间日期
create table temp_degree1_relation_data as 
SELECT a.order_id_src,
a.apply_date_src ,
a.cert_no_src,
a.order_id_dst1, 
a.apply_date_dst1,
a.cert_no_dst1
FROM fqz.fqz_relation_degree1  a 
join temp_contract_data b on a.order_id_src = b.order_id
GROUP BY 
a.order_id_src,
apply_date_src ,
a.cert_no_src,
a.order_id_dst1, 
apply_date_dst1,
a.cert_no_dst1;

--添加源订单，根据时间范围扩展
create table temp_degree1_relation_data_src as 
select  
tab.order_id_src,tab.apply_date_src,tab.cert_no_src,
tab.order_id_src as order_id_dst1,tab.apply_date_src as apply_date_dst1, tab.cert_no_src as cert_no_dst1  from 
(select a.order_id_src,a.apply_date_src,a.cert_no_src from temp_degree1_relation_data a group by a.order_id_src,a.apply_date_src,a.cert_no_src) tab
union all 
select a.order_id_src,a.apply_date_src,a.cert_no_src,a.order_id_dst1,a.apply_date_dst1,a.cert_no_dst1
from temp_degree1_relation_data a;

--关联订单属性  ，增加关联订单号、时间
create table temp_degree1_relation_data_attribute as 
select 
a.order_id_src,
a.apply_date_src,
a.cert_no_src,
a.order_id_dst1,
a.apply_date_dst1,
--a.cert_no_dst1,
b.loan_pan as loan_pan_dst1,
b.cert_no as cert_no_dst1,
b.email as email_dst1,
b.mobile as mobile_dst1,
regexp_replace(b.comp_phone,'[^0-9]','') as comp_phone_dst1, 
b.emergency_contact_mobile as emergency_contact_mobile_dst1,
b.contact_mobile as contact_mobile_dst1,
b.device_id as device_id_dst1,
b.product_name as product_name_dst1,
case when b.product_name = '易分期' then 1 else 0 end as yfq_dst1,
case when b.product_name = '替你还' then 1 else 0 end as tnh_dst1,
case when b.type = 'pass' then 1 else 0 end as pass_contract_dst1,
case when b.performance = 'q_refuse' then 1 else 0 end as q_refuse_dst1,
case when b.current_due_day <=0 then 1 else 0 end as current_overdue0_dst1,
case when b.current_due_day >3 then 1 else 0 end as current_overdue3_dst1,
case when b.current_due_day >30 then 1 else 0 end as current_overdue30_dst1,
case when b.history_due_day <=0 then 1 else 0 end as history_overdue0_dst1,
case when b.history_due_day >3 then 1 else 0 end as history_overdue3_dst1,
case when b.history_due_day >30 then 1 else 0 end as history_overdue30_dst1
from temp_degree1_relation_data_src a 
join fqz.fqz_knowledge_graph_data_external b on a.order_id_dst1 = b.order_id;

--提取黑名单数据
--人工定性加黑与行为定性加黑数据合并
create table fqz_black_attribute_data as  --278124
select b.CONTENT, 'black_cid' as type from fqz.fqz_community_black_data b 
where b.type = 1 and b.origin = 0 and b.content_type = 3 
union all
select a.cert_no as content, 'black_cid' as type from fqz.fqz_fraud_contract_data_with_attribute a --25849
union all
select b.CONTENT, 'black_mobile' as type from fqz.fqz_community_black_data b 
where b.type = 1 and b.origin = 0 and b.content_type = 1 
union all
select a.mobile as content, 'black_mobile' as type from fqz.fqz_fraud_contract_data_with_attribute a 
union all
select b.CONTENT, 'black_bankcard' as type from fqz.fqz_community_black_data b 
where b.type = 1 and b.origin = 0 and b.content_type = 2 
union all
select a.loan_pan as content, 'black_bankcard' as type from fqz.fqz_fraud_contract_data_with_attribute a 
union all
select b.CONTENT, 'black_imei' as type from fqz.fqz_community_black_data b 
where b.type = 1 and b.origin = 0 and b.content_type = 8 
union all
select a.device_id as content, 'black_imei' as type from fqz.fqz_fraud_contract_data_with_attribute a 
union all
select b.CONTENT, 'black_email' as type from fqz.fqz_community_black_data b 
where b.type = 1 and b.origin = 0 and b.content_type = 5 
union all
select a.email as content, 'black_email' as type from fqz.fqz_fraud_contract_data_with_attribute a
union all
select regexp_replace(b.CONTENT,'[^0-9]','') as content, 'black_company_phone' as type from fqz.fqz_community_black_data b 
where b.type = 1 and b.origin = 0 and b.content_type = 11
union all --黑合同抽取 
select a.order_id as content, 'black_contract' as type from fqz.fqz_fraud_contract_data_with_attribute a
union all
select a.order_id as content,'black_contract' as type  from (
select b.apply_id from fqz.fqz_community_black_data b 
where b.type = 1 and b.origin = 0 and b.apply_id <> '' group by b.apply_id) tab
join fqz.fqz_knowledge_graph_data_external a on tab.apply_id = a.contract_no;
--数据除重
insert overwrite table fqz_black_attribute_data
select content,type from fqz_black_attribute_data group by content,type;

--================================================================================================

--指标统计
--================================================================================================
--订单合同表现指标
FROM (select * from temp_degree1_relation_data_attribute where order_id_src <> order_id_dst1 ) a
INSERT INTO degree1_features partition (title='order_cnt')  --一度含自身订单数量，
SELECT a.order_id_src, count(distinct a.order_id_dst1) cnt group by  a.order_id_src 
INSERT INTO degree1_features partition (title='pass_contract_cnt')   --一度含自身通过合同数量
SELECT a.order_id_src, sum(a.pass_contract_dst1) cnt  group by  a.order_id_src
INSERT INTO degree1_features partition (title='q_order_cnt')   --一度含自身Q标订单数量
SELECT a.order_id_src, sum(a.q_refuse_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='current_overdue0_contract_cnt')   --一度含自身当前无逾期合同数量
select a.order_id_src, sum(a.current_overdue0_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='current_overdue3_contract_cnt')   --一度含自身当前3+合同数量
select a.order_id_src, sum(a.current_overdue3_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='current_overdue30_contract_cnt')   --一度含自身当前30+合同数量
select a.order_id_src, sum(a.current_overdue30_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='history_overdue0_contract_cnt')   --一度含自身历史无逾期合同数量
select a.order_id_src, sum(a.history_overdue0_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='history_overdue3_contract_cnt')   --一度含自身历史3+合同数量
select a.order_id_src, sum(a.history_overdue3_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='history_overdue30_contract_cnt')  --一度含自身历史30+合同数量
select a.order_id_src, sum(a.history_overdue30_dst1) cnt group by a.order_id_src;

--关联边指标，区别于订单合同表现指标（包含原始订单）
FROM (select * from temp_degree1_relation_data_attribute) a
INSERT INTO degree1_features partition (title='cid_cnt')   --一度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt  group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt')  --一度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt')  --一度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt')  --一度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt')  --一度含自身Email数量
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt')  --一度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt')  --一度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt')  --一度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src 
--申请产品指标
INSERT INTO degree1_features partition (title='product_cnt')  --一度含自身总产品数
select a.order_id_src, count(distinct a.product_name_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='yfq_cnt')  --一度含自身yfq数量
select a.order_id_src, sum(a.yfq_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='tnh_cnt')  --一度含自身tnh数量
select a.order_id_src, sum(a.tnh_dst1) cnt group by a.order_id_src;  

--关联边小图指标，按时间1\3\7\30切片
--===================================================================================================
FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 1) a
INSERT INTO degree1_features partition (title='cid_cnt1')   --一度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt1')  --一度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt1')  --一度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt1')  --一度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt1')  --一度含自身Email数量
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt1')  --一度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt1')  --一度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt1')  --一度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;

FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 7) a
INSERT INTO degree1_features partition (title='cid_cnt7')   --一度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt7')  --一度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt7')  --一度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt7')  --一度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt7')  --一度含自身Email数量
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt7')  --一度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt7')  --一度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt7')  --一度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;

FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 30) a
INSERT INTO degree1_features partition (title='cid_cnt30')   --一度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt30')  --一度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt30')  --一度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt30')  --一度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt30')  --一度含自身Email数量
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt30')  --一度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt30')  --一度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt30')  --一度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;
--===================================================================================================

--关联边命中黑指标
INSERT INTO degree1_features partition (title='black_cid_cnt')   --一度含自身黑身份证数量
select a.order_id_src,count(distinct a.cert_no_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.cert_no_dst1 = b.CONTENT
where  b.type = 'black_cid' GROUP BY a.order_id_src ;
INSERT INTO degree1_features partition (title='black_mobile_cnt')   --一度含自身黑手机数量
select a.order_id_src,count(distinct a.mobile_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.mobile_dst1 = b.CONTENT
where  b.type = 'black_mobile' GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_bankcard_cnt')   --一度含自身黑银行卡数量
select a.order_id_src,count(distinct a.loan_pan_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.loan_pan_dst1 = b.CONTENT
where  b.type = 'black_bankcard' GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_imei_cnt')   --一度含自身黑IMEI数量
select a.order_id_src,count(distinct a.device_id_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.device_id_dst1 = b.CONTENT
where  b.type = 'black_imei' GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_email_cnt')   --一度含自身黑Email数量
select a.order_id_src,count(distinct a.email_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.email_dst1 = b.CONTENT
where  b.type = 'black_email' GROUP BY a.order_id_src ;
INSERT INTO degree1_features partition (title='black_company_phone_cnt')   --一度含自身黑单电数量
select a.order_id_src,count(distinct a.comp_phone_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.comp_phone_dst1 = b.CONTENT
where  b.type =  'black_company_phone' GROUP BY a.order_id_src; --单电是否正则化处理  
INSERT INTO degree1_features partition (title='black_contract_cnt')   --一度含自身黑合同数量
select a.order_id_src,count(distinct a.order_id_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.order_id_dst1 = b.CONTENT
where b.type = 'black_contract' GROUP BY a.order_id_src;


--================================================================================================================
--排除自身的指标
FROM (select * from temp_degree1_relation_data_attribute where order_id_src <> order_id_dst1 and cert_no_src <> cert_no_dst1) a
INSERT INTO degree1_features partition (title='order_cnt_exception_self')  --一度排除自身订单数量，
SELECT a.order_id_src, count(distinct a.order_id_dst1) cnt group by  a.order_id_src 
INSERT INTO degree1_features partition (title='pass_contract_cnt_exception_self')   --一度排除自身通过合同数量
SELECT a.order_id_src, sum(a.pass_contract_dst1) cnt  group by  a.order_id_src
INSERT INTO degree1_features partition (title='q_order_cnt_exception_self')   --一度排除自身Q标订单数量
SELECT a.order_id_src, sum(a.q_refuse_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='current_overdue0_contract_cnt_exception_self')   --一度排除自身当前无逾期合同数量
select a.order_id_src, sum(a.current_overdue0_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='current_overdue3_contract_cnt_exception_self')   --一度排除自身当前3+合同数量
select a.order_id_src, sum(a.current_overdue3_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='current_overdue30_contract_cnt_exception_self')   --一度排除自身当前30+合同数量
select a.order_id_src, sum(a.current_overdue30_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='history_overdue0_contract_cnt_exception_self')   --一度排除自身历史无逾期合同数量
select a.order_id_src, sum(a.history_overdue0_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='history_overdue3_contract_cnt_exception_self')   --一度排除自身历史3+合同数量
select a.order_id_src, sum(a.history_overdue3_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='history_overdue30_contract_cnt_exception_self')  --一度排除自身历史30+合同数量
select a.order_id_src, sum(a.history_overdue30_dst1) cnt group by a.order_id_src;

--关联边指标，区别于订单合同表现指标（包含原始订单）
FROM (select * from temp_degree1_relation_data_attribute where and cert_no_src <> cert_no_dst1) a
INSERT INTO degree1_features partition (title='cid_cnt_exception_self')   --一度排除自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt  group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt_exception_self')  --一度排除自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt_exception_self')  --一度排除自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt_exception_self')  --一度排除自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt_exception_self')  --一度排除自身Email数量
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt_exception_self')  --一度排除自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt_exception_self')  --一度排除自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt_exception_self')  --一度排除自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src 
--申请产品指标
INSERT INTO degree1_features partition (title='product_cnt_exception_self')  --一度排除自身总产品数
select a.order_id_src, count(distinct a.product_name_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='yfq_cnt_exception_self')  --一度排除自身yfq数量
select a.order_id_src, sum(a.yfq_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='tnh_cnt_exception_self')  --一度排除自身tnh数量
select a.order_id_src, sum(a.tnh_dst1) cnt group by a.order_id_src ; 

--关联边小图指标，按时间1\3\7\30切片
--===================================================================================================
FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 1 and cert_no_src <> cert_no_dst1) a
INSERT INTO degree1_features partition (title='cid_cnt1_exception_self')   --一度排除自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt1_exception_self')  --一度排除自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt1_exception_self')  --一度排除自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt1_exception_self')  --一度排除自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt1_exception_self')  --一度排除自身Email数量
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt1_exception_self')  --一度排除自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt1_exception_self')  --一度排除自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt1_exception_self')  --一度排除自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;

FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 3 and cert_no_src <> cert_no_dst1) a
INSERT INTO degree1_features partition (title='cid_cnt3_exception_self')   --一度排除自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt3_exception_self')  --一度排除自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt3_exception_self')  --一度排除自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt3_exception_self')  --一度排除自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt3_exception_self')  --一度排除自身Email数量
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt3_exception_self')  --一度排除自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt3_exception_self')  --一度排除自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt3_exception_self')  --一度排除自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;

FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 7 and cert_no_src <> cert_no_dst1) a
INSERT INTO degree1_features partition (title='cid_cnt7_exception_self')   --一度排除自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt7_exception_self')  --一度排除自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt7_exception_self')  --一度排除自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt7_exception_self')  --一度排除自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt7_exception_self')  --一度排除自身Email数量
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt7_exception_self')  --一度排除自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt7_exception_self')  --一度排除自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt7_exception_self')  --一度排除自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;

FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 30 and cert_no_src <> cert_no_dst1) a
INSERT INTO degree1_features partition (title='cid_cnt30_exception_self')   --一度排除自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt30_exception_self')  --一度排除自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt30_exception_self')  --一度排除自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt30_exception_self')  --一度排除自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt30_exception_self')  --一度排除自身Email数量
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt30_exception_self')  --一度排除自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt30_exception_self')  --一度排除自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt30_exception_self')  --一度排除自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;
--===================================================================================================

--关联边命中黑指标
INSERT INTO degree1_features partition (title='black_cid_cnt_exception_self')   --一度排除自身黑身份证数量
select a.order_id_src,count(distinct a.cert_no_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.cert_no_dst1 = b.CONTENT
where  b.type = 'black_cid' and a.cert_no_src <> a.cert_no_dst1  GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_mobile_cnt_exception_self')   --一度排除自身黑手机数量
select a.order_id_src,count(distinct a.mobile_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.mobile_dst1 = b.CONTENT
where  b.type = 'black_mobile' and a.cert_no_src <> a.cert_no_dst1  GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_bankcard_cnt_exception_self')   --一度排除自身黑银行卡数量
select a.order_id_src,count(distinct a.loan_pan_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.loan_pan_dst1 = b.CONTENT
where  b.type = 'black_bankcard' and a.cert_no_src <> a.cert_no_dst1  GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_imei_cnt_exception_self')   --一度排除自身黑IMEI数量
select a.order_id_src,count(distinct a.device_id_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.device_id_dst1 = b.CONTENT
where  b.type = 'black_imei' and a.cert_no_src <> a.cert_no_dst1  GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_email_cnt_exception_self')   --一度排除自身黑Email数量
select a.order_id_src,count(distinct a.email_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.email_dst1 = b.CONTENT
where  b.type = 'black_email' and a.cert_no_src <> a.cert_no_dst1  GROUP BY a.order_id_src; 
INSERT INTO degree1_features partition (title='black_company_phone_cnt_exception_self')   --一度排除自身黑单电数量
select a.order_id_src,count(distinct a.comp_phone_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.comp_phone_dst1 = b.CONTENT
where  b.type =  'black_company_phone' and a.cert_no_src <> a.cert_no_dst1   GROUP BY a.order_id_src; --单电是否正则化处理  
INSERT INTO degree1_features partition (title='black_contract_cnt_exception_self')   --一度排除自身黑合同数量
select a.order_id_src,count(distinct a.order_id_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.order_id_dst1 = b.CONTENT
where b.type = 'black_contract' and a.cert_no_src <> a.cert_no_dst1  GROUP BY a.order_id_src;


--====================================================================================================
--二度关联
--根据二度取关联数据，增加时间日期 , 通配符统一替换关联边
--数据量太大，进行拆分
create table temp_degree2_relation_data as
SELECT a.order_id_src,
a.apply_date_src ,
a.cert_no_src,
a.order_id_dst2,
a.apply_date_dst2,
a.cert_no_dst2
FROM fqz.fqz_relation_degree2  a
join temp_contract_data b on a.order_id_src = b.order_id
--where edg_type_src1 = '$edge1' and edg_type_src2 = '$edge2'
GROUP BY
a.order_id_src,
a.apply_date_src,
a.cert_no_src,
a.order_id_dst2,
a.apply_date_dst2,
a.cert_no_dst2;

--添加源订单，根据时间范围扩展
create table temp_degree2_relation_data_src as
select
tab.order_id_src,tab.apply_date_src,tab.cert_no_src,
tab.order_id_src as order_id_dst2,tab.apply_date_src as apply_date_dst2, tab.cert_no_src as cert_no_dst2  from
(select a.order_id_src,a.apply_date_src,a.cert_no_src from temp_degree2_relation_data a group by a.order_id_src,a.apply_date_src,cert_no_src) tab
union all
select a.order_id_src,a.apply_date_src,a.cert_no_src,a.order_id_dst2,a.apply_date_dst2,a.cert_no_dst2
from temp_degree2_relation_data a;

--关联订单属性  ，增加关联订单号、时间
create table temp_degree2_relation_data_attribute as
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
from temp_degree2_relation_data_src a
join fqz.fqz_knowledge_graph_data_external b on a.order_id_dst2 = b.order_id;

--================================================================================================

--指标统计
--================================================================================================
--订单合同表现指标
FROM (select * from temp_degree2_relation_data_attribute where order_id_src <> order_id_dst2 ) a
INSERT INTO degree2_features partition (title='order_cnt')  --二度含自身订单数量，
SELECT a.order_id_src, count(distinct a.order_id_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='pass_contract_cnt')   --二度含自身通过合同数量
SELECT a.order_id_src, sum(a.pass_contract_dst2) cnt  group by  a.order_id_src
INSERT INTO degree2_features partition (title='q_order_cnt')   --二度含自身Q标订单数量
SELECT a.order_id_src, sum(a.q_refuse_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='current_overdue0_contract_cnt')   --二度含自身当前无逾期合同数量
select a.order_id_src, sum(a.current_overdue0_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='current_overdue3_contract_cnt')   --二度含自身当前3+合同数量
select a.order_id_src, sum(a.current_overdue3_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='current_overdue30_contract_cnt')   --二度含自身当前30+合同数量
select a.order_id_src, sum(a.current_overdue30_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='history_overdue0_contract_cnt')   --二度含自身历史无逾期合同数量
select a.order_id_src, sum(a.history_overdue0_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='history_overdue3_contract_cnt')   --二度含自身历史3+合同数量
select a.order_id_src, sum(a.history_overdue3_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='history_overdue30_contract_cnt')  --二度含自身历史30+合同数量
select a.order_id_src, sum(a.history_overdue30_dst2) cnt group by a.order_id_src;

--关联边指标，区别于订单合同表现指标（包含原始订单）
FROM (select * from temp_degree2_relation_data_attribute) a
INSERT INTO degree2_features partition (title='cid_cnt')   --二度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt  group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt')  --二度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='bankcard_cnt')  --二度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt')  --二度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt')  --二度含自身Email数量
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt')  --二度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt')  --二度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt')  --二度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src
--申请产品指标
INSERT INTO degree2_features partition (title='product_cnt')  --二度含自身总产品数
select a.order_id_src, count(distinct a.product_name_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='yfq_cnt')  --二度含自身yfq数量
select a.order_id_src, sum(a.yfq_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='tnh_cnt')  --二度含自身tnh数量
select a.order_id_src, sum(a.tnh_dst2) cnt group by a.order_id_src;

--关联边小图指标，按时间1\3\7\30切片
--===================================================================================================
FROM (select * from temp_degree2_relation_data_attribute where datediff(apply_date_src,apply_date_dst2) <= 1) a
INSERT INTO degree2_features partition (title='cid_cnt1')   --二度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt1')  --二度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='bankcard_cnt1')  --二度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt1')  --二度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt1')  --二度含自身Email数量
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt1')  --二度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt1')  --二度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt1')  --二度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

FROM (select * from temp_degree2_relation_data_attribute where datediff(apply_date_src,apply_date_dst2) <= 3) a
INSERT INTO degree2_features partition (title='cid_cnt3')   --二度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt3')  --二度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='bankcard_cnt3')  --二度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt3')  --二度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt3')  --二度含自身Email数量
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt3')  --二度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt3')  --二度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt3')  --二度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

FROM (select * from temp_degree2_relation_data_attribute where datediff(apply_date_src,apply_date_dst2) <= 7) a
INSERT INTO degree2_features partition (title='cid_cnt7')   --二度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt  group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt7')  --二度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='bankcard_cnt7')  --二度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt7')  --二度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt7')  --二度含自身Email数量
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt7')  --二度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt7')  --二度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt7')  --二度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

FROM (select * from temp_degree2_relation_data_attribute where datediff(apply_date_src,apply_date_dst2) <= 30) a
INSERT INTO degree2_features partition (title='cid_cnt30')   --二度含自身身份证数量
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt30')  --二度含自身手机号数量
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='bankcard_cnt30')  --二度含自身银行卡数量
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt30')  --二度含自身IMEI数量
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt30')  --二度含自身Email数量
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt30')  --二度含自身联系人手机数量
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt30')  --二度含自身紧联手机数量
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt30')  --二度含自身单电数量
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

--关联边命中黑指标
INSERT INTO degree2_features partition (title='black_cid_cnt')   --二度含自身黑身份证数量
select a.order_id_src,count(distinct a.cert_no_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.cert_no_dst2 = b.CONTENT
where  b.type = 'black_cid' group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_mobile_cnt')   --二度含自身黑手机数量
select a.order_id_src,count(distinct a.mobile_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.mobile_dst2 = b.CONTENT
where  b.type = 'black_mobile' group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_bankcard_cnt')   --二度含自身黑银行卡数量
select a.order_id_src,count(distinct a.loan_pan_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.loan_pan_dst2 = b.CONTENT
where  b.type = 'black_bankcard' group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_imei_cnt')   --二度含自身黑IMEI数量
select a.order_id_src,count(distinct a.device_id_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.device_id_dst2 = b.CONTENT
where  b.type = 'black_imei' group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_email_cnt')   --二度含自身黑Email数量
select a.order_id_src,count(distinct a.email_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.email_dst2 = b.CONTENT
where  b.type = 'black_email'  group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_company_phone_cnt')   --二度含自身黑单电数量
select a.order_id_src,count(distinct a.comp_phone_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.comp_phone_dst2 = b.CONTENT
where  b.type =  'black_company_phone' group by a.order_id_src; --单电是否正则化处理
INSERT INTO degree2_features partition (title='black_contract_cnt')   --二度含自身黑合同数量
select a.order_id_src,count(distinct a.order_id_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.order_id_dst2 = b.CONTENT
where b.type = 'black_contract' group by a.order_id_src;

--反欺诈共用类变量
--==============================================================================
--先基于全量申请订单，清洗人的数据

--基于一度圈构造共用类变量
