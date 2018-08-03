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

--===========================================================
--合并特征变量
create table fqz_knowledge_features as
select
c.label,
a.order_id_src,
c.apply_time
,a.bankcard_cnt1 as degree1_bankcard_cnt1
,a.bankcard_cnt1_bankcard as degree1_bankcard_cnt1_bankcard
,a.bankcard_cnt1_companyphone as degree1_bankcard_cnt1_companyphone
,a.bankcard_cnt1_exception_self as degree1_bankcard_cnt1_exception_self
,a.bankcard_cnt1_exception_self_bankcard as degree1_bankcard_cnt1_exception_self_bankcard
,a.bankcard_cnt1_exception_self_companyphone as degree1_bankcard_cnt1_exception_self_companyphone
,a.bankcard_cnt30 as degree1_bankcard_cnt30
,a.bankcard_cnt30_bankcard as degree1_bankcard_cnt30_bankcard
,a.bankcard_cnt30_companyphone as degree1_bankcard_cnt30_companyphone
,a.bankcard_cnt30_exception_self as degree1_bankcard_cnt30_exception_self
,a.bankcard_cnt30_exception_self_bankcard as degree1_bankcard_cnt30_exception_self_bankcard
,a.bankcard_cnt30_exception_self_companyphone as degree1_bankcard_cnt30_exception_self_companyphone
,a.bankcard_cnt3_bankcard as degree1_bankcard_cnt3_bankcard
,a.bankcard_cnt3_companyphone as degree1_bankcard_cnt3_companyphone
,a.bankcard_cnt3_exception_self as degree1_bankcard_cnt3_exception_self
,a.bankcard_cnt7 as degree1_bankcard_cnt7
,a.bankcard_cnt7_bankcard as degree1_bankcard_cnt7_bankcard
,a.bankcard_cnt7_companyphone as degree1_bankcard_cnt7_companyphone
,a.bankcard_cnt7_exception_self as degree1_bankcard_cnt7_exception_self
,a.bankcard_cnt7_exception_self_bankcard as degree1_bankcard_cnt7_exception_self_bankcard
,a.bankcard_cnt7_exception_self_companyphone as degree1_bankcard_cnt7_exception_self_companyphone
,a.bankcard_cnt_bankcard as degree1_bankcard_cnt_bankcard
,a.bankcard_cnt_companyphone as degree1_bankcard_cnt_companyphone
,a.bankcard_cnt_exception_self_bankcard as degree1_bankcard_cnt_exception_self_bankcard
,a.bankcard_cnt_exception_self_companyphone as degree1_bankcard_cnt_exception_self_companyphone
,a.black_bankcard_cnt as degree1_black_bankcard_cnt
,a.black_bankcard_cnt_bankcard as degree1_black_bankcard_cnt_bankcard
,a.black_bankcard_cnt_companyphone as degree1_black_bankcard_cnt_companyphone
,a.black_bankcard_cnt_exception_self as degree1_black_bankcard_cnt_exception_self
,a.black_bankcard_cnt_exception_self_bankcard as degree1_black_bankcard_cnt_exception_self_bankcard
,a.black_bankcard_cnt_exception_self_companyphone as degree1_black_bankcard_cnt_exception_self_companyphone
,a.black_cid_cnt as degree1_black_cid_cnt
,a.black_cid_cnt_bankcard as degree1_black_cid_cnt_bankcard
,a.black_cid_cnt_companyphone as degree1_black_cid_cnt_companyphone
,a.black_cid_cnt_exception_self as degree1_black_cid_cnt_exception_self
,a.black_cid_cnt_exception_self_bankcard as degree1_black_cid_cnt_exception_self_bankcard
,a.black_cid_cnt_exception_self_companyphone as degree1_black_cid_cnt_exception_self_companyphone
,a.black_company_phone_cnt as degree1_black_company_phone_cnt
,a.black_company_phone_cnt_bankcard as degree1_black_company_phone_cnt_bankcard
,a.black_company_phone_cnt_companyphone as degree1_black_company_phone_cnt_companyphone
,a.black_company_phone_cnt_exception_self as degree1_black_company_phone_cnt_exception_self
,a.black_company_phone_cnt_exception_self_bankcard as degree1_black_company_phone_cnt_exception_self_bankcard
,a.black_company_phone_cnt_exception_self_companyphone as degree1_black_company_phone_cnt_exception_self_companyphone
,a.black_contract_cnt as degree1_black_contract_cnt
,a.black_contract_cnt_bankcard as degree1_black_contract_cnt_bankcard
,a.black_contract_cnt_companyphone as degree1_black_contract_cnt_companyphone
,a.black_contract_cnt_exception_self as degree1_black_contract_cnt_exception_self
,a.black_contract_cnt_exception_self_bankcard as degree1_black_contract_cnt_exception_self_bankcard
,a.black_contract_cnt_exception_self_companyphone as degree1_black_contract_cnt_exception_self_companyphone
,a.black_email_cnt as degree1_black_email_cnt
,a.black_email_cnt_bankcard as degree1_black_email_cnt_bankcard
,a.black_email_cnt_companyphone as degree1_black_email_cnt_companyphone
,a.black_email_cnt_exception_self as degree1_black_email_cnt_exception_self
,a.black_email_cnt_exception_self_bankcard as degree1_black_email_cnt_exception_self_bankcard
,a.black_email_cnt_exception_self_companyphone as degree1_black_email_cnt_exception_self_companyphone
,a.black_imei_cnt as degree1_black_imei_cnt
,a.black_imei_cnt_bankcard as degree1_black_imei_cnt_bankcard
,a.black_imei_cnt_companyphone as degree1_black_imei_cnt_companyphone
,a.black_imei_cnt_exception_self as degree1_black_imei_cnt_exception_self
,a.black_imei_cnt_exception_self_bankcard as degree1_black_imei_cnt_exception_self_bankcard
,a.black_imei_cnt_exception_self_companyphone as degree1_black_imei_cnt_exception_self_companyphone
,a.black_mobile_cnt as degree1_black_mobile_cnt
,a.black_mobile_cnt_bankcard as degree1_black_mobile_cnt_bankcard
,a.black_mobile_cnt_companyphone as degree1_black_mobile_cnt_companyphone
,a.black_mobile_cnt_exception_self as degree1_black_mobile_cnt_exception_self
,a.black_mobile_cnt_exception_self_bankcard as degree1_black_mobile_cnt_exception_self_bankcard
,a.black_mobile_cnt_exception_self_companyphone as degree1_black_mobile_cnt_exception_self_companyphone
,a.cid_cnt1 as degree1_cid_cnt1
,a.cid_cnt1_bankcard as degree1_cid_cnt1_bankcard
,a.cid_cnt1_companyphone as degree1_cid_cnt1_companyphone
,a.cid_cnt1_exception_self as degree1_cid_cnt1_exception_self
,a.cid_cnt1_exception_self_bankcard as degree1_cid_cnt1_exception_self_bankcard
,a.cid_cnt1_exception_self_companyphone as degree1_cid_cnt1_exception_self_companyphone
,a.cid_cnt30 as degree1_cid_cnt30
,a.cid_cnt30_bankcard as degree1_cid_cnt30_bankcard
,a.cid_cnt30_companyphone as degree1_cid_cnt30_companyphone
,a.cid_cnt30_exception_self as degree1_cid_cnt30_exception_self
,a.cid_cnt30_exception_self_bankcard as degree1_cid_cnt30_exception_self_bankcard
,a.cid_cnt30_exception_self_companyphone as degree1_cid_cnt30_exception_self_companyphone
,a.cid_cnt3_bankcard as degree1_cid_cnt3_bankcard
,a.cid_cnt3_companyphone as degree1_cid_cnt3_companyphone
,a.cid_cnt3_exception_self as degree1_cid_cnt3_exception_self
,a.cid_cnt7 as degree1_cid_cnt7
,a.cid_cnt7_bankcard as degree1_cid_cnt7_bankcard
,a.cid_cnt7_companyphone as degree1_cid_cnt7_companyphone
,a.cid_cnt7_exception_self as degree1_cid_cnt7_exception_self
,a.cid_cnt7_exception_self_bankcard as degree1_cid_cnt7_exception_self_bankcard
,a.cid_cnt7_exception_self_companyphone as degree1_cid_cnt7_exception_self_companyphone
,a.cid_cnt_bankcard as degree1_cid_cnt_bankcard
,a.cid_cnt_companyphone as degree1_cid_cnt_companyphone
,a.cid_cnt_exception_self_bankcard as degree1_cid_cnt_exception_self_bankcard
,a.cid_cnt_exception_self_companyphone as degree1_cid_cnt_exception_self_companyphone
,a.company_phone_cnt1 as degree1_company_phone_cnt1
,a.company_phone_cnt1_bankcard as degree1_company_phone_cnt1_bankcard
,a.company_phone_cnt1_companyphone as degree1_company_phone_cnt1_companyphone
,a.company_phone_cnt1_exception_self as degree1_company_phone_cnt1_exception_self
,a.company_phone_cnt1_exception_self_bankcard as degree1_company_phone_cnt1_exception_self_bankcard
,a.company_phone_cnt1_exception_self_companyphone as degree1_company_phone_cnt1_exception_self_companyphone
,a.company_phone_cnt30 as degree1_company_phone_cnt30
,a.company_phone_cnt30_bankcard as degree1_company_phone_cnt30_bankcard
,a.company_phone_cnt30_companyphone as degree1_company_phone_cnt30_companyphone
,a.company_phone_cnt30_exception_self as degree1_company_phone_cnt30_exception_self
,a.company_phone_cnt30_exception_self_bankcard as degree1_company_phone_cnt30_exception_self_bankcard
,a.company_phone_cnt30_exception_self_companyphone as degree1_company_phone_cnt30_exception_self_companyphone
,a.company_phone_cnt3_bankcard as degree1_company_phone_cnt3_bankcard
,a.company_phone_cnt3_companyphone as degree1_company_phone_cnt3_companyphone
,a.company_phone_cnt3_exception_self as degree1_company_phone_cnt3_exception_self
,a.company_phone_cnt7 as degree1_company_phone_cnt7
,a.company_phone_cnt7_bankcard as degree1_company_phone_cnt7_bankcard
,a.company_phone_cnt7_companyphone as degree1_company_phone_cnt7_companyphone
,a.company_phone_cnt7_exception_self as degree1_company_phone_cnt7_exception_self
,a.company_phone_cnt7_exception_self_bankcard as degree1_company_phone_cnt7_exception_self_bankcard
,a.company_phone_cnt7_exception_self_companyphone as degree1_company_phone_cnt7_exception_self_companyphone
,a.company_phone_cnt_bankcard as degree1_company_phone_cnt_bankcard
,a.company_phone_cnt_companyphone as degree1_company_phone_cnt_companyphone
,a.company_phone_cnt_exception_self_bankcard as degree1_company_phone_cnt_exception_self_bankcard
,a.company_phone_cnt_exception_self_companyphone as degree1_company_phone_cnt_exception_self_companyphone
,a.contact_mobile_cnt1 as degree1_contact_mobile_cnt1
,a.contact_mobile_cnt1_bankcard as degree1_contact_mobile_cnt1_bankcard
,a.contact_mobile_cnt1_companyphone as degree1_contact_mobile_cnt1_companyphone
,a.contact_mobile_cnt1_exception_self as degree1_contact_mobile_cnt1_exception_self
,a.contact_mobile_cnt1_exception_self_bankcard as degree1_contact_mobile_cnt1_exception_self_bankcard
,a.contact_mobile_cnt1_exception_self_companyphone as degree1_contact_mobile_cnt1_exception_self_companyphone
,a.contact_mobile_cnt30 as degree1_contact_mobile_cnt30
,a.contact_mobile_cnt30_bankcard as degree1_contact_mobile_cnt30_bankcard
,a.contact_mobile_cnt30_companyphone as degree1_contact_mobile_cnt30_companyphone
,a.contact_mobile_cnt30_exception_self as degree1_contact_mobile_cnt30_exception_self
,a.contact_mobile_cnt30_exception_self_bankcard as degree1_contact_mobile_cnt30_exception_self_bankcard
,a.contact_mobile_cnt30_exception_self_companyphone as degree1_contact_mobile_cnt30_exception_self_companyphone
,a.contact_mobile_cnt3_bankcard as degree1_contact_mobile_cnt3_bankcard
,a.contact_mobile_cnt3_companyphone as degree1_contact_mobile_cnt3_companyphone
,a.contact_mobile_cnt3_exception_self as degree1_contact_mobile_cnt3_exception_self
,a.contact_mobile_cnt7 as degree1_contact_mobile_cnt7
,a.contact_mobile_cnt7_bankcard as degree1_contact_mobile_cnt7_bankcard
,a.contact_mobile_cnt7_companyphone as degree1_contact_mobile_cnt7_companyphone
,a.contact_mobile_cnt7_exception_self as degree1_contact_mobile_cnt7_exception_self
,a.contact_mobile_cnt7_exception_self_bankcard as degree1_contact_mobile_cnt7_exception_self_bankcard
,a.contact_mobile_cnt7_exception_self_companyphone as degree1_contact_mobile_cnt7_exception_self_companyphone
,a.contact_mobile_cnt_bankcard as degree1_contact_mobile_cnt_bankcard
,a.contact_mobile_cnt_companyphone as degree1_contact_mobile_cnt_companyphone
,a.contact_mobile_cnt_exception_self_bankcard as degree1_contact_mobile_cnt_exception_self_bankcard
,a.contact_mobile_cnt_exception_self_companyphone as degree1_contact_mobile_cnt_exception_self_companyphone
,a.current_overdue0_contract_cnt as degree1_current_overdue0_contract_cnt
,a.current_overdue0_contract_cnt_bankcard as degree1_current_overdue0_contract_cnt_bankcard
,a.current_overdue0_contract_cnt_companyphone as degree1_current_overdue0_contract_cnt_companyphone
,a.current_overdue0_contract_cnt_exception_self as degree1_current_overdue0_contract_cnt_exception_self
,a.current_overdue0_contract_cnt_exception_self_bankcard as degree1_current_overdue0_contract_cnt_exception_self_bankcard
,a.current_overdue0_contract_cnt_exception_self_companyphone as degree1_current_overdue0_contract_cnt_exception_self_companyphone
,a.current_overdue30_contract_cnt as degree1_current_overdue30_contract_cnt
,a.current_overdue30_contract_cnt_bankcard as degree1_current_overdue30_contract_cnt_bankcard
,a.current_overdue30_contract_cnt_companyphone as degree1_current_overdue30_contract_cnt_companyphone
,a.current_overdue30_contract_cnt_exception_self as degree1_current_overdue30_contract_cnt_exception_self
,a.current_overdue30_contract_cnt_exception_self_bankcard as degree1_current_overdue30_contract_cnt_exception_self_bankcard
,a.current_overdue30_contract_cnt_exception_self_companyphone as degree1_current_overdue30_contract_cnt_exception_self_companyphone
,a.current_overdue3_contract_cnt as degree1_current_overdue3_contract_cnt
,a.current_overdue3_contract_cnt_bankcard as degree1_current_overdue3_contract_cnt_bankcard
,a.current_overdue3_contract_cnt_companyphone as degree1_current_overdue3_contract_cnt_companyphone
,a.current_overdue3_contract_cnt_exception_self as degree1_current_overdue3_contract_cnt_exception_self
,a.current_overdue3_contract_cnt_exception_self_bankcard as degree1_current_overdue3_contract_cnt_exception_self_bankcard
,a.current_overdue3_contract_cnt_exception_self_companyphone as degree1_current_overdue3_contract_cnt_exception_self_companyphone
,a.email_cnt1 as degree1_email_cnt1
,a.email_cnt1_bankcard as degree1_email_cnt1_bankcard
,a.email_cnt1_companyphone as degree1_email_cnt1_companyphone
,a.email_cnt1_exception_self as degree1_email_cnt1_exception_self
,a.email_cnt1_exception_self_bankcard as degree1_email_cnt1_exception_self_bankcard
,a.email_cnt1_exception_self_companyphone as degree1_email_cnt1_exception_self_companyphone
,a.email_cnt30 as degree1_email_cnt30
,a.email_cnt30_bankcard as degree1_email_cnt30_bankcard
,a.email_cnt30_companyphone as degree1_email_cnt30_companyphone
,a.email_cnt30_exception_self as degree1_email_cnt30_exception_self
,a.email_cnt30_exception_self_bankcard as degree1_email_cnt30_exception_self_bankcard
,a.email_cnt30_exception_self_companyphone as degree1_email_cnt30_exception_self_companyphone
,a.email_cnt3_bankcard as degree1_email_cnt3_bankcard
,a.email_cnt3_companyphone as degree1_email_cnt3_companyphone
,a.email_cnt3_exception_self as degree1_email_cnt3_exception_self
,a.email_cnt7 as degree1_email_cnt7
,a.email_cnt7_bankcard as degree1_email_cnt7_bankcard
,a.email_cnt7_companyphone as degree1_email_cnt7_companyphone
,a.email_cnt7_exception_self as degree1_email_cnt7_exception_self
,a.email_cnt7_exception_self_bankcard as degree1_email_cnt7_exception_self_bankcard
,a.email_cnt7_exception_self_companyphone as degree1_email_cnt7_exception_self_companyphone
,a.email_cnt_bankcard as degree1_email_cnt_bankcard
,a.email_cnt_companyphone as degree1_email_cnt_companyphone
,a.email_cnt_exception_self_bankcard as degree1_email_cnt_exception_self_bankcard
,a.email_cnt_exception_self_companyphone as degree1_email_cnt_exception_self_companyphone
,a.emergency_mobile_cnt1 as degree1_emergency_mobile_cnt1
,a.emergency_mobile_cnt1_bankcard as degree1_emergency_mobile_cnt1_bankcard
,a.emergency_mobile_cnt1_companyphone as degree1_emergency_mobile_cnt1_companyphone
,a.emergency_mobile_cnt1_exception_self as degree1_emergency_mobile_cnt1_exception_self
,a.emergency_mobile_cnt1_exception_self_bankcard as degree1_emergency_mobile_cnt1_exception_self_bankcard
,a.emergency_mobile_cnt1_exception_self_companyphone as degree1_emergency_mobile_cnt1_exception_self_companyphone
,a.emergency_mobile_cnt30 as degree1_emergency_mobile_cnt30
,a.emergency_mobile_cnt30_bankcard as degree1_emergency_mobile_cnt30_bankcard
,a.emergency_mobile_cnt30_companyphone as degree1_emergency_mobile_cnt30_companyphone
,a.emergency_mobile_cnt30_exception_self as degree1_emergency_mobile_cnt30_exception_self
,a.emergency_mobile_cnt30_exception_self_bankcard as degree1_emergency_mobile_cnt30_exception_self_bankcard
,a.emergency_mobile_cnt30_exception_self_companyphone as degree1_emergency_mobile_cnt30_exception_self_companyphone
,a.emergency_mobile_cnt3_bankcard as degree1_emergency_mobile_cnt3_bankcard
,a.emergency_mobile_cnt3_companyphone as degree1_emergency_mobile_cnt3_companyphone
,a.emergency_mobile_cnt3_exception_self as degree1_emergency_mobile_cnt3_exception_self
,a.emergency_mobile_cnt7 as degree1_emergency_mobile_cnt7
,a.emergency_mobile_cnt7_bankcard as degree1_emergency_mobile_cnt7_bankcard
,a.emergency_mobile_cnt7_companyphone as degree1_emergency_mobile_cnt7_companyphone
,a.emergency_mobile_cnt7_exception_self as degree1_emergency_mobile_cnt7_exception_self
,a.emergency_mobile_cnt7_exception_self_bankcard as degree1_emergency_mobile_cnt7_exception_self_bankcard
,a.emergency_mobile_cnt7_exception_self_companyphone as degree1_emergency_mobile_cnt7_exception_self_companyphone
,a.emergency_mobile_cnt_bankcard as degree1_emergency_mobile_cnt_bankcard
,a.emergency_mobile_cnt_companyphone as degree1_emergency_mobile_cnt_companyphone
,a.emergency_mobile_cnt_exception_self_bankcard as degree1_emergency_mobile_cnt_exception_self_bankcard
,a.emergency_mobile_cnt_exception_self_companyphone as degree1_emergency_mobile_cnt_exception_self_companyphone
,a.history_overdue0_contract_cnt as degree1_history_overdue0_contract_cnt
,a.history_overdue0_contract_cnt_bankcard as degree1_history_overdue0_contract_cnt_bankcard
,a.history_overdue0_contract_cnt_companyphone as degree1_history_overdue0_contract_cnt_companyphone
,a.history_overdue0_contract_cnt_exception_self as degree1_history_overdue0_contract_cnt_exception_self
,a.history_overdue0_contract_cnt_exception_self_bankcard as degree1_history_overdue0_contract_cnt_exception_self_bankcard
,a.history_overdue0_contract_cnt_exception_self_companyphone as degree1_history_overdue0_contract_cnt_exception_self_companyphone
,a.history_overdue30_contract_cnt as degree1_history_overdue30_contract_cnt
,a.history_overdue30_contract_cnt_bankcard as degree1_history_overdue30_contract_cnt_bankcard
,a.history_overdue30_contract_cnt_companyphone as degree1_history_overdue30_contract_cnt_companyphone
,a.history_overdue30_contract_cnt_exception_self as degree1_history_overdue30_contract_cnt_exception_self
,a.history_overdue30_contract_cnt_exception_self_bankcard as degree1_history_overdue30_contract_cnt_exception_self_bankcard
,a.history_overdue30_contract_cnt_exception_self_companyphone as degree1_history_overdue30_contract_cnt_exception_self_companyphone
,a.history_overdue3_contract_cnt as degree1_history_overdue3_contract_cnt
,a.history_overdue3_contract_cnt_bankcard as degree1_history_overdue3_contract_cnt_bankcard
,a.history_overdue3_contract_cnt_companyphone as degree1_history_overdue3_contract_cnt_companyphone
,a.history_overdue3_contract_cnt_exception_self as degree1_history_overdue3_contract_cnt_exception_self
,a.history_overdue3_contract_cnt_exception_self_bankcard as degree1_history_overdue3_contract_cnt_exception_self_bankcard
,a.history_overdue3_contract_cnt_exception_self_companyphone as degree1_history_overdue3_contract_cnt_exception_self_companyphone
,a.imei_cnt1 as degree1_imei_cnt1
,a.imei_cnt1_bankcard as degree1_imei_cnt1_bankcard
,a.imei_cnt1_companyphone as degree1_imei_cnt1_companyphone
,a.imei_cnt1_exception_self as degree1_imei_cnt1_exception_self
,a.imei_cnt1_exception_self_bankcard as degree1_imei_cnt1_exception_self_bankcard
,a.imei_cnt1_exception_self_companyphone as degree1_imei_cnt1_exception_self_companyphone
,a.imei_cnt30 as degree1_imei_cnt30
,a.imei_cnt30_bankcard as degree1_imei_cnt30_bankcard
,a.imei_cnt30_companyphone as degree1_imei_cnt30_companyphone
,a.imei_cnt30_exception_self as degree1_imei_cnt30_exception_self
,a.imei_cnt30_exception_self_bankcard as degree1_imei_cnt30_exception_self_bankcard
,a.imei_cnt30_exception_self_companyphone as degree1_imei_cnt30_exception_self_companyphone
,a.imei_cnt3_bankcard as degree1_imei_cnt3_bankcard
,a.imei_cnt3_companyphone as degree1_imei_cnt3_companyphone
,a.imei_cnt3_exception_self as degree1_imei_cnt3_exception_self
,a.imei_cnt7 as degree1_imei_cnt7
,a.imei_cnt7_bankcard as degree1_imei_cnt7_bankcard
,a.imei_cnt7_companyphone as degree1_imei_cnt7_companyphone
,a.imei_cnt7_exception_self as degree1_imei_cnt7_exception_self
,a.imei_cnt7_exception_self_bankcard as degree1_imei_cnt7_exception_self_bankcard
,a.imei_cnt7_exception_self_companyphone as degree1_imei_cnt7_exception_self_companyphone
,a.imei_cnt_bankcard as degree1_imei_cnt_bankcard
,a.imei_cnt_companyphone as degree1_imei_cnt_companyphone
,a.imei_cnt_exception_self_bankcard as degree1_imei_cnt_exception_self_bankcard
,a.imei_cnt_exception_self_companyphone as degree1_imei_cnt_exception_self_companyphone
,a.mobile_cnt1 as degree1_mobile_cnt1
,a.mobile_cnt1_bankcard as degree1_mobile_cnt1_bankcard
,a.mobile_cnt1_companyphone as degree1_mobile_cnt1_companyphone
,a.mobile_cnt1_exception_self as degree1_mobile_cnt1_exception_self
,a.mobile_cnt1_exception_self_bankcard as degree1_mobile_cnt1_exception_self_bankcard
,a.mobile_cnt1_exception_self_companyphone as degree1_mobile_cnt1_exception_self_companyphone
,a.mobile_cnt30 as degree1_mobile_cnt30
,a.mobile_cnt30_bankcard as degree1_mobile_cnt30_bankcard
,a.mobile_cnt30_companyphone as degree1_mobile_cnt30_companyphone
,a.mobile_cnt30_exception_self as degree1_mobile_cnt30_exception_self
,a.mobile_cnt30_exception_self_bankcard as degree1_mobile_cnt30_exception_self_bankcard
,a.mobile_cnt30_exception_self_companyphone as degree1_mobile_cnt30_exception_self_companyphone
,a.mobile_cnt3_bankcard as degree1_mobile_cnt3_bankcard
,a.mobile_cnt3_companyphone as degree1_mobile_cnt3_companyphone
,a.mobile_cnt3_exception_self as degree1_mobile_cnt3_exception_self
,a.mobile_cnt7 as degree1_mobile_cnt7
,a.mobile_cnt7_bankcard as degree1_mobile_cnt7_bankcard
,a.mobile_cnt7_companyphone as degree1_mobile_cnt7_companyphone
,a.mobile_cnt7_exception_self as degree1_mobile_cnt7_exception_self
,a.mobile_cnt7_exception_self_bankcard as degree1_mobile_cnt7_exception_self_bankcard
,a.mobile_cnt7_exception_self_companyphone as degree1_mobile_cnt7_exception_self_companyphone
,a.mobile_cnt_bankcard as degree1_mobile_cnt_bankcard
,a.mobile_cnt_companyphone as degree1_mobile_cnt_companyphone
,a.mobile_cnt_exception_self_bankcard as degree1_mobile_cnt_exception_self_bankcard
,a.mobile_cnt_exception_self_companyphone as degree1_mobile_cnt_exception_self_companyphone
,a.order_cnt as degree1_order_cnt
,a.order_cnt_bankcard as degree1_order_cnt_bankcard
,a.order_cnt_companyphone as degree1_order_cnt_companyphone
,a.order_cnt_exception_self as degree1_order_cnt_exception_self
,a.order_cnt_exception_self_bankcard as degree1_order_cnt_exception_self_bankcard
,a.order_cnt_exception_self_companyphone as degree1_order_cnt_exception_self_companyphone
,a.order_cnt_myphone as degree1_order_cnt_myphone
,a.pass_contract_cnt as degree1_pass_contract_cnt
,a.pass_contract_cnt_bankcard as degree1_pass_contract_cnt_bankcard
,a.pass_contract_cnt_companyphone as degree1_pass_contract_cnt_companyphone
,a.pass_contract_cnt_exception_self as degree1_pass_contract_cnt_exception_self
,a.pass_contract_cnt_exception_self_bankcard as degree1_pass_contract_cnt_exception_self_bankcard
,a.pass_contract_cnt_exception_self_companyphone as degree1_pass_contract_cnt_exception_self_companyphone
,a.product_cnt_bankcard as degree1_product_cnt_bankcard
,a.product_cnt_companyphone as degree1_product_cnt_companyphone
,a.product_cnt_exception_self_bankcard as degree1_product_cnt_exception_self_bankcard
,a.product_cnt_exception_self_companyphone as degree1_product_cnt_exception_self_companyphone
,a.q_order_cnt as degree1_q_order_cnt
,a.q_order_cnt_bankcard as degree1_q_order_cnt_bankcard
,a.q_order_cnt_companyphone as degree1_q_order_cnt_companyphone
,a.q_order_cnt_exception_self as degree1_q_order_cnt_exception_self
,a.q_order_cnt_exception_self_bankcard as degree1_q_order_cnt_exception_self_bankcard
,a.q_order_cnt_exception_self_companyphone as degree1_q_order_cnt_exception_self_companyphone
,a.tnh_cnt_bankcard as degree1_tnh_cnt_bankcard
,a.tnh_cnt_companyphone as degree1_tnh_cnt_companyphone
,a.tnh_cnt_exception_self_bankcard as degree1_tnh_cnt_exception_self_bankcard
,a.tnh_cnt_exception_self_companyphone as degree1_tnh_cnt_exception_self_companyphone
,a.yfq_cnt_bankcard as degree1_yfq_cnt_bankcard
,a.yfq_cnt_companyphone as degree1_yfq_cnt_companyphone
,a.yfq_cnt_exception_self_bankcard as degree1_yfq_cnt_exception_self_bankcard
,a.yfq_cnt_exception_self_companyphone as degree1_yfq_cnt_exception_self_companyphone
,b.bankcard_cnt
,b.bankcard_cnt_bankcard_bankcard
,b.bankcard_cnt_bankcard_companyphone
,b.bankcard_cnt_bankcard_contact
,b.bankcard_cnt_bankcard_device
,b.bankcard_cnt_bankcard_email
,b.bankcard_cnt_bankcard_emergency
,b.bankcard_cnt_bankcard_idcard
,b.bankcard_cnt_bankcard_myphone
,b.bankcard_cnt_companyphone_bankcard
,b.bankcard_cnt_companyphone_companyphone
,b.bankcard_cnt_companyphone_contact
,b.bankcard_cnt_companyphone_device
,b.bankcard_cnt_companyphone_email
,b.bankcard_cnt_companyphone_emergency
,b.bankcard_cnt_companyphone_idcard
,b.bankcard_cnt_companyphone_myphone
,b.bankcard_cnt_contact_bankcard
,b.bankcard_cnt_contact_companyphone
,b.bankcard_cnt_contact_contact
,b.bankcard_cnt_contact_device
,b.bankcard_cnt_contact_email
,b.bankcard_cnt_contact_emergency
,b.bankcard_cnt_contact_idcard
,b.bankcard_cnt_contact_myphone
,b.bankcard_cnt_device_bankcard
,b.bankcard_cnt_device_companyphone
,b.bankcard_cnt_device_contact
,b.bankcard_cnt_device_device
,b.bankcard_cnt_device_email
,b.bankcard_cnt_device_emergency
,b.bankcard_cnt_device_idcard
,b.bankcard_cnt_device_myphone
,b.bankcard_cnt_email_bankcard
,b.bankcard_cnt_email_companyphone
,b.bankcard_cnt_email_contact
,b.bankcard_cnt_email_device
,b.bankcard_cnt_email_email
,b.bankcard_cnt_email_emergency
,b.bankcard_cnt_email_idcard
,b.bankcard_cnt_email_myphone
,b.bankcard_cnt_emergency_bankcard
,b.bankcard_cnt_emergency_companyphone
,b.bankcard_cnt_emergency_contact
,b.bankcard_cnt_emergency_device
,b.bankcard_cnt_emergency_email
,b.bankcard_cnt_emergency_emergency
,b.bankcard_cnt_emergency_idcard
,b.bankcard_cnt_emergency_myphone
,b.bankcard_cnt_idcard_bankcard
,b.bankcard_cnt_idcard_companyphone
,b.bankcard_cnt_idcard_contact
,b.bankcard_cnt_idcard_device
,b.bankcard_cnt_idcard_email
,b.bankcard_cnt_idcard_emergency
,b.bankcard_cnt_idcard_idcard
,b.bankcard_cnt_idcard_myphone
,b.bankcard_cnt_myphone_bankcard
,b.bankcard_cnt_myphone_companyphone
,b.bankcard_cnt_myphone_contact
,b.bankcard_cnt_myphone_device
,b.bankcard_cnt_myphone_email
,b.bankcard_cnt_myphone_emergency
,b.bankcard_cnt_myphone_idcard
,b.bankcard_cnt_myphone_myphone
,b.bankcard_cnt1
,b.bankcard_cnt1_bankcard_bankcard
,b.bankcard_cnt1_bankcard_companyphone
,b.bankcard_cnt1_bankcard_contact
,b.bankcard_cnt1_bankcard_device
,b.bankcard_cnt1_bankcard_email
,b.bankcard_cnt1_bankcard_emergency
,b.bankcard_cnt1_bankcard_idcard
,b.bankcard_cnt1_bankcard_myphone
,b.bankcard_cnt1_companyphone_bankcard
,b.bankcard_cnt1_companyphone_companyphone
,b.bankcard_cnt1_companyphone_contact
,b.bankcard_cnt1_companyphone_device
,b.bankcard_cnt1_companyphone_email
,b.bankcard_cnt1_companyphone_emergency
,b.bankcard_cnt1_companyphone_idcard
,b.bankcard_cnt1_companyphone_myphone
,b.bankcard_cnt1_contact_bankcard
,b.bankcard_cnt1_contact_companyphone
,b.bankcard_cnt1_contact_contact
,b.bankcard_cnt1_contact_device
,b.bankcard_cnt1_contact_email
,b.bankcard_cnt1_contact_emergency
,b.bankcard_cnt1_contact_idcard
,b.bankcard_cnt1_contact_myphone
,b.bankcard_cnt1_device_bankcard
,b.bankcard_cnt1_device_companyphone
,b.bankcard_cnt1_device_contact
,b.bankcard_cnt1_device_device
,b.bankcard_cnt1_device_email
,b.bankcard_cnt1_device_emergency
,b.bankcard_cnt1_device_idcard
,b.bankcard_cnt1_device_myphone
,b.bankcard_cnt1_email_bankcard
,b.bankcard_cnt1_email_companyphone
,b.bankcard_cnt1_email_contact
,b.bankcard_cnt1_email_device
,b.bankcard_cnt1_email_email
,b.bankcard_cnt1_email_emergency
,b.bankcard_cnt1_email_idcard
,b.bankcard_cnt1_email_myphone
,b.bankcard_cnt1_emergency_bankcard
,b.bankcard_cnt1_emergency_companyphone
,b.bankcard_cnt1_emergency_contact
,b.bankcard_cnt1_emergency_device
,b.bankcard_cnt1_emergency_email
,b.bankcard_cnt1_emergency_emergency
,b.bankcard_cnt1_emergency_idcard
,b.bankcard_cnt1_emergency_myphone
,b.bankcard_cnt1_idcard_bankcard
,b.bankcard_cnt1_idcard_companyphone
,b.bankcard_cnt1_idcard_contact
,b.bankcard_cnt1_idcard_device
,b.bankcard_cnt1_idcard_email
,b.bankcard_cnt1_idcard_emergency
,b.bankcard_cnt1_idcard_idcard
,b.bankcard_cnt1_idcard_myphone
,b.bankcard_cnt1_myphone_bankcard
,b.bankcard_cnt1_myphone_companyphone
,b.bankcard_cnt1_myphone_contact
,b.bankcard_cnt1_myphone_device
,b.bankcard_cnt1_myphone_email
,b.bankcard_cnt1_myphone_emergency
,b.bankcard_cnt1_myphone_idcard
,b.bankcard_cnt1_myphone_myphone
,b.bankcard_cnt3
,b.bankcard_cnt3_bankcard_bankcard
,b.bankcard_cnt3_bankcard_companyphone
,b.bankcard_cnt3_bankcard_contact
,b.bankcard_cnt3_bankcard_device
,b.bankcard_cnt3_bankcard_email
,b.bankcard_cnt3_bankcard_emergency
,b.bankcard_cnt3_bankcard_idcard
,b.bankcard_cnt3_bankcard_myphone
,b.bankcard_cnt3_companyphone_bankcard
,b.bankcard_cnt3_companyphone_companyphone
,b.bankcard_cnt3_companyphone_contact
,b.bankcard_cnt3_companyphone_device
,b.bankcard_cnt3_companyphone_email
,b.bankcard_cnt3_companyphone_emergency
,b.bankcard_cnt3_companyphone_idcard
,b.bankcard_cnt3_companyphone_myphone
,b.bankcard_cnt3_contact_bankcard
,b.bankcard_cnt3_contact_companyphone
,b.bankcard_cnt3_contact_contact
,b.bankcard_cnt3_contact_device
,b.bankcard_cnt3_contact_email
,b.bankcard_cnt3_contact_emergency
,b.bankcard_cnt3_contact_idcard
,b.bankcard_cnt3_contact_myphone
,b.bankcard_cnt3_device_bankcard
,b.bankcard_cnt3_device_companyphone
,b.bankcard_cnt3_device_contact
,b.bankcard_cnt3_device_device
,b.bankcard_cnt3_device_email
,b.bankcard_cnt3_device_emergency
,b.bankcard_cnt3_device_idcard
,b.bankcard_cnt3_device_myphone
,b.bankcard_cnt3_email_bankcard
,b.bankcard_cnt3_email_companyphone
,b.bankcard_cnt3_email_contact
,b.bankcard_cnt3_email_device
,b.bankcard_cnt3_email_email
,b.bankcard_cnt3_email_emergency
,b.bankcard_cnt3_email_idcard
,b.bankcard_cnt3_email_myphone
,b.bankcard_cnt3_emergency_bankcard
,b.bankcard_cnt3_emergency_companyphone
,b.bankcard_cnt3_emergency_contact
,b.bankcard_cnt3_emergency_device
,b.bankcard_cnt3_emergency_email
,b.bankcard_cnt3_emergency_emergency
,b.bankcard_cnt3_emergency_idcard
,b.bankcard_cnt3_emergency_myphone
,b.bankcard_cnt3_idcard_bankcard
,b.bankcard_cnt3_idcard_companyphone
,b.bankcard_cnt3_idcard_contact
,b.bankcard_cnt3_idcard_device
,b.bankcard_cnt3_idcard_email
,b.bankcard_cnt3_idcard_emergency
,b.bankcard_cnt3_idcard_idcard
,b.bankcard_cnt3_idcard_myphone
,b.bankcard_cnt3_myphone_bankcard
,b.bankcard_cnt3_myphone_companyphone
,b.bankcard_cnt3_myphone_contact
,b.bankcard_cnt3_myphone_device
,b.bankcard_cnt3_myphone_email
,b.bankcard_cnt3_myphone_emergency
,b.bankcard_cnt3_myphone_idcard
,b.bankcard_cnt3_myphone_myphone
,b.bankcard_cnt30
,b.bankcard_cnt30_bankcard_bankcard
,b.bankcard_cnt30_bankcard_companyphone
,b.bankcard_cnt30_bankcard_contact
,b.bankcard_cnt30_bankcard_device
,b.bankcard_cnt30_bankcard_email
,b.bankcard_cnt30_bankcard_emergency
,b.bankcard_cnt30_bankcard_idcard
,b.bankcard_cnt30_bankcard_myphone
,b.bankcard_cnt30_companyphone_bankcard
,b.bankcard_cnt30_companyphone_companyphone
,b.bankcard_cnt30_companyphone_contact
,b.bankcard_cnt30_companyphone_device
,b.bankcard_cnt30_companyphone_email
,b.bankcard_cnt30_companyphone_emergency
,b.bankcard_cnt30_companyphone_idcard
,b.bankcard_cnt30_companyphone_myphone
,b.bankcard_cnt30_contact_bankcard
,b.bankcard_cnt30_contact_companyphone
,b.bankcard_cnt30_contact_contact
,b.bankcard_cnt30_contact_device
,b.bankcard_cnt30_contact_email
,b.bankcard_cnt30_contact_emergency
,b.bankcard_cnt30_contact_idcard
,b.bankcard_cnt30_contact_myphone
,b.bankcard_cnt30_device_bankcard
,b.bankcard_cnt30_device_companyphone
,b.bankcard_cnt30_device_contact
,b.bankcard_cnt30_device_device
,b.bankcard_cnt30_device_email
,b.bankcard_cnt30_device_emergency
,b.bankcard_cnt30_device_idcard
,b.bankcard_cnt30_device_myphone
,b.bankcard_cnt30_email_bankcard
,b.bankcard_cnt30_email_companyphone
,b.bankcard_cnt30_email_contact
,b.bankcard_cnt30_email_device
,b.bankcard_cnt30_email_email
,b.bankcard_cnt30_email_emergency
,b.bankcard_cnt30_email_idcard
,b.bankcard_cnt30_email_myphone
,b.bankcard_cnt30_emergency_bankcard
,b.bankcard_cnt30_emergency_companyphone
,b.bankcard_cnt30_emergency_contact
,b.bankcard_cnt30_emergency_device
,b.bankcard_cnt30_emergency_email
,b.bankcard_cnt30_emergency_emergency
,b.bankcard_cnt30_emergency_idcard
,b.bankcard_cnt30_emergency_myphone
,b.bankcard_cnt30_idcard_bankcard
,b.bankcard_cnt30_idcard_companyphone
,b.bankcard_cnt30_idcard_contact
,b.bankcard_cnt30_idcard_device
,b.bankcard_cnt30_idcard_email
,b.bankcard_cnt30_idcard_emergency
,b.bankcard_cnt30_idcard_idcard
,b.bankcard_cnt30_idcard_myphone
,b.bankcard_cnt30_myphone_bankcard
,b.bankcard_cnt30_myphone_companyphone
,b.bankcard_cnt30_myphone_contact
,b.bankcard_cnt30_myphone_device
,b.bankcard_cnt30_myphone_email
,b.bankcard_cnt30_myphone_emergency
,b.bankcard_cnt30_myphone_idcard
,b.bankcard_cnt30_myphone_myphone
,b.bankcard_cnt7
,b.bankcard_cnt7_bankcard_bankcard
,b.bankcard_cnt7_bankcard_companyphone
,b.bankcard_cnt7_bankcard_contact
,b.bankcard_cnt7_bankcard_device
,b.bankcard_cnt7_bankcard_email
,b.bankcard_cnt7_bankcard_emergency
,b.bankcard_cnt7_bankcard_idcard
,b.bankcard_cnt7_bankcard_myphone
,b.bankcard_cnt7_companyphone_bankcard
,b.bankcard_cnt7_companyphone_companyphone
,b.bankcard_cnt7_companyphone_contact
,b.bankcard_cnt7_companyphone_device
,b.bankcard_cnt7_companyphone_email
,b.bankcard_cnt7_companyphone_emergency
,b.bankcard_cnt7_companyphone_idcard
,b.bankcard_cnt7_companyphone_myphone
,b.bankcard_cnt7_contact_bankcard
,b.bankcard_cnt7_contact_companyphone
,b.bankcard_cnt7_contact_contact
,b.bankcard_cnt7_contact_device
,b.bankcard_cnt7_contact_email
,b.bankcard_cnt7_contact_emergency
,b.bankcard_cnt7_contact_idcard
,b.bankcard_cnt7_contact_myphone
,b.bankcard_cnt7_device_bankcard
,b.bankcard_cnt7_device_companyphone
,b.bankcard_cnt7_device_contact
,b.bankcard_cnt7_device_device
,b.bankcard_cnt7_device_email
,b.bankcard_cnt7_device_emergency
,b.bankcard_cnt7_device_idcard
,b.bankcard_cnt7_device_myphone
,b.bankcard_cnt7_email_bankcard
,b.bankcard_cnt7_email_companyphone
,b.bankcard_cnt7_email_contact
,b.bankcard_cnt7_email_device
,b.bankcard_cnt7_email_email
,b.bankcard_cnt7_email_emergency
,b.bankcard_cnt7_email_idcard
,b.bankcard_cnt7_email_myphone
,b.bankcard_cnt7_emergency_bankcard
,b.bankcard_cnt7_emergency_companyphone
,b.bankcard_cnt7_emergency_contact
,b.bankcard_cnt7_emergency_device
,b.bankcard_cnt7_emergency_email
,b.bankcard_cnt7_emergency_emergency
,b.bankcard_cnt7_emergency_idcard
,b.bankcard_cnt7_emergency_myphone
,b.bankcard_cnt7_idcard_bankcard
,b.bankcard_cnt7_idcard_companyphone
,b.bankcard_cnt7_idcard_contact
,b.bankcard_cnt7_idcard_device
,b.bankcard_cnt7_idcard_email
,b.bankcard_cnt7_idcard_emergency
,b.bankcard_cnt7_idcard_idcard
,b.bankcard_cnt7_idcard_myphone
,b.bankcard_cnt7_myphone_bankcard
,b.bankcard_cnt7_myphone_companyphone
,b.bankcard_cnt7_myphone_contact
,b.bankcard_cnt7_myphone_device
,b.bankcard_cnt7_myphone_email
,b.bankcard_cnt7_myphone_emergency
,b.bankcard_cnt7_myphone_idcard
,b.bankcard_cnt7_myphone_myphone
,b.black_bankcard_cnt
,b.black_bankcard_cnt_bankcard_bankcard
,b.black_bankcard_cnt_bankcard_companyphone
,b.black_bankcard_cnt_bankcard_contact
,b.black_bankcard_cnt_bankcard_device
,b.black_bankcard_cnt_bankcard_email
,b.black_bankcard_cnt_bankcard_emergency
,b.black_bankcard_cnt_bankcard_idcard
,b.black_bankcard_cnt_bankcard_myphone
,b.black_bankcard_cnt_companyphone_bankcard
,b.black_bankcard_cnt_companyphone_companyphone
,b.black_bankcard_cnt_companyphone_contact
,b.black_bankcard_cnt_companyphone_device
,b.black_bankcard_cnt_companyphone_email
,b.black_bankcard_cnt_companyphone_emergency
,b.black_bankcard_cnt_companyphone_idcard
,b.black_bankcard_cnt_companyphone_myphone
,b.black_bankcard_cnt_contact_bankcard
,b.black_bankcard_cnt_contact_companyphone
,b.black_bankcard_cnt_contact_contact
,b.black_bankcard_cnt_contact_device
,b.black_bankcard_cnt_contact_email
,b.black_bankcard_cnt_contact_emergency
,b.black_bankcard_cnt_contact_idcard
,b.black_bankcard_cnt_contact_myphone
,b.black_bankcard_cnt_device_bankcard
,b.black_bankcard_cnt_device_companyphone
,b.black_bankcard_cnt_device_contact
,b.black_bankcard_cnt_device_device
,b.black_bankcard_cnt_device_email
,b.black_bankcard_cnt_device_emergency
,b.black_bankcard_cnt_device_idcard
,b.black_bankcard_cnt_device_myphone
,b.black_bankcard_cnt_email_bankcard
,b.black_bankcard_cnt_email_companyphone
,b.black_bankcard_cnt_email_contact
,b.black_bankcard_cnt_email_device
,b.black_bankcard_cnt_email_email
,b.black_bankcard_cnt_email_emergency
,b.black_bankcard_cnt_email_idcard
,b.black_bankcard_cnt_email_myphone
,b.black_bankcard_cnt_emergency_bankcard
,b.black_bankcard_cnt_emergency_companyphone
,b.black_bankcard_cnt_emergency_contact
,b.black_bankcard_cnt_emergency_device
,b.black_bankcard_cnt_emergency_email
,b.black_bankcard_cnt_emergency_emergency
,b.black_bankcard_cnt_emergency_idcard
,b.black_bankcard_cnt_emergency_myphone
,b.black_bankcard_cnt_idcard_bankcard
,b.black_bankcard_cnt_idcard_companyphone
,b.black_bankcard_cnt_idcard_contact
,b.black_bankcard_cnt_idcard_device
,b.black_bankcard_cnt_idcard_email
,b.black_bankcard_cnt_idcard_emergency
,b.black_bankcard_cnt_idcard_idcard
,b.black_bankcard_cnt_idcard_myphone
,b.black_bankcard_cnt_myphone_bankcard
,b.black_bankcard_cnt_myphone_companyphone
,b.black_bankcard_cnt_myphone_contact
,b.black_bankcard_cnt_myphone_device
,b.black_bankcard_cnt_myphone_email
,b.black_bankcard_cnt_myphone_emergency
,b.black_bankcard_cnt_myphone_idcard
,b.black_bankcard_cnt_myphone_myphone
,b.black_cid_cnt
,b.black_cid_cnt_bankcard_bankcard
,b.black_cid_cnt_bankcard_companyphone
,b.black_cid_cnt_bankcard_contact
,b.black_cid_cnt_bankcard_device
,b.black_cid_cnt_bankcard_email
,b.black_cid_cnt_bankcard_emergency
,b.black_cid_cnt_bankcard_idcard
,b.black_cid_cnt_bankcard_myphone
,b.black_cid_cnt_companyphone_bankcard
,b.black_cid_cnt_companyphone_companyphone
,b.black_cid_cnt_companyphone_contact
,b.black_cid_cnt_companyphone_device
,b.black_cid_cnt_companyphone_email
,b.black_cid_cnt_companyphone_emergency
,b.black_cid_cnt_companyphone_idcard
,b.black_cid_cnt_companyphone_myphone
,b.black_cid_cnt_contact_bankcard
,b.black_cid_cnt_contact_companyphone
,b.black_cid_cnt_contact_contact
,b.black_cid_cnt_contact_device
,b.black_cid_cnt_contact_email
,b.black_cid_cnt_contact_emergency
,b.black_cid_cnt_contact_idcard
,b.black_cid_cnt_contact_myphone
,b.black_cid_cnt_device_bankcard
,b.black_cid_cnt_device_companyphone
,b.black_cid_cnt_device_contact
,b.black_cid_cnt_device_device
,b.black_cid_cnt_device_email
,b.black_cid_cnt_device_emergency
,b.black_cid_cnt_device_idcard
,b.black_cid_cnt_device_myphone
,b.black_cid_cnt_email_bankcard
,b.black_cid_cnt_email_companyphone
,b.black_cid_cnt_email_contact
,b.black_cid_cnt_email_device
,b.black_cid_cnt_email_email
,b.black_cid_cnt_email_emergency
,b.black_cid_cnt_email_idcard
,b.black_cid_cnt_email_myphone
,b.black_cid_cnt_emergency_bankcard
,b.black_cid_cnt_emergency_companyphone
,b.black_cid_cnt_emergency_contact
,b.black_cid_cnt_emergency_device
,b.black_cid_cnt_emergency_email
,b.black_cid_cnt_emergency_emergency
,b.black_cid_cnt_emergency_idcard
,b.black_cid_cnt_emergency_myphone
,b.black_cid_cnt_idcard_bankcard
,b.black_cid_cnt_idcard_companyphone
,b.black_cid_cnt_idcard_contact
,b.black_cid_cnt_idcard_device
,b.black_cid_cnt_idcard_email
,b.black_cid_cnt_idcard_emergency
,b.black_cid_cnt_idcard_idcard
,b.black_cid_cnt_idcard_myphone
,b.black_cid_cnt_myphone_bankcard
,b.black_cid_cnt_myphone_companyphone
,b.black_cid_cnt_myphone_contact
,b.black_cid_cnt_myphone_device
,b.black_cid_cnt_myphone_email
,b.black_cid_cnt_myphone_emergency
,b.black_cid_cnt_myphone_idcard
,b.black_cid_cnt_myphone_myphone
,b.black_company_phone_cnt
,b.black_company_phone_cnt_bankcard_bankcard
,b.black_company_phone_cnt_bankcard_companyphone
,b.black_company_phone_cnt_bankcard_contact
,b.black_company_phone_cnt_bankcard_device
,b.black_company_phone_cnt_bankcard_email
,b.black_company_phone_cnt_bankcard_emergency
,b.black_company_phone_cnt_bankcard_idcard
,b.black_company_phone_cnt_bankcard_myphone
,b.black_company_phone_cnt_companyphone_bankcard
,b.black_company_phone_cnt_companyphone_companyphone
,b.black_company_phone_cnt_companyphone_contact
,b.black_company_phone_cnt_companyphone_device
,b.black_company_phone_cnt_companyphone_email
,b.black_company_phone_cnt_companyphone_emergency
,b.black_company_phone_cnt_companyphone_idcard
,b.black_company_phone_cnt_companyphone_myphone
,b.black_company_phone_cnt_contact_bankcard
,b.black_company_phone_cnt_contact_companyphone
,b.black_company_phone_cnt_contact_contact
,b.black_company_phone_cnt_contact_device
,b.black_company_phone_cnt_contact_email
,b.black_company_phone_cnt_contact_emergency
,b.black_company_phone_cnt_contact_idcard
,b.black_company_phone_cnt_contact_myphone
,b.black_company_phone_cnt_device_bankcard
,b.black_company_phone_cnt_device_companyphone
,b.black_company_phone_cnt_device_contact
,b.black_company_phone_cnt_device_device
,b.black_company_phone_cnt_device_email
,b.black_company_phone_cnt_device_emergency
,b.black_company_phone_cnt_device_idcard
,b.black_company_phone_cnt_device_myphone
,b.black_company_phone_cnt_email_bankcard
,b.black_company_phone_cnt_email_companyphone
,b.black_company_phone_cnt_email_contact
,b.black_company_phone_cnt_email_device
,b.black_company_phone_cnt_email_email
,b.black_company_phone_cnt_email_emergency
,b.black_company_phone_cnt_email_idcard
,b.black_company_phone_cnt_email_myphone
,b.black_company_phone_cnt_emergency_bankcard
,b.black_company_phone_cnt_emergency_companyphone
,b.black_company_phone_cnt_emergency_contact
,b.black_company_phone_cnt_emergency_device
,b.black_company_phone_cnt_emergency_email
,b.black_company_phone_cnt_emergency_emergency
,b.black_company_phone_cnt_emergency_idcard
,b.black_company_phone_cnt_emergency_myphone
,b.black_company_phone_cnt_idcard_bankcard
,b.black_company_phone_cnt_idcard_companyphone
,b.black_company_phone_cnt_idcard_contact
,b.black_company_phone_cnt_idcard_device
,b.black_company_phone_cnt_idcard_email
,b.black_company_phone_cnt_idcard_emergency
,b.black_company_phone_cnt_idcard_idcard
,b.black_company_phone_cnt_idcard_myphone
,b.black_company_phone_cnt_myphone_bankcard
,b.black_company_phone_cnt_myphone_companyphone
,b.black_company_phone_cnt_myphone_contact
,b.black_company_phone_cnt_myphone_device
,b.black_company_phone_cnt_myphone_email
,b.black_company_phone_cnt_myphone_emergency
,b.black_company_phone_cnt_myphone_idcard
,b.black_company_phone_cnt_myphone_myphone
,b.black_contract_cnt
,b.black_contract_cnt_bankcard_bankcard
,b.black_contract_cnt_bankcard_companyphone
,b.black_contract_cnt_bankcard_contact
,b.black_contract_cnt_bankcard_device
,b.black_contract_cnt_bankcard_email
,b.black_contract_cnt_bankcard_emergency
,b.black_contract_cnt_bankcard_idcard
,b.black_contract_cnt_bankcard_myphone
,b.black_contract_cnt_companyphone_bankcard
,b.black_contract_cnt_companyphone_companyphone
,b.black_contract_cnt_companyphone_contact
,b.black_contract_cnt_companyphone_device
,b.black_contract_cnt_companyphone_email
,b.black_contract_cnt_companyphone_emergency
,b.black_contract_cnt_companyphone_idcard
,b.black_contract_cnt_companyphone_myphone
,b.black_contract_cnt_contact_bankcard
,b.black_contract_cnt_contact_companyphone
,b.black_contract_cnt_contact_contact
,b.black_contract_cnt_contact_device
,b.black_contract_cnt_contact_email
,b.black_contract_cnt_contact_emergency
,b.black_contract_cnt_contact_idcard
,b.black_contract_cnt_contact_myphone
,b.black_contract_cnt_device_bankcard
,b.black_contract_cnt_device_companyphone
,b.black_contract_cnt_device_contact
,b.black_contract_cnt_device_device
,b.black_contract_cnt_device_email
,b.black_contract_cnt_device_emergency
,b.black_contract_cnt_device_idcard
,b.black_contract_cnt_device_myphone
,b.black_contract_cnt_email_bankcard
,b.black_contract_cnt_email_companyphone
,b.black_contract_cnt_email_contact
,b.black_contract_cnt_email_device
,b.black_contract_cnt_email_email
,b.black_contract_cnt_email_emergency
,b.black_contract_cnt_email_idcard
,b.black_contract_cnt_email_myphone
,b.black_contract_cnt_emergency_bankcard
,b.black_contract_cnt_emergency_companyphone
,b.black_contract_cnt_emergency_contact
,b.black_contract_cnt_emergency_device
,b.black_contract_cnt_emergency_email
,b.black_contract_cnt_emergency_emergency
,b.black_contract_cnt_emergency_idcard
,b.black_contract_cnt_emergency_myphone
,b.black_contract_cnt_idcard_bankcard
,b.black_contract_cnt_idcard_companyphone
,b.black_contract_cnt_idcard_contact
,b.black_contract_cnt_idcard_device
,b.black_contract_cnt_idcard_email
,b.black_contract_cnt_idcard_emergency
,b.black_contract_cnt_idcard_idcard
,b.black_contract_cnt_idcard_myphone
,b.black_contract_cnt_myphone_bankcard
,b.black_contract_cnt_myphone_companyphone
,b.black_contract_cnt_myphone_contact
,b.black_contract_cnt_myphone_device
,b.black_contract_cnt_myphone_email
,b.black_contract_cnt_myphone_emergency
,b.black_contract_cnt_myphone_idcard
,b.black_contract_cnt_myphone_myphone
,b.black_email_cnt
,b.black_email_cnt_bankcard_bankcard
,b.black_email_cnt_bankcard_companyphone
,b.black_email_cnt_bankcard_contact
,b.black_email_cnt_bankcard_device
,b.black_email_cnt_bankcard_email
,b.black_email_cnt_bankcard_emergency
,b.black_email_cnt_bankcard_idcard
,b.black_email_cnt_bankcard_myphone
,b.black_email_cnt_companyphone_bankcard
,b.black_email_cnt_companyphone_companyphone
,b.black_email_cnt_companyphone_contact
,b.black_email_cnt_companyphone_device
,b.black_email_cnt_companyphone_email
,b.black_email_cnt_companyphone_emergency
,b.black_email_cnt_companyphone_idcard
,b.black_email_cnt_companyphone_myphone
,b.black_email_cnt_contact_bankcard
,b.black_email_cnt_contact_companyphone
,b.black_email_cnt_contact_contact
,b.black_email_cnt_contact_device
,b.black_email_cnt_contact_email
,b.black_email_cnt_contact_emergency
,b.black_email_cnt_contact_idcard
,b.black_email_cnt_contact_myphone
,b.black_email_cnt_device_bankcard
,b.black_email_cnt_device_companyphone
,b.black_email_cnt_device_contact
,b.black_email_cnt_device_device
,b.black_email_cnt_device_email
,b.black_email_cnt_device_emergency
,b.black_email_cnt_device_idcard
,b.black_email_cnt_device_myphone
,b.black_email_cnt_email_bankcard
,b.black_email_cnt_email_companyphone
,b.black_email_cnt_email_contact
,b.black_email_cnt_email_device
,b.black_email_cnt_email_email
,b.black_email_cnt_email_emergency
,b.black_email_cnt_email_idcard
,b.black_email_cnt_email_myphone
,b.black_email_cnt_emergency_bankcard
,b.black_email_cnt_emergency_companyphone
,b.black_email_cnt_emergency_contact
,b.black_email_cnt_emergency_device
,b.black_email_cnt_emergency_email
,b.black_email_cnt_emergency_emergency
,b.black_email_cnt_emergency_idcard
,b.black_email_cnt_emergency_myphone
,b.black_email_cnt_idcard_bankcard
,b.black_email_cnt_idcard_companyphone
,b.black_email_cnt_idcard_contact
,b.black_email_cnt_idcard_device
,b.black_email_cnt_idcard_email
,b.black_email_cnt_idcard_emergency
,b.black_email_cnt_idcard_idcard
,b.black_email_cnt_idcard_myphone
,b.black_email_cnt_myphone_bankcard
,b.black_email_cnt_myphone_companyphone
,b.black_email_cnt_myphone_contact
,b.black_email_cnt_myphone_device
,b.black_email_cnt_myphone_email
,b.black_email_cnt_myphone_emergency
,b.black_email_cnt_myphone_idcard
,b.black_email_cnt_myphone_myphone
,b.black_imei_cnt
,b.black_imei_cnt_bankcard_bankcard
,b.black_imei_cnt_bankcard_companyphone
,b.black_imei_cnt_bankcard_contact
,b.black_imei_cnt_bankcard_device
,b.black_imei_cnt_bankcard_email
,b.black_imei_cnt_bankcard_emergency
,b.black_imei_cnt_bankcard_idcard
,b.black_imei_cnt_bankcard_myphone
,b.black_imei_cnt_companyphone_bankcard
,b.black_imei_cnt_companyphone_companyphone
,b.black_imei_cnt_companyphone_contact
,b.black_imei_cnt_companyphone_device
,b.black_imei_cnt_companyphone_email
,b.black_imei_cnt_companyphone_emergency
,b.black_imei_cnt_companyphone_idcard
,b.black_imei_cnt_companyphone_myphone
,b.black_imei_cnt_contact_bankcard
,b.black_imei_cnt_contact_companyphone
,b.black_imei_cnt_contact_contact
,b.black_imei_cnt_contact_device
,b.black_imei_cnt_contact_email
,b.black_imei_cnt_contact_emergency
,b.black_imei_cnt_contact_idcard
,b.black_imei_cnt_contact_myphone
,b.black_imei_cnt_device_bankcard
,b.black_imei_cnt_device_companyphone
,b.black_imei_cnt_device_contact
,b.black_imei_cnt_device_device
,b.black_imei_cnt_device_email
,b.black_imei_cnt_device_emergency
,b.black_imei_cnt_device_idcard
,b.black_imei_cnt_device_myphone
,b.black_imei_cnt_email_bankcard
,b.black_imei_cnt_email_companyphone
,b.black_imei_cnt_email_contact
,b.black_imei_cnt_email_device
,b.black_imei_cnt_email_email
,b.black_imei_cnt_email_emergency
,b.black_imei_cnt_email_idcard
,b.black_imei_cnt_email_myphone
,b.black_imei_cnt_emergency_bankcard
,b.black_imei_cnt_emergency_companyphone
,b.black_imei_cnt_emergency_contact
,b.black_imei_cnt_emergency_device
,b.black_imei_cnt_emergency_email
,b.black_imei_cnt_emergency_emergency
,b.black_imei_cnt_emergency_idcard
,b.black_imei_cnt_emergency_myphone
,b.black_imei_cnt_idcard_bankcard
,b.black_imei_cnt_idcard_companyphone
,b.black_imei_cnt_idcard_contact
,b.black_imei_cnt_idcard_device
,b.black_imei_cnt_idcard_email
,b.black_imei_cnt_idcard_emergency
,b.black_imei_cnt_idcard_idcard
,b.black_imei_cnt_idcard_myphone
,b.black_imei_cnt_myphone_bankcard
,b.black_imei_cnt_myphone_companyphone
,b.black_imei_cnt_myphone_contact
,b.black_imei_cnt_myphone_device
,b.black_imei_cnt_myphone_email
,b.black_imei_cnt_myphone_emergency
,b.black_imei_cnt_myphone_idcard
,b.black_imei_cnt_myphone_myphone
,b.black_mobile_cnt
,b.black_mobile_cnt_bankcard_bankcard
,b.black_mobile_cnt_bankcard_companyphone
,b.black_mobile_cnt_bankcard_contact
,b.black_mobile_cnt_bankcard_device
,b.black_mobile_cnt_bankcard_email
,b.black_mobile_cnt_bankcard_emergency
,b.black_mobile_cnt_bankcard_idcard
,b.black_mobile_cnt_bankcard_myphone
,b.black_mobile_cnt_companyphone_bankcard
,b.black_mobile_cnt_companyphone_companyphone
,b.black_mobile_cnt_companyphone_contact
,b.black_mobile_cnt_companyphone_device
,b.black_mobile_cnt_companyphone_email
,b.black_mobile_cnt_companyphone_emergency
,b.black_mobile_cnt_companyphone_idcard
,b.black_mobile_cnt_companyphone_myphone
,b.black_mobile_cnt_contact_bankcard
,b.black_mobile_cnt_contact_companyphone
,b.black_mobile_cnt_contact_contact
,b.black_mobile_cnt_contact_device
,b.black_mobile_cnt_contact_email
,b.black_mobile_cnt_contact_emergency
,b.black_mobile_cnt_contact_idcard
,b.black_mobile_cnt_contact_myphone
,b.black_mobile_cnt_device_bankcard
,b.black_mobile_cnt_device_companyphone
,b.black_mobile_cnt_device_contact
,b.black_mobile_cnt_device_device
,b.black_mobile_cnt_device_email
,b.black_mobile_cnt_device_emergency
,b.black_mobile_cnt_device_idcard
,b.black_mobile_cnt_device_myphone
,b.black_mobile_cnt_email_bankcard
,b.black_mobile_cnt_email_companyphone
,b.black_mobile_cnt_email_contact
,b.black_mobile_cnt_email_device
,b.black_mobile_cnt_email_email
,b.black_mobile_cnt_email_emergency
,b.black_mobile_cnt_email_idcard
,b.black_mobile_cnt_email_myphone
,b.black_mobile_cnt_emergency_bankcard
,b.black_mobile_cnt_emergency_companyphone
,b.black_mobile_cnt_emergency_contact
,b.black_mobile_cnt_emergency_device
,b.black_mobile_cnt_emergency_email
,b.black_mobile_cnt_emergency_emergency
,b.black_mobile_cnt_emergency_idcard
,b.black_mobile_cnt_emergency_myphone
,b.black_mobile_cnt_idcard_bankcard
,b.black_mobile_cnt_idcard_companyphone
,b.black_mobile_cnt_idcard_contact
,b.black_mobile_cnt_idcard_device
,b.black_mobile_cnt_idcard_email
,b.black_mobile_cnt_idcard_emergency
,b.black_mobile_cnt_idcard_idcard
,b.black_mobile_cnt_idcard_myphone
,b.black_mobile_cnt_myphone_bankcard
,b.black_mobile_cnt_myphone_companyphone
,b.black_mobile_cnt_myphone_contact
,b.black_mobile_cnt_myphone_device
,b.black_mobile_cnt_myphone_email
,b.black_mobile_cnt_myphone_emergency
,b.black_mobile_cnt_myphone_idcard
,b.black_mobile_cnt_myphone_myphone
,b.cid_cnt
,b.cid_cnt_bankcard_bankcard
,b.cid_cnt_bankcard_companyphone
,b.cid_cnt_bankcard_contact
,b.cid_cnt_bankcard_device
,b.cid_cnt_bankcard_email
,b.cid_cnt_bankcard_emergency
,b.cid_cnt_bankcard_idcard
,b.cid_cnt_bankcard_myphone
,b.cid_cnt_companyphone_bankcard
,b.cid_cnt_companyphone_companyphone
,b.cid_cnt_companyphone_contact
,b.cid_cnt_companyphone_device
,b.cid_cnt_companyphone_email
,b.cid_cnt_companyphone_emergency
,b.cid_cnt_companyphone_idcard
,b.cid_cnt_companyphone_myphone
,b.cid_cnt_contact_bankcard
,b.cid_cnt_contact_companyphone
,b.cid_cnt_contact_contact
,b.cid_cnt_contact_device
,b.cid_cnt_contact_email
,b.cid_cnt_contact_emergency
,b.cid_cnt_contact_idcard
,b.cid_cnt_contact_myphone
,b.cid_cnt_device_bankcard
,b.cid_cnt_device_companyphone
,b.cid_cnt_device_contact
,b.cid_cnt_device_device
,b.cid_cnt_device_email
,b.cid_cnt_device_emergency
,b.cid_cnt_device_idcard
,b.cid_cnt_device_myphone
,b.cid_cnt_email_bankcard
,b.cid_cnt_email_companyphone
,b.cid_cnt_email_contact
,b.cid_cnt_email_device
,b.cid_cnt_email_email
,b.cid_cnt_email_emergency
,b.cid_cnt_email_idcard
,b.cid_cnt_email_myphone
,b.cid_cnt_emergency_bankcard
,b.cid_cnt_emergency_companyphone
,b.cid_cnt_emergency_contact
,b.cid_cnt_emergency_device
,b.cid_cnt_emergency_email
,b.cid_cnt_emergency_emergency
,b.cid_cnt_emergency_idcard
,b.cid_cnt_emergency_myphone
,b.cid_cnt_idcard_bankcard
,b.cid_cnt_idcard_companyphone
,b.cid_cnt_idcard_contact
,b.cid_cnt_idcard_device
,b.cid_cnt_idcard_email
,b.cid_cnt_idcard_emergency
,b.cid_cnt_idcard_idcard
,b.cid_cnt_idcard_myphone
,b.cid_cnt_myphone_bankcard
,b.cid_cnt_myphone_companyphone
,b.cid_cnt_myphone_contact
,b.cid_cnt_myphone_device
,b.cid_cnt_myphone_email
,b.cid_cnt_myphone_emergency
,b.cid_cnt_myphone_idcard
,b.cid_cnt_myphone_myphone
,b.cid_cnt1
,b.cid_cnt1_bankcard_bankcard
,b.cid_cnt1_bankcard_companyphone
,b.cid_cnt1_bankcard_contact
,b.cid_cnt1_bankcard_device
,b.cid_cnt1_bankcard_email
,b.cid_cnt1_bankcard_emergency
,b.cid_cnt1_bankcard_idcard
,b.cid_cnt1_bankcard_myphone
,b.cid_cnt1_companyphone_bankcard
,b.cid_cnt1_companyphone_companyphone
,b.cid_cnt1_companyphone_contact
,b.cid_cnt1_companyphone_device
,b.cid_cnt1_companyphone_email
,b.cid_cnt1_companyphone_emergency
,b.cid_cnt1_companyphone_idcard
,b.cid_cnt1_companyphone_myphone
,b.cid_cnt1_contact_bankcard
,b.cid_cnt1_contact_companyphone
,b.cid_cnt1_contact_contact
,b.cid_cnt1_contact_device
,b.cid_cnt1_contact_email
,b.cid_cnt1_contact_emergency
,b.cid_cnt1_contact_idcard
,b.cid_cnt1_contact_myphone
,b.cid_cnt1_device_bankcard
,b.cid_cnt1_device_companyphone
,b.cid_cnt1_device_contact
,b.cid_cnt1_device_device
,b.cid_cnt1_device_email
,b.cid_cnt1_device_emergency
,b.cid_cnt1_device_idcard
,b.cid_cnt1_device_myphone
,b.cid_cnt1_email_bankcard
,b.cid_cnt1_email_companyphone
,b.cid_cnt1_email_contact
,b.cid_cnt1_email_device
,b.cid_cnt1_email_email
,b.cid_cnt1_email_emergency
,b.cid_cnt1_email_idcard
,b.cid_cnt1_email_myphone
,b.cid_cnt1_emergency_bankcard
,b.cid_cnt1_emergency_companyphone
,b.cid_cnt1_emergency_contact
,b.cid_cnt1_emergency_device
,b.cid_cnt1_emergency_email
,b.cid_cnt1_emergency_emergency
,b.cid_cnt1_emergency_idcard
,b.cid_cnt1_emergency_myphone
,b.cid_cnt1_idcard_bankcard
,b.cid_cnt1_idcard_companyphone
,b.cid_cnt1_idcard_contact
,b.cid_cnt1_idcard_device
,b.cid_cnt1_idcard_email
,b.cid_cnt1_idcard_emergency
,b.cid_cnt1_idcard_idcard
,b.cid_cnt1_idcard_myphone
,b.cid_cnt1_myphone_bankcard
,b.cid_cnt1_myphone_companyphone
,b.cid_cnt1_myphone_contact
,b.cid_cnt1_myphone_device
,b.cid_cnt1_myphone_email
,b.cid_cnt1_myphone_emergency
,b.cid_cnt1_myphone_idcard
,b.cid_cnt1_myphone_myphone
,b.cid_cnt3
,b.cid_cnt3_bankcard_bankcard
,b.cid_cnt3_bankcard_companyphone
,b.cid_cnt3_bankcard_contact
,b.cid_cnt3_bankcard_device
,b.cid_cnt3_bankcard_email
,b.cid_cnt3_bankcard_emergency
,b.cid_cnt3_bankcard_idcard
,b.cid_cnt3_bankcard_myphone
,b.cid_cnt3_companyphone_bankcard
,b.cid_cnt3_companyphone_companyphone
,b.cid_cnt3_companyphone_contact
,b.cid_cnt3_companyphone_device
,b.cid_cnt3_companyphone_email
,b.cid_cnt3_companyphone_emergency
,b.cid_cnt3_companyphone_idcard
,b.cid_cnt3_companyphone_myphone
,b.cid_cnt3_contact_bankcard
,b.cid_cnt3_contact_companyphone
,b.cid_cnt3_contact_contact
,b.cid_cnt3_contact_device
,b.cid_cnt3_contact_email
,b.cid_cnt3_contact_emergency
,b.cid_cnt3_contact_idcard
,b.cid_cnt3_contact_myphone
,b.cid_cnt3_device_bankcard
,b.cid_cnt3_device_companyphone
,b.cid_cnt3_device_contact
,b.cid_cnt3_device_device
,b.cid_cnt3_device_email
,b.cid_cnt3_device_emergency
,b.cid_cnt3_device_idcard
,b.cid_cnt3_device_myphone
,b.cid_cnt3_email_bankcard
,b.cid_cnt3_email_companyphone
,b.cid_cnt3_email_contact
,b.cid_cnt3_email_device
,b.cid_cnt3_email_email
,b.cid_cnt3_email_emergency
,b.cid_cnt3_email_idcard
,b.cid_cnt3_email_myphone
,b.cid_cnt3_emergency_bankcard
,b.cid_cnt3_emergency_companyphone
,b.cid_cnt3_emergency_contact
,b.cid_cnt3_emergency_device
,b.cid_cnt3_emergency_email
,b.cid_cnt3_emergency_emergency
,b.cid_cnt3_emergency_idcard
,b.cid_cnt3_emergency_myphone
,b.cid_cnt3_idcard_bankcard
,b.cid_cnt3_idcard_companyphone
,b.cid_cnt3_idcard_contact
,b.cid_cnt3_idcard_device
,b.cid_cnt3_idcard_email
,b.cid_cnt3_idcard_emergency
,b.cid_cnt3_idcard_idcard
,b.cid_cnt3_idcard_myphone
,b.cid_cnt3_myphone_bankcard
,b.cid_cnt3_myphone_companyphone
,b.cid_cnt3_myphone_contact
,b.cid_cnt3_myphone_device
,b.cid_cnt3_myphone_email
,b.cid_cnt3_myphone_emergency
,b.cid_cnt3_myphone_idcard
,b.cid_cnt3_myphone_myphone
,b.cid_cnt30
,b.cid_cnt30_bankcard_bankcard
,b.cid_cnt30_bankcard_companyphone
,b.cid_cnt30_bankcard_contact
,b.cid_cnt30_bankcard_device
,b.cid_cnt30_bankcard_email
,b.cid_cnt30_bankcard_emergency
,b.cid_cnt30_bankcard_idcard
,b.cid_cnt30_bankcard_myphone
,b.cid_cnt30_companyphone_bankcard
,b.cid_cnt30_companyphone_companyphone
,b.cid_cnt30_companyphone_contact
,b.cid_cnt30_companyphone_device
,b.cid_cnt30_companyphone_email
,b.cid_cnt30_companyphone_emergency
,b.cid_cnt30_companyphone_idcard
,b.cid_cnt30_companyphone_myphone
,b.cid_cnt30_contact_bankcard
,b.cid_cnt30_contact_companyphone
,b.cid_cnt30_contact_contact
,b.cid_cnt30_contact_device
,b.cid_cnt30_contact_email
,b.cid_cnt30_contact_emergency
,b.cid_cnt30_contact_idcard
,b.cid_cnt30_contact_myphone
,b.cid_cnt30_device_bankcard
,b.cid_cnt30_device_companyphone
,b.cid_cnt30_device_contact
,b.cid_cnt30_device_device
,b.cid_cnt30_device_email
,b.cid_cnt30_device_emergency
,b.cid_cnt30_device_idcard
,b.cid_cnt30_device_myphone
,b.cid_cnt30_email_bankcard
,b.cid_cnt30_email_companyphone
,b.cid_cnt30_email_contact
,b.cid_cnt30_email_device
,b.cid_cnt30_email_email
,b.cid_cnt30_email_emergency
,b.cid_cnt30_email_idcard
,b.cid_cnt30_email_myphone
,b.cid_cnt30_emergency_bankcard
,b.cid_cnt30_emergency_companyphone
,b.cid_cnt30_emergency_contact
,b.cid_cnt30_emergency_device
,b.cid_cnt30_emergency_email
,b.cid_cnt30_emergency_emergency
,b.cid_cnt30_emergency_idcard
,b.cid_cnt30_emergency_myphone
,b.cid_cnt30_idcard_bankcard
,b.cid_cnt30_idcard_companyphone
,b.cid_cnt30_idcard_contact
,b.cid_cnt30_idcard_device
,b.cid_cnt30_idcard_email
,b.cid_cnt30_idcard_emergency
,b.cid_cnt30_idcard_idcard
,b.cid_cnt30_idcard_myphone
,b.cid_cnt30_myphone_bankcard
,b.cid_cnt30_myphone_companyphone
,b.cid_cnt30_myphone_contact
,b.cid_cnt30_myphone_device
,b.cid_cnt30_myphone_email
,b.cid_cnt30_myphone_emergency
,b.cid_cnt30_myphone_idcard
,b.cid_cnt30_myphone_myphone
,b.cid_cnt7
,b.cid_cnt7_bankcard_bankcard
,b.cid_cnt7_bankcard_companyphone
,b.cid_cnt7_bankcard_contact
,b.cid_cnt7_bankcard_device
,b.cid_cnt7_bankcard_email
,b.cid_cnt7_bankcard_emergency
,b.cid_cnt7_bankcard_idcard
,b.cid_cnt7_bankcard_myphone
,b.cid_cnt7_companyphone_bankcard
,b.cid_cnt7_companyphone_companyphone
,b.cid_cnt7_companyphone_contact
,b.cid_cnt7_companyphone_device
,b.cid_cnt7_companyphone_email
,b.cid_cnt7_companyphone_emergency
,b.cid_cnt7_companyphone_idcard
,b.cid_cnt7_companyphone_myphone
,b.cid_cnt7_contact_bankcard
,b.cid_cnt7_contact_companyphone
,b.cid_cnt7_contact_contact
,b.cid_cnt7_contact_device
,b.cid_cnt7_contact_email
,b.cid_cnt7_contact_emergency
,b.cid_cnt7_contact_idcard
,b.cid_cnt7_contact_myphone
,b.cid_cnt7_device_bankcard
,b.cid_cnt7_device_companyphone
,b.cid_cnt7_device_contact
,b.cid_cnt7_device_device
,b.cid_cnt7_device_email
,b.cid_cnt7_device_emergency
,b.cid_cnt7_device_idcard
,b.cid_cnt7_device_myphone
,b.cid_cnt7_email_bankcard
,b.cid_cnt7_email_companyphone
,b.cid_cnt7_email_contact
,b.cid_cnt7_email_device
,b.cid_cnt7_email_email
,b.cid_cnt7_email_emergency
,b.cid_cnt7_email_idcard
,b.cid_cnt7_email_myphone
,b.cid_cnt7_emergency_bankcard
,b.cid_cnt7_emergency_companyphone
,b.cid_cnt7_emergency_contact
,b.cid_cnt7_emergency_device
,b.cid_cnt7_emergency_email
,b.cid_cnt7_emergency_emergency
,b.cid_cnt7_emergency_idcard
,b.cid_cnt7_emergency_myphone
,b.cid_cnt7_idcard_bankcard
,b.cid_cnt7_idcard_companyphone
,b.cid_cnt7_idcard_contact
,b.cid_cnt7_idcard_device
,b.cid_cnt7_idcard_email
,b.cid_cnt7_idcard_emergency
,b.cid_cnt7_idcard_idcard
,b.cid_cnt7_idcard_myphone
,b.cid_cnt7_myphone_bankcard
,b.cid_cnt7_myphone_companyphone
,b.cid_cnt7_myphone_contact
,b.cid_cnt7_myphone_device
,b.cid_cnt7_myphone_email
,b.cid_cnt7_myphone_emergency
,b.cid_cnt7_myphone_idcard
,b.cid_cnt7_myphone_myphone
,b.company_phone_cnt
,b.company_phone_cnt_bankcard_bankcard
,b.company_phone_cnt_bankcard_companyphone
,b.company_phone_cnt_bankcard_contact
,b.company_phone_cnt_bankcard_device
,b.company_phone_cnt_bankcard_email
,b.company_phone_cnt_bankcard_emergency
,b.company_phone_cnt_bankcard_idcard
,b.company_phone_cnt_bankcard_myphone
,b.company_phone_cnt_companyphone_bankcard
,b.company_phone_cnt_companyphone_companyphone
,b.company_phone_cnt_companyphone_contact
,b.company_phone_cnt_companyphone_device
,b.company_phone_cnt_companyphone_email
,b.company_phone_cnt_companyphone_emergency
,b.company_phone_cnt_companyphone_idcard
,b.company_phone_cnt_companyphone_myphone
,b.company_phone_cnt_contact_bankcard
,b.company_phone_cnt_contact_companyphone
,b.company_phone_cnt_contact_contact
,b.company_phone_cnt_contact_device
,b.company_phone_cnt_contact_email
,b.company_phone_cnt_contact_emergency
,b.company_phone_cnt_contact_idcard
,b.company_phone_cnt_contact_myphone
,b.company_phone_cnt_device_bankcard
,b.company_phone_cnt_device_companyphone
,b.company_phone_cnt_device_contact
,b.company_phone_cnt_device_device
,b.company_phone_cnt_device_email
,b.company_phone_cnt_device_emergency
,b.company_phone_cnt_device_idcard
,b.company_phone_cnt_device_myphone
,b.company_phone_cnt_email_bankcard
,b.company_phone_cnt_email_companyphone
,b.company_phone_cnt_email_contact
,b.company_phone_cnt_email_device
,b.company_phone_cnt_email_email
,b.company_phone_cnt_email_emergency
,b.company_phone_cnt_email_idcard
,b.company_phone_cnt_email_myphone
,b.company_phone_cnt_emergency_bankcard
,b.company_phone_cnt_emergency_companyphone
,b.company_phone_cnt_emergency_contact
,b.company_phone_cnt_emergency_device
,b.company_phone_cnt_emergency_email
,b.company_phone_cnt_emergency_emergency
,b.company_phone_cnt_emergency_idcard
,b.company_phone_cnt_emergency_myphone
,b.company_phone_cnt_idcard_bankcard
,b.company_phone_cnt_idcard_companyphone
,b.company_phone_cnt_idcard_contact
,b.company_phone_cnt_idcard_device
,b.company_phone_cnt_idcard_email
,b.company_phone_cnt_idcard_emergency
,b.company_phone_cnt_idcard_idcard
,b.company_phone_cnt_idcard_myphone
,b.company_phone_cnt_myphone_bankcard
,b.company_phone_cnt_myphone_companyphone
,b.company_phone_cnt_myphone_contact
,b.company_phone_cnt_myphone_device
,b.company_phone_cnt_myphone_email
,b.company_phone_cnt_myphone_emergency
,b.company_phone_cnt_myphone_idcard
,b.company_phone_cnt_myphone_myphone
,b.company_phone_cnt1
,b.company_phone_cnt1_bankcard_bankcard
,b.company_phone_cnt1_bankcard_companyphone
,b.company_phone_cnt1_bankcard_contact
,b.company_phone_cnt1_bankcard_device
,b.company_phone_cnt1_bankcard_email
,b.company_phone_cnt1_bankcard_emergency
,b.company_phone_cnt1_bankcard_idcard
,b.company_phone_cnt1_bankcard_myphone
,b.company_phone_cnt1_companyphone_bankcard
,b.company_phone_cnt1_companyphone_companyphone
,b.company_phone_cnt1_companyphone_contact
,b.company_phone_cnt1_companyphone_device
,b.company_phone_cnt1_companyphone_email
,b.company_phone_cnt1_companyphone_emergency
,b.company_phone_cnt1_companyphone_idcard
,b.company_phone_cnt1_companyphone_myphone
,b.company_phone_cnt1_contact_bankcard
,b.company_phone_cnt1_contact_companyphone
,b.company_phone_cnt1_contact_contact
,b.company_phone_cnt1_contact_device
,b.company_phone_cnt1_contact_email
,b.company_phone_cnt1_contact_emergency
,b.company_phone_cnt1_contact_idcard
,b.company_phone_cnt1_contact_myphone
,b.company_phone_cnt1_device_bankcard
,b.company_phone_cnt1_device_companyphone
,b.company_phone_cnt1_device_contact
,b.company_phone_cnt1_device_device
,b.company_phone_cnt1_device_email
,b.company_phone_cnt1_device_emergency
,b.company_phone_cnt1_device_idcard
,b.company_phone_cnt1_device_myphone
,b.company_phone_cnt1_email_bankcard
,b.company_phone_cnt1_email_companyphone
,b.company_phone_cnt1_email_contact
,b.company_phone_cnt1_email_device
,b.company_phone_cnt1_email_email
,b.company_phone_cnt1_email_emergency
,b.company_phone_cnt1_email_idcard
,b.company_phone_cnt1_email_myphone
,b.company_phone_cnt1_emergency_bankcard
,b.company_phone_cnt1_emergency_companyphone
,b.company_phone_cnt1_emergency_contact
,b.company_phone_cnt1_emergency_device
,b.company_phone_cnt1_emergency_email
,b.company_phone_cnt1_emergency_emergency
,b.company_phone_cnt1_emergency_idcard
,b.company_phone_cnt1_emergency_myphone
,b.company_phone_cnt1_idcard_bankcard
,b.company_phone_cnt1_idcard_companyphone
,b.company_phone_cnt1_idcard_contact
,b.company_phone_cnt1_idcard_device
,b.company_phone_cnt1_idcard_email
,b.company_phone_cnt1_idcard_emergency
,b.company_phone_cnt1_idcard_idcard
,b.company_phone_cnt1_idcard_myphone
,b.company_phone_cnt1_myphone_bankcard
,b.company_phone_cnt1_myphone_companyphone
,b.company_phone_cnt1_myphone_contact
,b.company_phone_cnt1_myphone_device
,b.company_phone_cnt1_myphone_email
,b.company_phone_cnt1_myphone_emergency
,b.company_phone_cnt1_myphone_idcard
,b.company_phone_cnt1_myphone_myphone
,b.company_phone_cnt3
,b.company_phone_cnt3_bankcard_bankcard
,b.company_phone_cnt3_bankcard_companyphone
,b.company_phone_cnt3_bankcard_contact
,b.company_phone_cnt3_bankcard_device
,b.company_phone_cnt3_bankcard_email
,b.company_phone_cnt3_bankcard_emergency
,b.company_phone_cnt3_bankcard_idcard
,b.company_phone_cnt3_bankcard_myphone
,b.company_phone_cnt3_companyphone_bankcard
,b.company_phone_cnt3_companyphone_companyphone
,b.company_phone_cnt3_companyphone_contact
,b.company_phone_cnt3_companyphone_device
,b.company_phone_cnt3_companyphone_email
,b.company_phone_cnt3_companyphone_emergency
,b.company_phone_cnt3_companyphone_idcard
,b.company_phone_cnt3_companyphone_myphone
,b.company_phone_cnt3_contact_bankcard
,b.company_phone_cnt3_contact_companyphone
,b.company_phone_cnt3_contact_contact
,b.company_phone_cnt3_contact_device
,b.company_phone_cnt3_contact_email
,b.company_phone_cnt3_contact_emergency
,b.company_phone_cnt3_contact_idcard
,b.company_phone_cnt3_contact_myphone
,b.company_phone_cnt3_device_bankcard
,b.company_phone_cnt3_device_companyphone
,b.company_phone_cnt3_device_contact
,b.company_phone_cnt3_device_device
,b.company_phone_cnt3_device_email
,b.company_phone_cnt3_device_emergency
,b.company_phone_cnt3_device_idcard
,b.company_phone_cnt3_device_myphone
,b.company_phone_cnt3_email_bankcard
,b.company_phone_cnt3_email_companyphone
,b.company_phone_cnt3_email_contact
,b.company_phone_cnt3_email_device
,b.company_phone_cnt3_email_email
,b.company_phone_cnt3_email_emergency
,b.company_phone_cnt3_email_idcard
,b.company_phone_cnt3_email_myphone
,b.company_phone_cnt3_emergency_bankcard
,b.company_phone_cnt3_emergency_companyphone
,b.company_phone_cnt3_emergency_contact
,b.company_phone_cnt3_emergency_device
,b.company_phone_cnt3_emergency_email
,b.company_phone_cnt3_emergency_emergency
,b.company_phone_cnt3_emergency_idcard
,b.company_phone_cnt3_emergency_myphone
,b.company_phone_cnt3_idcard_bankcard
,b.company_phone_cnt3_idcard_companyphone
,b.company_phone_cnt3_idcard_contact
,b.company_phone_cnt3_idcard_device
,b.company_phone_cnt3_idcard_email
,b.company_phone_cnt3_idcard_emergency
,b.company_phone_cnt3_idcard_idcard
,b.company_phone_cnt3_idcard_myphone
,b.company_phone_cnt3_myphone_bankcard
,b.company_phone_cnt3_myphone_companyphone
,b.company_phone_cnt3_myphone_contact
,b.company_phone_cnt3_myphone_device
,b.company_phone_cnt3_myphone_email
,b.company_phone_cnt3_myphone_emergency
,b.company_phone_cnt3_myphone_idcard
,b.company_phone_cnt3_myphone_myphone
,b.company_phone_cnt30
,b.company_phone_cnt30_bankcard_bankcard
,b.company_phone_cnt30_bankcard_companyphone
,b.company_phone_cnt30_bankcard_contact
,b.company_phone_cnt30_bankcard_device
,b.company_phone_cnt30_bankcard_email
,b.company_phone_cnt30_bankcard_emergency
,b.company_phone_cnt30_bankcard_idcard
,b.company_phone_cnt30_bankcard_myphone
,b.company_phone_cnt30_companyphone_bankcard
,b.company_phone_cnt30_companyphone_companyphone
,b.company_phone_cnt30_companyphone_contact
,b.company_phone_cnt30_companyphone_device
,b.company_phone_cnt30_companyphone_email
,b.company_phone_cnt30_companyphone_emergency
,b.company_phone_cnt30_companyphone_idcard
,b.company_phone_cnt30_companyphone_myphone
,b.company_phone_cnt30_contact_bankcard
,b.company_phone_cnt30_contact_companyphone
,b.company_phone_cnt30_contact_contact
,b.company_phone_cnt30_contact_device
,b.company_phone_cnt30_contact_email
,b.company_phone_cnt30_contact_emergency
,b.company_phone_cnt30_contact_idcard
,b.company_phone_cnt30_contact_myphone
,b.company_phone_cnt30_device_bankcard
,b.company_phone_cnt30_device_companyphone
,b.company_phone_cnt30_device_contact
,b.company_phone_cnt30_device_device
,b.company_phone_cnt30_device_email
,b.company_phone_cnt30_device_emergency
,b.company_phone_cnt30_device_idcard
,b.company_phone_cnt30_device_myphone
,b.company_phone_cnt30_email_bankcard
,b.company_phone_cnt30_email_companyphone
,b.company_phone_cnt30_email_contact
,b.company_phone_cnt30_email_device
,b.company_phone_cnt30_email_email
,b.company_phone_cnt30_email_emergency
,b.company_phone_cnt30_email_idcard
,b.company_phone_cnt30_email_myphone
,b.company_phone_cnt30_emergency_bankcard
,b.company_phone_cnt30_emergency_companyphone
,b.company_phone_cnt30_emergency_contact
,b.company_phone_cnt30_emergency_device
,b.company_phone_cnt30_emergency_email
,b.company_phone_cnt30_emergency_emergency
,b.company_phone_cnt30_emergency_idcard
,b.company_phone_cnt30_emergency_myphone
,b.company_phone_cnt30_idcard_bankcard
,b.company_phone_cnt30_idcard_companyphone
,b.company_phone_cnt30_idcard_contact
,b.company_phone_cnt30_idcard_device
,b.company_phone_cnt30_idcard_email
,b.company_phone_cnt30_idcard_emergency
,b.company_phone_cnt30_idcard_idcard
,b.company_phone_cnt30_idcard_myphone
,b.company_phone_cnt30_myphone_bankcard
,b.company_phone_cnt30_myphone_companyphone
,b.company_phone_cnt30_myphone_contact
,b.company_phone_cnt30_myphone_device
,b.company_phone_cnt30_myphone_email
,b.company_phone_cnt30_myphone_emergency
,b.company_phone_cnt30_myphone_idcard
,b.company_phone_cnt30_myphone_myphone
,b.company_phone_cnt7
,b.company_phone_cnt7_bankcard_bankcard
,b.company_phone_cnt7_bankcard_companyphone
,b.company_phone_cnt7_bankcard_contact
,b.company_phone_cnt7_bankcard_device
,b.company_phone_cnt7_bankcard_email
,b.company_phone_cnt7_bankcard_emergency
,b.company_phone_cnt7_bankcard_idcard
,b.company_phone_cnt7_bankcard_myphone
,b.company_phone_cnt7_companyphone_bankcard
,b.company_phone_cnt7_companyphone_companyphone
,b.company_phone_cnt7_companyphone_contact
,b.company_phone_cnt7_companyphone_device
,b.company_phone_cnt7_companyphone_email
,b.company_phone_cnt7_companyphone_emergency
,b.company_phone_cnt7_companyphone_idcard
,b.company_phone_cnt7_companyphone_myphone
,b.company_phone_cnt7_contact_bankcard
,b.company_phone_cnt7_contact_companyphone
,b.company_phone_cnt7_contact_contact
,b.company_phone_cnt7_contact_device
,b.company_phone_cnt7_contact_email
,b.company_phone_cnt7_contact_emergency
,b.company_phone_cnt7_contact_idcard
,b.company_phone_cnt7_contact_myphone
,b.company_phone_cnt7_device_bankcard
,b.company_phone_cnt7_device_companyphone
,b.company_phone_cnt7_device_contact
,b.company_phone_cnt7_device_device
,b.company_phone_cnt7_device_email
,b.company_phone_cnt7_device_emergency
,b.company_phone_cnt7_device_idcard
,b.company_phone_cnt7_device_myphone
,b.company_phone_cnt7_email_bankcard
,b.company_phone_cnt7_email_companyphone
,b.company_phone_cnt7_email_contact
,b.company_phone_cnt7_email_device
,b.company_phone_cnt7_email_email
,b.company_phone_cnt7_email_emergency
,b.company_phone_cnt7_email_idcard
,b.company_phone_cnt7_email_myphone
,b.company_phone_cnt7_emergency_bankcard
,b.company_phone_cnt7_emergency_companyphone
,b.company_phone_cnt7_emergency_contact
,b.company_phone_cnt7_emergency_device
,b.company_phone_cnt7_emergency_email
,b.company_phone_cnt7_emergency_emergency
,b.company_phone_cnt7_emergency_idcard
,b.company_phone_cnt7_emergency_myphone
,b.company_phone_cnt7_idcard_bankcard
,b.company_phone_cnt7_idcard_companyphone
,b.company_phone_cnt7_idcard_contact
,b.company_phone_cnt7_idcard_device
,b.company_phone_cnt7_idcard_email
,b.company_phone_cnt7_idcard_emergency
,b.company_phone_cnt7_idcard_idcard
,b.company_phone_cnt7_idcard_myphone
,b.company_phone_cnt7_myphone_bankcard
,b.company_phone_cnt7_myphone_companyphone
,b.company_phone_cnt7_myphone_contact
,b.company_phone_cnt7_myphone_device
,b.company_phone_cnt7_myphone_email
,b.company_phone_cnt7_myphone_emergency
,b.company_phone_cnt7_myphone_idcard
,b.company_phone_cnt7_myphone_myphone
,b.contact_mobile_cnt
,b.contact_mobile_cnt_bankcard_bankcard
,b.contact_mobile_cnt_bankcard_companyphone
,b.contact_mobile_cnt_bankcard_contact
,b.contact_mobile_cnt_bankcard_device
,b.contact_mobile_cnt_bankcard_email
,b.contact_mobile_cnt_bankcard_emergency
,b.contact_mobile_cnt_bankcard_idcard
,b.contact_mobile_cnt_bankcard_myphone
,b.contact_mobile_cnt_companyphone_bankcard
,b.contact_mobile_cnt_companyphone_companyphone
,b.contact_mobile_cnt_companyphone_contact
,b.contact_mobile_cnt_companyphone_device
,b.contact_mobile_cnt_companyphone_email
,b.contact_mobile_cnt_companyphone_emergency
,b.contact_mobile_cnt_companyphone_idcard
,b.contact_mobile_cnt_companyphone_myphone
,b.contact_mobile_cnt_contact_bankcard
,b.contact_mobile_cnt_contact_companyphone
,b.contact_mobile_cnt_contact_contact
,b.contact_mobile_cnt_contact_device
,b.contact_mobile_cnt_contact_email
,b.contact_mobile_cnt_contact_emergency
,b.contact_mobile_cnt_contact_idcard
,b.contact_mobile_cnt_contact_myphone
,b.contact_mobile_cnt_device_bankcard
,b.contact_mobile_cnt_device_companyphone
,b.contact_mobile_cnt_device_contact
,b.contact_mobile_cnt_device_device
,b.contact_mobile_cnt_device_email
,b.contact_mobile_cnt_device_emergency
,b.contact_mobile_cnt_device_idcard
,b.contact_mobile_cnt_device_myphone
,b.contact_mobile_cnt_email_bankcard
,b.contact_mobile_cnt_email_companyphone
,b.contact_mobile_cnt_email_contact
,b.contact_mobile_cnt_email_device
,b.contact_mobile_cnt_email_email
,b.contact_mobile_cnt_email_emergency
,b.contact_mobile_cnt_email_idcard
,b.contact_mobile_cnt_email_myphone
,b.contact_mobile_cnt_emergency_bankcard
,b.contact_mobile_cnt_emergency_companyphone
,b.contact_mobile_cnt_emergency_contact
,b.contact_mobile_cnt_emergency_device
,b.contact_mobile_cnt_emergency_email
,b.contact_mobile_cnt_emergency_emergency
,b.contact_mobile_cnt_emergency_idcard
,b.contact_mobile_cnt_emergency_myphone
,b.contact_mobile_cnt_idcard_bankcard
,b.contact_mobile_cnt_idcard_companyphone
,b.contact_mobile_cnt_idcard_contact
,b.contact_mobile_cnt_idcard_device
,b.contact_mobile_cnt_idcard_email
,b.contact_mobile_cnt_idcard_emergency
,b.contact_mobile_cnt_idcard_idcard
,b.contact_mobile_cnt_idcard_myphone
,b.contact_mobile_cnt_myphone_bankcard
,b.contact_mobile_cnt_myphone_companyphone
,b.contact_mobile_cnt_myphone_contact
,b.contact_mobile_cnt_myphone_device
,b.contact_mobile_cnt_myphone_email
,b.contact_mobile_cnt_myphone_emergency
,b.contact_mobile_cnt_myphone_idcard
,b.contact_mobile_cnt_myphone_myphone
,b.contact_mobile_cnt1
,b.contact_mobile_cnt1_bankcard_bankcard
,b.contact_mobile_cnt1_bankcard_companyphone
,b.contact_mobile_cnt1_bankcard_contact
,b.contact_mobile_cnt1_bankcard_device
,b.contact_mobile_cnt1_bankcard_email
,b.contact_mobile_cnt1_bankcard_emergency
,b.contact_mobile_cnt1_bankcard_idcard
,b.contact_mobile_cnt1_bankcard_myphone
,b.contact_mobile_cnt1_companyphone_bankcard
,b.contact_mobile_cnt1_companyphone_companyphone
,b.contact_mobile_cnt1_companyphone_contact
,b.contact_mobile_cnt1_companyphone_device
,b.contact_mobile_cnt1_companyphone_email
,b.contact_mobile_cnt1_companyphone_emergency
,b.contact_mobile_cnt1_companyphone_idcard
,b.contact_mobile_cnt1_companyphone_myphone
,b.contact_mobile_cnt1_contact_bankcard
,b.contact_mobile_cnt1_contact_companyphone
,b.contact_mobile_cnt1_contact_contact
,b.contact_mobile_cnt1_contact_device
,b.contact_mobile_cnt1_contact_email
,b.contact_mobile_cnt1_contact_emergency
,b.contact_mobile_cnt1_contact_idcard
,b.contact_mobile_cnt1_contact_myphone
,b.contact_mobile_cnt1_device_bankcard
,b.contact_mobile_cnt1_device_companyphone
,b.contact_mobile_cnt1_device_contact
,b.contact_mobile_cnt1_device_device
,b.contact_mobile_cnt1_device_email
,b.contact_mobile_cnt1_device_emergency
,b.contact_mobile_cnt1_device_idcard
,b.contact_mobile_cnt1_device_myphone
,b.contact_mobile_cnt1_email_bankcard
,b.contact_mobile_cnt1_email_companyphone
,b.contact_mobile_cnt1_email_contact
,b.contact_mobile_cnt1_email_device
,b.contact_mobile_cnt1_email_email
,b.contact_mobile_cnt1_email_emergency
,b.contact_mobile_cnt1_email_idcard
,b.contact_mobile_cnt1_email_myphone
,b.contact_mobile_cnt1_emergency_bankcard
,b.contact_mobile_cnt1_emergency_companyphone
,b.contact_mobile_cnt1_emergency_contact
,b.contact_mobile_cnt1_emergency_device
,b.contact_mobile_cnt1_emergency_email
,b.contact_mobile_cnt1_emergency_emergency
,b.contact_mobile_cnt1_emergency_idcard
,b.contact_mobile_cnt1_emergency_myphone
,b.contact_mobile_cnt1_idcard_bankcard
,b.contact_mobile_cnt1_idcard_companyphone
,b.contact_mobile_cnt1_idcard_contact
,b.contact_mobile_cnt1_idcard_device
,b.contact_mobile_cnt1_idcard_email
,b.contact_mobile_cnt1_idcard_emergency
,b.contact_mobile_cnt1_idcard_idcard
,b.contact_mobile_cnt1_idcard_myphone
,b.contact_mobile_cnt1_myphone_bankcard
,b.contact_mobile_cnt1_myphone_companyphone
,b.contact_mobile_cnt1_myphone_contact
,b.contact_mobile_cnt1_myphone_device
,b.contact_mobile_cnt1_myphone_email
,b.contact_mobile_cnt1_myphone_emergency
,b.contact_mobile_cnt1_myphone_idcard
,b.contact_mobile_cnt1_myphone_myphone
,b.contact_mobile_cnt3
,b.contact_mobile_cnt3_bankcard_bankcard
,b.contact_mobile_cnt3_bankcard_companyphone
,b.contact_mobile_cnt3_bankcard_contact
,b.contact_mobile_cnt3_bankcard_device
,b.contact_mobile_cnt3_bankcard_email
,b.contact_mobile_cnt3_bankcard_emergency
,b.contact_mobile_cnt3_bankcard_idcard
,b.contact_mobile_cnt3_bankcard_myphone
,b.contact_mobile_cnt3_companyphone_bankcard
,b.contact_mobile_cnt3_companyphone_companyphone
,b.contact_mobile_cnt3_companyphone_contact
,b.contact_mobile_cnt3_companyphone_device
,b.contact_mobile_cnt3_companyphone_email
,b.contact_mobile_cnt3_companyphone_emergency
,b.contact_mobile_cnt3_companyphone_idcard
,b.contact_mobile_cnt3_companyphone_myphone
,b.contact_mobile_cnt3_contact_bankcard
,b.contact_mobile_cnt3_contact_companyphone
,b.contact_mobile_cnt3_contact_contact
,b.contact_mobile_cnt3_contact_device
,b.contact_mobile_cnt3_contact_email
,b.contact_mobile_cnt3_contact_emergency
,b.contact_mobile_cnt3_contact_idcard
,b.contact_mobile_cnt3_contact_myphone
,b.contact_mobile_cnt3_device_bankcard
,b.contact_mobile_cnt3_device_companyphone
,b.contact_mobile_cnt3_device_contact
,b.contact_mobile_cnt3_device_device
,b.contact_mobile_cnt3_device_email
,b.contact_mobile_cnt3_device_emergency
,b.contact_mobile_cnt3_device_idcard
,b.contact_mobile_cnt3_device_myphone
,b.contact_mobile_cnt3_email_bankcard
,b.contact_mobile_cnt3_email_companyphone
,b.contact_mobile_cnt3_email_contact
,b.contact_mobile_cnt3_email_device
,b.contact_mobile_cnt3_email_email
,b.contact_mobile_cnt3_email_emergency
,b.contact_mobile_cnt3_email_idcard
,b.contact_mobile_cnt3_email_myphone
,b.contact_mobile_cnt3_emergency_bankcard
,b.contact_mobile_cnt3_emergency_companyphone
,b.contact_mobile_cnt3_emergency_contact
,b.contact_mobile_cnt3_emergency_device
,b.contact_mobile_cnt3_emergency_email
,b.contact_mobile_cnt3_emergency_emergency
,b.contact_mobile_cnt3_emergency_idcard
,b.contact_mobile_cnt3_emergency_myphone
,b.contact_mobile_cnt3_idcard_bankcard
,b.contact_mobile_cnt3_idcard_companyphone
,b.contact_mobile_cnt3_idcard_contact
,b.contact_mobile_cnt3_idcard_device
,b.contact_mobile_cnt3_idcard_email
,b.contact_mobile_cnt3_idcard_emergency
,b.contact_mobile_cnt3_idcard_idcard
,b.contact_mobile_cnt3_idcard_myphone
,b.contact_mobile_cnt3_myphone_bankcard
,b.contact_mobile_cnt3_myphone_companyphone
,b.contact_mobile_cnt3_myphone_contact
,b.contact_mobile_cnt3_myphone_device
,b.contact_mobile_cnt3_myphone_email
,b.contact_mobile_cnt3_myphone_emergency
,b.contact_mobile_cnt3_myphone_idcard
,b.contact_mobile_cnt3_myphone_myphone
,b.contact_mobile_cnt30
,b.contact_mobile_cnt30_bankcard_bankcard
,b.contact_mobile_cnt30_bankcard_companyphone
,b.contact_mobile_cnt30_bankcard_contact
,b.contact_mobile_cnt30_bankcard_device
,b.contact_mobile_cnt30_bankcard_email
,b.contact_mobile_cnt30_bankcard_emergency
,b.contact_mobile_cnt30_bankcard_idcard
,b.contact_mobile_cnt30_bankcard_myphone
,b.contact_mobile_cnt30_companyphone_bankcard
,b.contact_mobile_cnt30_companyphone_companyphone
,b.contact_mobile_cnt30_companyphone_contact
,b.contact_mobile_cnt30_companyphone_device
,b.contact_mobile_cnt30_companyphone_email
,b.contact_mobile_cnt30_companyphone_emergency
,b.contact_mobile_cnt30_companyphone_idcard
,b.contact_mobile_cnt30_companyphone_myphone
,b.contact_mobile_cnt30_contact_bankcard
,b.contact_mobile_cnt30_contact_companyphone
,b.contact_mobile_cnt30_contact_contact
,b.contact_mobile_cnt30_contact_device
,b.contact_mobile_cnt30_contact_email
,b.contact_mobile_cnt30_contact_emergency
,b.contact_mobile_cnt30_contact_idcard
,b.contact_mobile_cnt30_contact_myphone
,b.contact_mobile_cnt30_device_bankcard
,b.contact_mobile_cnt30_device_companyphone
,b.contact_mobile_cnt30_device_contact
,b.contact_mobile_cnt30_device_device
,b.contact_mobile_cnt30_device_email
,b.contact_mobile_cnt30_device_emergency
,b.contact_mobile_cnt30_device_idcard
,b.contact_mobile_cnt30_device_myphone
,b.contact_mobile_cnt30_email_bankcard
,b.contact_mobile_cnt30_email_companyphone
,b.contact_mobile_cnt30_email_contact
,b.contact_mobile_cnt30_email_device
,b.contact_mobile_cnt30_email_email
,b.contact_mobile_cnt30_email_emergency
,b.contact_mobile_cnt30_email_idcard
,b.contact_mobile_cnt30_email_myphone
,b.contact_mobile_cnt30_emergency_bankcard
,b.contact_mobile_cnt30_emergency_companyphone
,b.contact_mobile_cnt30_emergency_contact
,b.contact_mobile_cnt30_emergency_device
,b.contact_mobile_cnt30_emergency_email
,b.contact_mobile_cnt30_emergency_emergency
,b.contact_mobile_cnt30_emergency_idcard
,b.contact_mobile_cnt30_emergency_myphone
,b.contact_mobile_cnt30_idcard_bankcard
,b.contact_mobile_cnt30_idcard_companyphone
,b.contact_mobile_cnt30_idcard_contact
,b.contact_mobile_cnt30_idcard_device
,b.contact_mobile_cnt30_idcard_email
,b.contact_mobile_cnt30_idcard_emergency
,b.contact_mobile_cnt30_idcard_idcard
,b.contact_mobile_cnt30_idcard_myphone
,b.contact_mobile_cnt30_myphone_bankcard
,b.contact_mobile_cnt30_myphone_companyphone
,b.contact_mobile_cnt30_myphone_contact
,b.contact_mobile_cnt30_myphone_device
,b.contact_mobile_cnt30_myphone_email
,b.contact_mobile_cnt30_myphone_emergency
,b.contact_mobile_cnt30_myphone_idcard
,b.contact_mobile_cnt30_myphone_myphone
,b.contact_mobile_cnt7
,b.contact_mobile_cnt7_bankcard_bankcard
,b.contact_mobile_cnt7_bankcard_companyphone
,b.contact_mobile_cnt7_bankcard_contact
,b.contact_mobile_cnt7_bankcard_device
,b.contact_mobile_cnt7_bankcard_email
,b.contact_mobile_cnt7_bankcard_emergency
,b.contact_mobile_cnt7_bankcard_idcard
,b.contact_mobile_cnt7_bankcard_myphone
,b.contact_mobile_cnt7_companyphone_bankcard
,b.contact_mobile_cnt7_companyphone_companyphone
,b.contact_mobile_cnt7_companyphone_contact
,b.contact_mobile_cnt7_companyphone_device
,b.contact_mobile_cnt7_companyphone_email
,b.contact_mobile_cnt7_companyphone_emergency
,b.contact_mobile_cnt7_companyphone_idcard
,b.contact_mobile_cnt7_companyphone_myphone
,b.contact_mobile_cnt7_contact_bankcard
,b.contact_mobile_cnt7_contact_companyphone
,b.contact_mobile_cnt7_contact_contact
,b.contact_mobile_cnt7_contact_device
,b.contact_mobile_cnt7_contact_email
,b.contact_mobile_cnt7_contact_emergency
,b.contact_mobile_cnt7_contact_idcard
,b.contact_mobile_cnt7_contact_myphone
,b.contact_mobile_cnt7_device_bankcard
,b.contact_mobile_cnt7_device_companyphone
,b.contact_mobile_cnt7_device_contact
,b.contact_mobile_cnt7_device_device
,b.contact_mobile_cnt7_device_email
,b.contact_mobile_cnt7_device_emergency
,b.contact_mobile_cnt7_device_idcard
,b.contact_mobile_cnt7_device_myphone
,b.contact_mobile_cnt7_email_bankcard
,b.contact_mobile_cnt7_email_companyphone
,b.contact_mobile_cnt7_email_contact
,b.contact_mobile_cnt7_email_device
,b.contact_mobile_cnt7_email_email
,b.contact_mobile_cnt7_email_emergency
,b.contact_mobile_cnt7_email_idcard
,b.contact_mobile_cnt7_email_myphone
,b.contact_mobile_cnt7_emergency_bankcard
,b.contact_mobile_cnt7_emergency_companyphone
,b.contact_mobile_cnt7_emergency_contact
,b.contact_mobile_cnt7_emergency_device
,b.contact_mobile_cnt7_emergency_email
,b.contact_mobile_cnt7_emergency_emergency
,b.contact_mobile_cnt7_emergency_idcard
,b.contact_mobile_cnt7_emergency_myphone
,b.contact_mobile_cnt7_idcard_bankcard
,b.contact_mobile_cnt7_idcard_companyphone
,b.contact_mobile_cnt7_idcard_contact
,b.contact_mobile_cnt7_idcard_device
,b.contact_mobile_cnt7_idcard_email
,b.contact_mobile_cnt7_idcard_emergency
,b.contact_mobile_cnt7_idcard_idcard
,b.contact_mobile_cnt7_idcard_myphone
,b.contact_mobile_cnt7_myphone_bankcard
,b.contact_mobile_cnt7_myphone_companyphone
,b.contact_mobile_cnt7_myphone_contact
,b.contact_mobile_cnt7_myphone_device
,b.contact_mobile_cnt7_myphone_email
,b.contact_mobile_cnt7_myphone_emergency
,b.contact_mobile_cnt7_myphone_idcard
,b.contact_mobile_cnt7_myphone_myphone
,b.current_overdue0_contract_cnt
,b.current_overdue0_contract_cnt_bankcard_bankcard
,b.current_overdue0_contract_cnt_bankcard_companyphone
,b.current_overdue0_contract_cnt_bankcard_contact
,b.current_overdue0_contract_cnt_bankcard_device
,b.current_overdue0_contract_cnt_bankcard_email
,b.current_overdue0_contract_cnt_bankcard_emergency
,b.current_overdue0_contract_cnt_bankcard_idcard
,b.current_overdue0_contract_cnt_bankcard_myphone
,b.current_overdue0_contract_cnt_companyphone_bankcard
,b.current_overdue0_contract_cnt_companyphone_companyphone
,b.current_overdue0_contract_cnt_companyphone_contact
,b.current_overdue0_contract_cnt_companyphone_device
,b.current_overdue0_contract_cnt_companyphone_email
,b.current_overdue0_contract_cnt_companyphone_emergency
,b.current_overdue0_contract_cnt_companyphone_idcard
,b.current_overdue0_contract_cnt_companyphone_myphone
,b.current_overdue0_contract_cnt_contact_bankcard
,b.current_overdue0_contract_cnt_contact_companyphone
,b.current_overdue0_contract_cnt_contact_contact
,b.current_overdue0_contract_cnt_contact_device
,b.current_overdue0_contract_cnt_contact_email
,b.current_overdue0_contract_cnt_contact_emergency
,b.current_overdue0_contract_cnt_contact_idcard
,b.current_overdue0_contract_cnt_contact_myphone
,b.current_overdue0_contract_cnt_device_bankcard
,b.current_overdue0_contract_cnt_device_companyphone
,b.current_overdue0_contract_cnt_device_contact
,b.current_overdue0_contract_cnt_device_device
,b.current_overdue0_contract_cnt_device_email
,b.current_overdue0_contract_cnt_device_emergency
,b.current_overdue0_contract_cnt_device_idcard
,b.current_overdue0_contract_cnt_device_myphone
,b.current_overdue0_contract_cnt_email_bankcard
,b.current_overdue0_contract_cnt_email_companyphone
,b.current_overdue0_contract_cnt_email_contact
,b.current_overdue0_contract_cnt_email_device
,b.current_overdue0_contract_cnt_email_email
,b.current_overdue0_contract_cnt_email_emergency
,b.current_overdue0_contract_cnt_email_idcard
,b.current_overdue0_contract_cnt_email_myphone
,b.current_overdue0_contract_cnt_emergency_bankcard
,b.current_overdue0_contract_cnt_emergency_companyphone
,b.current_overdue0_contract_cnt_emergency_contact
,b.current_overdue0_contract_cnt_emergency_device
,b.current_overdue0_contract_cnt_emergency_email
,b.current_overdue0_contract_cnt_emergency_emergency
,b.current_overdue0_contract_cnt_emergency_idcard
,b.current_overdue0_contract_cnt_emergency_myphone
,b.current_overdue0_contract_cnt_idcard_bankcard
,b.current_overdue0_contract_cnt_idcard_companyphone
,b.current_overdue0_contract_cnt_idcard_contact
,b.current_overdue0_contract_cnt_idcard_device
,b.current_overdue0_contract_cnt_idcard_email
,b.current_overdue0_contract_cnt_idcard_emergency
,b.current_overdue0_contract_cnt_idcard_idcard
,b.current_overdue0_contract_cnt_idcard_myphone
,b.current_overdue0_contract_cnt_myphone_bankcard
,b.current_overdue0_contract_cnt_myphone_companyphone
,b.current_overdue0_contract_cnt_myphone_contact
,b.current_overdue0_contract_cnt_myphone_device
,b.current_overdue0_contract_cnt_myphone_email
,b.current_overdue0_contract_cnt_myphone_emergency
,b.current_overdue0_contract_cnt_myphone_idcard
,b.current_overdue0_contract_cnt_myphone_myphone
,b.current_overdue3_contract_cnt
,b.current_overdue3_contract_cnt_bankcard_bankcard
,b.current_overdue3_contract_cnt_bankcard_companyphone
,b.current_overdue3_contract_cnt_bankcard_contact
,b.current_overdue3_contract_cnt_bankcard_device
,b.current_overdue3_contract_cnt_bankcard_email
,b.current_overdue3_contract_cnt_bankcard_emergency
,b.current_overdue3_contract_cnt_bankcard_idcard
,b.current_overdue3_contract_cnt_bankcard_myphone
,b.current_overdue3_contract_cnt_companyphone_bankcard
,b.current_overdue3_contract_cnt_companyphone_companyphone
,b.current_overdue3_contract_cnt_companyphone_contact
,b.current_overdue3_contract_cnt_companyphone_device
,b.current_overdue3_contract_cnt_companyphone_email
,b.current_overdue3_contract_cnt_companyphone_emergency
,b.current_overdue3_contract_cnt_companyphone_idcard
,b.current_overdue3_contract_cnt_companyphone_myphone
,b.current_overdue3_contract_cnt_contact_bankcard
,b.current_overdue3_contract_cnt_contact_companyphone
,b.current_overdue3_contract_cnt_contact_contact
,b.current_overdue3_contract_cnt_contact_device
,b.current_overdue3_contract_cnt_contact_email
,b.current_overdue3_contract_cnt_contact_emergency
,b.current_overdue3_contract_cnt_contact_idcard
,b.current_overdue3_contract_cnt_contact_myphone
,b.current_overdue3_contract_cnt_device_bankcard
,b.current_overdue3_contract_cnt_device_companyphone
,b.current_overdue3_contract_cnt_device_contact
,b.current_overdue3_contract_cnt_device_device
,b.current_overdue3_contract_cnt_device_email
,b.current_overdue3_contract_cnt_device_emergency
,b.current_overdue3_contract_cnt_device_idcard
,b.current_overdue3_contract_cnt_device_myphone
,b.current_overdue3_contract_cnt_email_bankcard
,b.current_overdue3_contract_cnt_email_companyphone
,b.current_overdue3_contract_cnt_email_contact
,b.current_overdue3_contract_cnt_email_device
,b.current_overdue3_contract_cnt_email_email
,b.current_overdue3_contract_cnt_email_emergency
,b.current_overdue3_contract_cnt_email_idcard
,b.current_overdue3_contract_cnt_email_myphone
,b.current_overdue3_contract_cnt_emergency_bankcard
,b.current_overdue3_contract_cnt_emergency_companyphone
,b.current_overdue3_contract_cnt_emergency_contact
,b.current_overdue3_contract_cnt_emergency_device
,b.current_overdue3_contract_cnt_emergency_email
,b.current_overdue3_contract_cnt_emergency_emergency
,b.current_overdue3_contract_cnt_emergency_idcard
,b.current_overdue3_contract_cnt_emergency_myphone
,b.current_overdue3_contract_cnt_idcard_bankcard
,b.current_overdue3_contract_cnt_idcard_companyphone
,b.current_overdue3_contract_cnt_idcard_contact
,b.current_overdue3_contract_cnt_idcard_device
,b.current_overdue3_contract_cnt_idcard_email
,b.current_overdue3_contract_cnt_idcard_emergency
,b.current_overdue3_contract_cnt_idcard_idcard
,b.current_overdue3_contract_cnt_idcard_myphone
,b.current_overdue3_contract_cnt_myphone_bankcard
,b.current_overdue3_contract_cnt_myphone_companyphone
,b.current_overdue3_contract_cnt_myphone_contact
,b.current_overdue3_contract_cnt_myphone_device
,b.current_overdue3_contract_cnt_myphone_email
,b.current_overdue3_contract_cnt_myphone_emergency
,b.current_overdue3_contract_cnt_myphone_idcard
,b.current_overdue3_contract_cnt_myphone_myphone
,b.current_overdue30_contract_cnt
,b.current_overdue30_contract_cnt_bankcard_bankcard
,b.current_overdue30_contract_cnt_bankcard_companyphone
,b.current_overdue30_contract_cnt_bankcard_contact
,b.current_overdue30_contract_cnt_bankcard_device
,b.current_overdue30_contract_cnt_bankcard_email
,b.current_overdue30_contract_cnt_bankcard_emergency
,b.current_overdue30_contract_cnt_bankcard_idcard
,b.current_overdue30_contract_cnt_bankcard_myphone
,b.current_overdue30_contract_cnt_companyphone_bankcard
,b.current_overdue30_contract_cnt_companyphone_companyphone
,b.current_overdue30_contract_cnt_companyphone_contact
,b.current_overdue30_contract_cnt_companyphone_device
,b.current_overdue30_contract_cnt_companyphone_email
,b.current_overdue30_contract_cnt_companyphone_emergency
,b.current_overdue30_contract_cnt_companyphone_idcard
,b.current_overdue30_contract_cnt_companyphone_myphone
,b.current_overdue30_contract_cnt_contact_bankcard
,b.current_overdue30_contract_cnt_contact_companyphone
,b.current_overdue30_contract_cnt_contact_contact
,b.current_overdue30_contract_cnt_contact_device
,b.current_overdue30_contract_cnt_contact_email
,b.current_overdue30_contract_cnt_contact_emergency
,b.current_overdue30_contract_cnt_contact_idcard
,b.current_overdue30_contract_cnt_contact_myphone
,b.current_overdue30_contract_cnt_device_bankcard
,b.current_overdue30_contract_cnt_device_companyphone
,b.current_overdue30_contract_cnt_device_contact
,b.current_overdue30_contract_cnt_device_device
,b.current_overdue30_contract_cnt_device_email
,b.current_overdue30_contract_cnt_device_emergency
,b.current_overdue30_contract_cnt_device_idcard
,b.current_overdue30_contract_cnt_device_myphone
,b.current_overdue30_contract_cnt_email_bankcard
,b.current_overdue30_contract_cnt_email_companyphone
,b.current_overdue30_contract_cnt_email_contact
,b.current_overdue30_contract_cnt_email_device
,b.current_overdue30_contract_cnt_email_email
,b.current_overdue30_contract_cnt_email_emergency
,b.current_overdue30_contract_cnt_email_idcard
,b.current_overdue30_contract_cnt_email_myphone
,b.current_overdue30_contract_cnt_emergency_bankcard
,b.current_overdue30_contract_cnt_emergency_companyphone
,b.current_overdue30_contract_cnt_emergency_contact
,b.current_overdue30_contract_cnt_emergency_device
,b.current_overdue30_contract_cnt_emergency_email
,b.current_overdue30_contract_cnt_emergency_emergency
,b.current_overdue30_contract_cnt_emergency_idcard
,b.current_overdue30_contract_cnt_emergency_myphone
,b.current_overdue30_contract_cnt_idcard_bankcard
,b.current_overdue30_contract_cnt_idcard_companyphone
,b.current_overdue30_contract_cnt_idcard_contact
,b.current_overdue30_contract_cnt_idcard_device
,b.current_overdue30_contract_cnt_idcard_email
,b.current_overdue30_contract_cnt_idcard_emergency
,b.current_overdue30_contract_cnt_idcard_idcard
,b.current_overdue30_contract_cnt_idcard_myphone
,b.current_overdue30_contract_cnt_myphone_bankcard
,b.current_overdue30_contract_cnt_myphone_companyphone
,b.current_overdue30_contract_cnt_myphone_contact
,b.current_overdue30_contract_cnt_myphone_device
,b.current_overdue30_contract_cnt_myphone_email
,b.current_overdue30_contract_cnt_myphone_emergency
,b.current_overdue30_contract_cnt_myphone_idcard
,b.current_overdue30_contract_cnt_myphone_myphone
,b.email_cnt
,b.email_cnt_bankcard_bankcard
,b.email_cnt_bankcard_companyphone
,b.email_cnt_bankcard_contact
,b.email_cnt_bankcard_device
,b.email_cnt_bankcard_email
,b.email_cnt_bankcard_emergency
,b.email_cnt_bankcard_idcard
,b.email_cnt_bankcard_myphone
,b.email_cnt_companyphone_bankcard
,b.email_cnt_companyphone_companyphone
,b.email_cnt_companyphone_contact
,b.email_cnt_companyphone_device
,b.email_cnt_companyphone_email
,b.email_cnt_companyphone_emergency
,b.email_cnt_companyphone_idcard
,b.email_cnt_companyphone_myphone
,b.email_cnt_contact_bankcard
,b.email_cnt_contact_companyphone
,b.email_cnt_contact_contact
,b.email_cnt_contact_device
,b.email_cnt_contact_email
,b.email_cnt_contact_emergency
,b.email_cnt_contact_idcard
,b.email_cnt_contact_myphone
,b.email_cnt_device_bankcard
,b.email_cnt_device_companyphone
,b.email_cnt_device_contact
,b.email_cnt_device_device
,b.email_cnt_device_email
,b.email_cnt_device_emergency
,b.email_cnt_device_idcard
,b.email_cnt_device_myphone
,b.email_cnt_email_bankcard
,b.email_cnt_email_companyphone
,b.email_cnt_email_contact
,b.email_cnt_email_device
,b.email_cnt_email_email
,b.email_cnt_email_emergency
,b.email_cnt_email_idcard
,b.email_cnt_email_myphone
,b.email_cnt_emergency_bankcard
,b.email_cnt_emergency_companyphone
,b.email_cnt_emergency_contact
,b.email_cnt_emergency_device
,b.email_cnt_emergency_email
,b.email_cnt_emergency_emergency
,b.email_cnt_emergency_idcard
,b.email_cnt_emergency_myphone
,b.email_cnt_idcard_bankcard
,b.email_cnt_idcard_companyphone
,b.email_cnt_idcard_contact
,b.email_cnt_idcard_device
,b.email_cnt_idcard_email
,b.email_cnt_idcard_emergency
,b.email_cnt_idcard_idcard
,b.email_cnt_idcard_myphone
,b.email_cnt_myphone_bankcard
,b.email_cnt_myphone_companyphone
,b.email_cnt_myphone_contact
,b.email_cnt_myphone_device
,b.email_cnt_myphone_email
,b.email_cnt_myphone_emergency
,b.email_cnt_myphone_idcard
,b.email_cnt_myphone_myphone
,b.email_cnt1
,b.email_cnt1_bankcard_bankcard
,b.email_cnt1_bankcard_companyphone
,b.email_cnt1_bankcard_contact
,b.email_cnt1_bankcard_device
,b.email_cnt1_bankcard_email
,b.email_cnt1_bankcard_emergency
,b.email_cnt1_bankcard_idcard
,b.email_cnt1_bankcard_myphone
,b.email_cnt1_companyphone_bankcard
,b.email_cnt1_companyphone_companyphone
,b.email_cnt1_companyphone_contact
,b.email_cnt1_companyphone_device
,b.email_cnt1_companyphone_email
,b.email_cnt1_companyphone_emergency
,b.email_cnt1_companyphone_idcard
,b.email_cnt1_companyphone_myphone
,b.email_cnt1_contact_bankcard
,b.email_cnt1_contact_companyphone
,b.email_cnt1_contact_contact
,b.email_cnt1_contact_device
,b.email_cnt1_contact_email
,b.email_cnt1_contact_emergency
,b.email_cnt1_contact_idcard
,b.email_cnt1_contact_myphone
,b.email_cnt1_device_bankcard
,b.email_cnt1_device_companyphone
,b.email_cnt1_device_contact
,b.email_cnt1_device_device
,b.email_cnt1_device_email
,b.email_cnt1_device_emergency
,b.email_cnt1_device_idcard
,b.email_cnt1_device_myphone
,b.email_cnt1_email_bankcard
,b.email_cnt1_email_companyphone
,b.email_cnt1_email_contact
,b.email_cnt1_email_device
,b.email_cnt1_email_email
,b.email_cnt1_email_emergency
,b.email_cnt1_email_idcard
,b.email_cnt1_email_myphone
,b.email_cnt1_emergency_bankcard
,b.email_cnt1_emergency_companyphone
,b.email_cnt1_emergency_contact
,b.email_cnt1_emergency_device
,b.email_cnt1_emergency_email
,b.email_cnt1_emergency_emergency
,b.email_cnt1_emergency_idcard
,b.email_cnt1_emergency_myphone
,b.email_cnt1_idcard_bankcard
,b.email_cnt1_idcard_companyphone
,b.email_cnt1_idcard_contact
,b.email_cnt1_idcard_device
,b.email_cnt1_idcard_email
,b.email_cnt1_idcard_emergency
,b.email_cnt1_idcard_idcard
,b.email_cnt1_idcard_myphone
,b.email_cnt1_myphone_bankcard
,b.email_cnt1_myphone_companyphone
,b.email_cnt1_myphone_contact
,b.email_cnt1_myphone_device
,b.email_cnt1_myphone_email
,b.email_cnt1_myphone_emergency
,b.email_cnt1_myphone_idcard
,b.email_cnt1_myphone_myphone
,b.email_cnt3
,b.email_cnt3_bankcard_bankcard
,b.email_cnt3_bankcard_companyphone
,b.email_cnt3_bankcard_contact
,b.email_cnt3_bankcard_device
,b.email_cnt3_bankcard_email
,b.email_cnt3_bankcard_emergency
,b.email_cnt3_bankcard_idcard
,b.email_cnt3_bankcard_myphone
,b.email_cnt3_companyphone_bankcard
,b.email_cnt3_companyphone_companyphone
,b.email_cnt3_companyphone_contact
,b.email_cnt3_companyphone_device
,b.email_cnt3_companyphone_email
,b.email_cnt3_companyphone_emergency
,b.email_cnt3_companyphone_idcard
,b.email_cnt3_companyphone_myphone
,b.email_cnt3_contact_bankcard
,b.email_cnt3_contact_companyphone
,b.email_cnt3_contact_contact
,b.email_cnt3_contact_device
,b.email_cnt3_contact_email
,b.email_cnt3_contact_emergency
,b.email_cnt3_contact_idcard
,b.email_cnt3_contact_myphone
,b.email_cnt3_device_bankcard
,b.email_cnt3_device_companyphone
,b.email_cnt3_device_contact
,b.email_cnt3_device_device
,b.email_cnt3_device_email
,b.email_cnt3_device_emergency
,b.email_cnt3_device_idcard
,b.email_cnt3_device_myphone
,b.email_cnt3_email_bankcard
,b.email_cnt3_email_companyphone
,b.email_cnt3_email_contact
,b.email_cnt3_email_device
,b.email_cnt3_email_email
,b.email_cnt3_email_emergency
,b.email_cnt3_email_idcard
,b.email_cnt3_email_myphone
,b.email_cnt3_emergency_bankcard
,b.email_cnt3_emergency_companyphone
,b.email_cnt3_emergency_contact
,b.email_cnt3_emergency_device
,b.email_cnt3_emergency_email
,b.email_cnt3_emergency_emergency
,b.email_cnt3_emergency_idcard
,b.email_cnt3_emergency_myphone
,b.email_cnt3_idcard_bankcard
,b.email_cnt3_idcard_companyphone
,b.email_cnt3_idcard_contact
,b.email_cnt3_idcard_device
,b.email_cnt3_idcard_email
,b.email_cnt3_idcard_emergency
,b.email_cnt3_idcard_idcard
,b.email_cnt3_idcard_myphone
,b.email_cnt3_myphone_bankcard
,b.email_cnt3_myphone_companyphone
,b.email_cnt3_myphone_contact
,b.email_cnt3_myphone_device
,b.email_cnt3_myphone_email
,b.email_cnt3_myphone_emergency
,b.email_cnt3_myphone_idcard
,b.email_cnt3_myphone_myphone
,b.email_cnt30
,b.email_cnt30_bankcard_bankcard
,b.email_cnt30_bankcard_companyphone
,b.email_cnt30_bankcard_contact
,b.email_cnt30_bankcard_device
,b.email_cnt30_bankcard_email
,b.email_cnt30_bankcard_emergency
,b.email_cnt30_bankcard_idcard
,b.email_cnt30_bankcard_myphone
,b.email_cnt30_companyphone_bankcard
,b.email_cnt30_companyphone_companyphone
,b.email_cnt30_companyphone_contact
,b.email_cnt30_companyphone_device
,b.email_cnt30_companyphone_email
,b.email_cnt30_companyphone_emergency
,b.email_cnt30_companyphone_idcard
,b.email_cnt30_companyphone_myphone
,b.email_cnt30_contact_bankcard
,b.email_cnt30_contact_companyphone
,b.email_cnt30_contact_contact
,b.email_cnt30_contact_device
,b.email_cnt30_contact_email
,b.email_cnt30_contact_emergency
,b.email_cnt30_contact_idcard
,b.email_cnt30_contact_myphone
,b.email_cnt30_device_bankcard
,b.email_cnt30_device_companyphone
,b.email_cnt30_device_contact
,b.email_cnt30_device_device
,b.email_cnt30_device_email
,b.email_cnt30_device_emergency
,b.email_cnt30_device_idcard
,b.email_cnt30_device_myphone
,b.email_cnt30_email_bankcard
,b.email_cnt30_email_companyphone
,b.email_cnt30_email_contact
,b.email_cnt30_email_device
,b.email_cnt30_email_email
,b.email_cnt30_email_emergency
,b.email_cnt30_email_idcard
,b.email_cnt30_email_myphone
,b.email_cnt30_emergency_bankcard
,b.email_cnt30_emergency_companyphone
,b.email_cnt30_emergency_contact
,b.email_cnt30_emergency_device
,b.email_cnt30_emergency_email
,b.email_cnt30_emergency_emergency
,b.email_cnt30_emergency_idcard
,b.email_cnt30_emergency_myphone
,b.email_cnt30_idcard_bankcard
,b.email_cnt30_idcard_companyphone
,b.email_cnt30_idcard_contact
,b.email_cnt30_idcard_device
,b.email_cnt30_idcard_email
,b.email_cnt30_idcard_emergency
,b.email_cnt30_idcard_idcard
,b.email_cnt30_idcard_myphone
,b.email_cnt30_myphone_bankcard
,b.email_cnt30_myphone_companyphone
,b.email_cnt30_myphone_contact
,b.email_cnt30_myphone_device
,b.email_cnt30_myphone_email
,b.email_cnt30_myphone_emergency
,b.email_cnt30_myphone_idcard
,b.email_cnt30_myphone_myphone
,b.email_cnt7
,b.email_cnt7_bankcard_bankcard
,b.email_cnt7_bankcard_companyphone
,b.email_cnt7_bankcard_contact
,b.email_cnt7_bankcard_device
,b.email_cnt7_bankcard_email
,b.email_cnt7_bankcard_emergency
,b.email_cnt7_bankcard_idcard
,b.email_cnt7_bankcard_myphone
,b.email_cnt7_companyphone_bankcard
,b.email_cnt7_companyphone_companyphone
,b.email_cnt7_companyphone_contact
,b.email_cnt7_companyphone_device
,b.email_cnt7_companyphone_email
,b.email_cnt7_companyphone_emergency
,b.email_cnt7_companyphone_idcard
,b.email_cnt7_companyphone_myphone
,b.email_cnt7_contact_bankcard
,b.email_cnt7_contact_companyphone
,b.email_cnt7_contact_contact
,b.email_cnt7_contact_device
,b.email_cnt7_contact_email
,b.email_cnt7_contact_emergency
,b.email_cnt7_contact_idcard
,b.email_cnt7_contact_myphone
,b.email_cnt7_device_bankcard
,b.email_cnt7_device_companyphone
,b.email_cnt7_device_contact
,b.email_cnt7_device_device
,b.email_cnt7_device_email
,b.email_cnt7_device_emergency
,b.email_cnt7_device_idcard
,b.email_cnt7_device_myphone
,b.email_cnt7_email_bankcard
,b.email_cnt7_email_companyphone
,b.email_cnt7_email_contact
,b.email_cnt7_email_device
,b.email_cnt7_email_email
,b.email_cnt7_email_emergency
,b.email_cnt7_email_idcard
,b.email_cnt7_email_myphone
,b.email_cnt7_emergency_bankcard
,b.email_cnt7_emergency_companyphone
,b.email_cnt7_emergency_contact
,b.email_cnt7_emergency_device
,b.email_cnt7_emergency_email
,b.email_cnt7_emergency_emergency
,b.email_cnt7_emergency_idcard
,b.email_cnt7_emergency_myphone
,b.email_cnt7_idcard_bankcard
,b.email_cnt7_idcard_companyphone
,b.email_cnt7_idcard_contact
,b.email_cnt7_idcard_device
,b.email_cnt7_idcard_email
,b.email_cnt7_idcard_emergency
,b.email_cnt7_idcard_idcard
,b.email_cnt7_idcard_myphone
,b.email_cnt7_myphone_bankcard
,b.email_cnt7_myphone_companyphone
,b.email_cnt7_myphone_contact
,b.email_cnt7_myphone_device
,b.email_cnt7_myphone_email
,b.email_cnt7_myphone_emergency
,b.email_cnt7_myphone_idcard
,b.email_cnt7_myphone_myphone
,b.emergency_mobile_cnt
,b.emergency_mobile_cnt_bankcard_bankcard
,b.emergency_mobile_cnt_bankcard_companyphone
,b.emergency_mobile_cnt_bankcard_contact
,b.emergency_mobile_cnt_bankcard_device
,b.emergency_mobile_cnt_bankcard_email
,b.emergency_mobile_cnt_bankcard_emergency
,b.emergency_mobile_cnt_bankcard_idcard
,b.emergency_mobile_cnt_bankcard_myphone
,b.emergency_mobile_cnt_companyphone_bankcard
,b.emergency_mobile_cnt_companyphone_companyphone
,b.emergency_mobile_cnt_companyphone_contact
,b.emergency_mobile_cnt_companyphone_device
,b.emergency_mobile_cnt_companyphone_email
,b.emergency_mobile_cnt_companyphone_emergency
,b.emergency_mobile_cnt_companyphone_idcard
,b.emergency_mobile_cnt_companyphone_myphone
,b.emergency_mobile_cnt_contact_bankcard
,b.emergency_mobile_cnt_contact_companyphone
,b.emergency_mobile_cnt_contact_contact
,b.emergency_mobile_cnt_contact_device
,b.emergency_mobile_cnt_contact_email
,b.emergency_mobile_cnt_contact_emergency
,b.emergency_mobile_cnt_contact_idcard
,b.emergency_mobile_cnt_contact_myphone
,b.emergency_mobile_cnt_device_bankcard
,b.emergency_mobile_cnt_device_companyphone
,b.emergency_mobile_cnt_device_contact
,b.emergency_mobile_cnt_device_device
,b.emergency_mobile_cnt_device_email
,b.emergency_mobile_cnt_device_emergency
,b.emergency_mobile_cnt_device_idcard
,b.emergency_mobile_cnt_device_myphone
,b.emergency_mobile_cnt_email_bankcard
,b.emergency_mobile_cnt_email_companyphone
,b.emergency_mobile_cnt_email_contact
,b.emergency_mobile_cnt_email_device
,b.emergency_mobile_cnt_email_email
,b.emergency_mobile_cnt_email_emergency
,b.emergency_mobile_cnt_email_idcard
,b.emergency_mobile_cnt_email_myphone
,b.emergency_mobile_cnt_emergency_bankcard
,b.emergency_mobile_cnt_emergency_companyphone
,b.emergency_mobile_cnt_emergency_contact
,b.emergency_mobile_cnt_emergency_device
,b.emergency_mobile_cnt_emergency_email
,b.emergency_mobile_cnt_emergency_emergency
,b.emergency_mobile_cnt_emergency_idcard
,b.emergency_mobile_cnt_emergency_myphone
,b.emergency_mobile_cnt_idcard_bankcard
,b.emergency_mobile_cnt_idcard_companyphone
,b.emergency_mobile_cnt_idcard_contact
,b.emergency_mobile_cnt_idcard_device
,b.emergency_mobile_cnt_idcard_email
,b.emergency_mobile_cnt_idcard_emergency
,b.emergency_mobile_cnt_idcard_idcard
,b.emergency_mobile_cnt_idcard_myphone
,b.emergency_mobile_cnt_myphone_bankcard
,b.emergency_mobile_cnt_myphone_companyphone
,b.emergency_mobile_cnt_myphone_contact
,b.emergency_mobile_cnt_myphone_device
,b.emergency_mobile_cnt_myphone_email
,b.emergency_mobile_cnt_myphone_emergency
,b.emergency_mobile_cnt_myphone_idcard
,b.emergency_mobile_cnt_myphone_myphone
,b.emergency_mobile_cnt1
,b.emergency_mobile_cnt1_bankcard_bankcard
,b.emergency_mobile_cnt1_bankcard_companyphone
,b.emergency_mobile_cnt1_bankcard_contact
,b.emergency_mobile_cnt1_bankcard_device
,b.emergency_mobile_cnt1_bankcard_email
,b.emergency_mobile_cnt1_bankcard_emergency
,b.emergency_mobile_cnt1_bankcard_idcard
,b.emergency_mobile_cnt1_bankcard_myphone
,b.emergency_mobile_cnt1_companyphone_bankcard
,b.emergency_mobile_cnt1_companyphone_companyphone
,b.emergency_mobile_cnt1_companyphone_contact
,b.emergency_mobile_cnt1_companyphone_device
,b.emergency_mobile_cnt1_companyphone_email
,b.emergency_mobile_cnt1_companyphone_emergency
,b.emergency_mobile_cnt1_companyphone_idcard
,b.emergency_mobile_cnt1_companyphone_myphone
,b.emergency_mobile_cnt1_contact_bankcard
,b.emergency_mobile_cnt1_contact_companyphone
,b.emergency_mobile_cnt1_contact_contact
,b.emergency_mobile_cnt1_contact_device
,b.emergency_mobile_cnt1_contact_email
,b.emergency_mobile_cnt1_contact_emergency
,b.emergency_mobile_cnt1_contact_idcard
,b.emergency_mobile_cnt1_contact_myphone
,b.emergency_mobile_cnt1_device_bankcard
,b.emergency_mobile_cnt1_device_companyphone
,b.emergency_mobile_cnt1_device_contact
,b.emergency_mobile_cnt1_device_device
,b.emergency_mobile_cnt1_device_email
,b.emergency_mobile_cnt1_device_emergency
,b.emergency_mobile_cnt1_device_idcard
,b.emergency_mobile_cnt1_device_myphone
,b.emergency_mobile_cnt1_email_bankcard
,b.emergency_mobile_cnt1_email_companyphone
,b.emergency_mobile_cnt1_email_contact
,b.emergency_mobile_cnt1_email_device
,b.emergency_mobile_cnt1_email_email
,b.emergency_mobile_cnt1_email_emergency
,b.emergency_mobile_cnt1_email_idcard
,b.emergency_mobile_cnt1_email_myphone
,b.emergency_mobile_cnt1_emergency_bankcard
,b.emergency_mobile_cnt1_emergency_companyphone
,b.emergency_mobile_cnt1_emergency_contact
,b.emergency_mobile_cnt1_emergency_device
,b.emergency_mobile_cnt1_emergency_email
,b.emergency_mobile_cnt1_emergency_emergency
,b.emergency_mobile_cnt1_emergency_idcard
,b.emergency_mobile_cnt1_emergency_myphone
,b.emergency_mobile_cnt1_idcard_bankcard
,b.emergency_mobile_cnt1_idcard_companyphone
,b.emergency_mobile_cnt1_idcard_contact
,b.emergency_mobile_cnt1_idcard_device
,b.emergency_mobile_cnt1_idcard_email
,b.emergency_mobile_cnt1_idcard_emergency
,b.emergency_mobile_cnt1_idcard_idcard
,b.emergency_mobile_cnt1_idcard_myphone
,b.emergency_mobile_cnt1_myphone_bankcard
,b.emergency_mobile_cnt1_myphone_companyphone
,b.emergency_mobile_cnt1_myphone_contact
,b.emergency_mobile_cnt1_myphone_device
,b.emergency_mobile_cnt1_myphone_email
,b.emergency_mobile_cnt1_myphone_emergency
,b.emergency_mobile_cnt1_myphone_idcard
,b.emergency_mobile_cnt1_myphone_myphone
,b.emergency_mobile_cnt3
,b.emergency_mobile_cnt3_bankcard_bankcard
,b.emergency_mobile_cnt3_bankcard_companyphone
,b.emergency_mobile_cnt3_bankcard_contact
,b.emergency_mobile_cnt3_bankcard_device
,b.emergency_mobile_cnt3_bankcard_email
,b.emergency_mobile_cnt3_bankcard_emergency
,b.emergency_mobile_cnt3_bankcard_idcard
,b.emergency_mobile_cnt3_bankcard_myphone
,b.emergency_mobile_cnt3_companyphone_bankcard
,b.emergency_mobile_cnt3_companyphone_companyphone
,b.emergency_mobile_cnt3_companyphone_contact
,b.emergency_mobile_cnt3_companyphone_device
,b.emergency_mobile_cnt3_companyphone_email
,b.emergency_mobile_cnt3_companyphone_emergency
,b.emergency_mobile_cnt3_companyphone_idcard
,b.emergency_mobile_cnt3_companyphone_myphone
,b.emergency_mobile_cnt3_contact_bankcard
,b.emergency_mobile_cnt3_contact_companyphone
,b.emergency_mobile_cnt3_contact_contact
,b.emergency_mobile_cnt3_contact_device
,b.emergency_mobile_cnt3_contact_email
,b.emergency_mobile_cnt3_contact_emergency
,b.emergency_mobile_cnt3_contact_idcard
,b.emergency_mobile_cnt3_contact_myphone
,b.emergency_mobile_cnt3_device_bankcard
,b.emergency_mobile_cnt3_device_companyphone
,b.emergency_mobile_cnt3_device_contact
,b.emergency_mobile_cnt3_device_device
,b.emergency_mobile_cnt3_device_email
,b.emergency_mobile_cnt3_device_emergency
,b.emergency_mobile_cnt3_device_idcard
,b.emergency_mobile_cnt3_device_myphone
,b.emergency_mobile_cnt3_email_bankcard
,b.emergency_mobile_cnt3_email_companyphone
,b.emergency_mobile_cnt3_email_contact
,b.emergency_mobile_cnt3_email_device
,b.emergency_mobile_cnt3_email_email
,b.emergency_mobile_cnt3_email_emergency
,b.emergency_mobile_cnt3_email_idcard
,b.emergency_mobile_cnt3_email_myphone
,b.emergency_mobile_cnt3_emergency_bankcard
,b.emergency_mobile_cnt3_emergency_companyphone
,b.emergency_mobile_cnt3_emergency_contact
,b.emergency_mobile_cnt3_emergency_device
,b.emergency_mobile_cnt3_emergency_email
,b.emergency_mobile_cnt3_emergency_emergency
,b.emergency_mobile_cnt3_emergency_idcard
,b.emergency_mobile_cnt3_emergency_myphone
,b.emergency_mobile_cnt3_idcard_bankcard
,b.emergency_mobile_cnt3_idcard_companyphone
,b.emergency_mobile_cnt3_idcard_contact
,b.emergency_mobile_cnt3_idcard_device
,b.emergency_mobile_cnt3_idcard_email
,b.emergency_mobile_cnt3_idcard_emergency
,b.emergency_mobile_cnt3_idcard_idcard
,b.emergency_mobile_cnt3_idcard_myphone
,b.emergency_mobile_cnt3_myphone_bankcard
,b.emergency_mobile_cnt3_myphone_companyphone
,b.emergency_mobile_cnt3_myphone_contact
,b.emergency_mobile_cnt3_myphone_device
,b.emergency_mobile_cnt3_myphone_email
,b.emergency_mobile_cnt3_myphone_emergency
,b.emergency_mobile_cnt3_myphone_idcard
,b.emergency_mobile_cnt3_myphone_myphone
,b.emergency_mobile_cnt30
,b.emergency_mobile_cnt30_bankcard_bankcard
,b.emergency_mobile_cnt30_bankcard_companyphone
,b.emergency_mobile_cnt30_bankcard_contact
,b.emergency_mobile_cnt30_bankcard_device
,b.emergency_mobile_cnt30_bankcard_email
,b.emergency_mobile_cnt30_bankcard_emergency
,b.emergency_mobile_cnt30_bankcard_idcard
,b.emergency_mobile_cnt30_bankcard_myphone
,b.emergency_mobile_cnt30_companyphone_bankcard
,b.emergency_mobile_cnt30_companyphone_companyphone
,b.emergency_mobile_cnt30_companyphone_contact
,b.emergency_mobile_cnt30_companyphone_device
,b.emergency_mobile_cnt30_companyphone_email
,b.emergency_mobile_cnt30_companyphone_emergency
,b.emergency_mobile_cnt30_companyphone_idcard
,b.emergency_mobile_cnt30_companyphone_myphone
,b.emergency_mobile_cnt30_contact_bankcard
,b.emergency_mobile_cnt30_contact_companyphone
,b.emergency_mobile_cnt30_contact_contact
,b.emergency_mobile_cnt30_contact_device
,b.emergency_mobile_cnt30_contact_email
,b.emergency_mobile_cnt30_contact_emergency
,b.emergency_mobile_cnt30_contact_idcard
,b.emergency_mobile_cnt30_contact_myphone
,b.emergency_mobile_cnt30_device_bankcard
,b.emergency_mobile_cnt30_device_companyphone
,b.emergency_mobile_cnt30_device_contact
,b.emergency_mobile_cnt30_device_device
,b.emergency_mobile_cnt30_device_email
,b.emergency_mobile_cnt30_device_emergency
,b.emergency_mobile_cnt30_device_idcard
,b.emergency_mobile_cnt30_device_myphone
,b.emergency_mobile_cnt30_email_bankcard
,b.emergency_mobile_cnt30_email_companyphone
,b.emergency_mobile_cnt30_email_contact
,b.emergency_mobile_cnt30_email_device
,b.emergency_mobile_cnt30_email_email
,b.emergency_mobile_cnt30_email_emergency
,b.emergency_mobile_cnt30_email_idcard
,b.emergency_mobile_cnt30_email_myphone
,b.emergency_mobile_cnt30_emergency_bankcard
,b.emergency_mobile_cnt30_emergency_companyphone
,b.emergency_mobile_cnt30_emergency_contact
,b.emergency_mobile_cnt30_emergency_device
,b.emergency_mobile_cnt30_emergency_email
,b.emergency_mobile_cnt30_emergency_emergency
,b.emergency_mobile_cnt30_emergency_idcard
,b.emergency_mobile_cnt30_emergency_myphone
,b.emergency_mobile_cnt30_idcard_bankcard
,b.emergency_mobile_cnt30_idcard_companyphone
,b.emergency_mobile_cnt30_idcard_contact
,b.emergency_mobile_cnt30_idcard_device
,b.emergency_mobile_cnt30_idcard_email
,b.emergency_mobile_cnt30_idcard_emergency
,b.emergency_mobile_cnt30_idcard_idcard
,b.emergency_mobile_cnt30_idcard_myphone
,b.emergency_mobile_cnt30_myphone_bankcard
,b.emergency_mobile_cnt30_myphone_companyphone
,b.emergency_mobile_cnt30_myphone_contact
,b.emergency_mobile_cnt30_myphone_device
,b.emergency_mobile_cnt30_myphone_email
,b.emergency_mobile_cnt30_myphone_emergency
,b.emergency_mobile_cnt30_myphone_idcard
,b.emergency_mobile_cnt30_myphone_myphone
,b.emergency_mobile_cnt7
,b.emergency_mobile_cnt7_bankcard_bankcard
,b.emergency_mobile_cnt7_bankcard_companyphone
,b.emergency_mobile_cnt7_bankcard_contact
,b.emergency_mobile_cnt7_bankcard_device
,b.emergency_mobile_cnt7_bankcard_email
,b.emergency_mobile_cnt7_bankcard_emergency
,b.emergency_mobile_cnt7_bankcard_idcard
,b.emergency_mobile_cnt7_bankcard_myphone
,b.emergency_mobile_cnt7_companyphone_bankcard
,b.emergency_mobile_cnt7_companyphone_companyphone
,b.emergency_mobile_cnt7_companyphone_contact
,b.emergency_mobile_cnt7_companyphone_device
,b.emergency_mobile_cnt7_companyphone_email
,b.emergency_mobile_cnt7_companyphone_emergency
,b.emergency_mobile_cnt7_companyphone_idcard
,b.emergency_mobile_cnt7_companyphone_myphone
,b.emergency_mobile_cnt7_contact_bankcard
,b.emergency_mobile_cnt7_contact_companyphone
,b.emergency_mobile_cnt7_contact_contact
,b.emergency_mobile_cnt7_contact_device
,b.emergency_mobile_cnt7_contact_email
,b.emergency_mobile_cnt7_contact_emergency
,b.emergency_mobile_cnt7_contact_idcard
,b.emergency_mobile_cnt7_contact_myphone
,b.emergency_mobile_cnt7_device_bankcard
,b.emergency_mobile_cnt7_device_companyphone
,b.emergency_mobile_cnt7_device_contact
,b.emergency_mobile_cnt7_device_device
,b.emergency_mobile_cnt7_device_email
,b.emergency_mobile_cnt7_device_emergency
,b.emergency_mobile_cnt7_device_idcard
,b.emergency_mobile_cnt7_device_myphone
,b.emergency_mobile_cnt7_email_bankcard
,b.emergency_mobile_cnt7_email_companyphone
,b.emergency_mobile_cnt7_email_contact
,b.emergency_mobile_cnt7_email_device
,b.emergency_mobile_cnt7_email_email
,b.emergency_mobile_cnt7_email_emergency
,b.emergency_mobile_cnt7_email_idcard
,b.emergency_mobile_cnt7_email_myphone
,b.emergency_mobile_cnt7_emergency_bankcard
,b.emergency_mobile_cnt7_emergency_companyphone
,b.emergency_mobile_cnt7_emergency_contact
,b.emergency_mobile_cnt7_emergency_device
,b.emergency_mobile_cnt7_emergency_email
,b.emergency_mobile_cnt7_emergency_emergency
,b.emergency_mobile_cnt7_emergency_idcard
,b.emergency_mobile_cnt7_emergency_myphone
,b.emergency_mobile_cnt7_idcard_bankcard
,b.emergency_mobile_cnt7_idcard_companyphone
,b.emergency_mobile_cnt7_idcard_contact
,b.emergency_mobile_cnt7_idcard_device
,b.emergency_mobile_cnt7_idcard_email
,b.emergency_mobile_cnt7_idcard_emergency
,b.emergency_mobile_cnt7_idcard_idcard
,b.emergency_mobile_cnt7_idcard_myphone
,b.emergency_mobile_cnt7_myphone_bankcard
,b.emergency_mobile_cnt7_myphone_companyphone
,b.emergency_mobile_cnt7_myphone_contact
,b.emergency_mobile_cnt7_myphone_device
,b.emergency_mobile_cnt7_myphone_email
,b.emergency_mobile_cnt7_myphone_emergency
,b.emergency_mobile_cnt7_myphone_idcard
,b.emergency_mobile_cnt7_myphone_myphone
,b.history_overdue0_contract_cnt
,b.history_overdue0_contract_cnt_bankcard_bankcard
,b.history_overdue0_contract_cnt_bankcard_companyphone
,b.history_overdue0_contract_cnt_bankcard_contact
,b.history_overdue0_contract_cnt_bankcard_device
,b.history_overdue0_contract_cnt_bankcard_email
,b.history_overdue0_contract_cnt_bankcard_emergency
,b.history_overdue0_contract_cnt_bankcard_idcard
,b.history_overdue0_contract_cnt_bankcard_myphone
,b.history_overdue0_contract_cnt_companyphone_bankcard
,b.history_overdue0_contract_cnt_companyphone_companyphone
,b.history_overdue0_contract_cnt_companyphone_contact
,b.history_overdue0_contract_cnt_companyphone_device
,b.history_overdue0_contract_cnt_companyphone_email
,b.history_overdue0_contract_cnt_companyphone_emergency
,b.history_overdue0_contract_cnt_companyphone_idcard
,b.history_overdue0_contract_cnt_companyphone_myphone
,b.history_overdue0_contract_cnt_contact_bankcard
,b.history_overdue0_contract_cnt_contact_companyphone
,b.history_overdue0_contract_cnt_contact_contact
,b.history_overdue0_contract_cnt_contact_device
,b.history_overdue0_contract_cnt_contact_email
,b.history_overdue0_contract_cnt_contact_emergency
,b.history_overdue0_contract_cnt_contact_idcard
,b.history_overdue0_contract_cnt_contact_myphone
,b.history_overdue0_contract_cnt_device_bankcard
,b.history_overdue0_contract_cnt_device_companyphone
,b.history_overdue0_contract_cnt_device_contact
,b.history_overdue0_contract_cnt_device_device
,b.history_overdue0_contract_cnt_device_email
,b.history_overdue0_contract_cnt_device_emergency
,b.history_overdue0_contract_cnt_device_idcard
,b.history_overdue0_contract_cnt_device_myphone
,b.history_overdue0_contract_cnt_email_bankcard
,b.history_overdue0_contract_cnt_email_companyphone
,b.history_overdue0_contract_cnt_email_contact
,b.history_overdue0_contract_cnt_email_device
,b.history_overdue0_contract_cnt_email_email
,b.history_overdue0_contract_cnt_email_emergency
,b.history_overdue0_contract_cnt_email_idcard
,b.history_overdue0_contract_cnt_email_myphone
,b.history_overdue0_contract_cnt_emergency_bankcard
,b.history_overdue0_contract_cnt_emergency_companyphone
,b.history_overdue0_contract_cnt_emergency_contact
,b.history_overdue0_contract_cnt_emergency_device
,b.history_overdue0_contract_cnt_emergency_email
,b.history_overdue0_contract_cnt_emergency_emergency
,b.history_overdue0_contract_cnt_emergency_idcard
,b.history_overdue0_contract_cnt_emergency_myphone
,b.history_overdue0_contract_cnt_idcard_bankcard
,b.history_overdue0_contract_cnt_idcard_companyphone
,b.history_overdue0_contract_cnt_idcard_contact
,b.history_overdue0_contract_cnt_idcard_device
,b.history_overdue0_contract_cnt_idcard_email
,b.history_overdue0_contract_cnt_idcard_emergency
,b.history_overdue0_contract_cnt_idcard_idcard
,b.history_overdue0_contract_cnt_idcard_myphone
,b.history_overdue0_contract_cnt_myphone_bankcard
,b.history_overdue0_contract_cnt_myphone_companyphone
,b.history_overdue0_contract_cnt_myphone_contact
,b.history_overdue0_contract_cnt_myphone_device
,b.history_overdue0_contract_cnt_myphone_email
,b.history_overdue0_contract_cnt_myphone_emergency
,b.history_overdue0_contract_cnt_myphone_idcard
,b.history_overdue0_contract_cnt_myphone_myphone
,b.history_overdue3_contract_cnt
,b.history_overdue3_contract_cnt_bankcard_bankcard
,b.history_overdue3_contract_cnt_bankcard_companyphone
,b.history_overdue3_contract_cnt_bankcard_contact
,b.history_overdue3_contract_cnt_bankcard_device
,b.history_overdue3_contract_cnt_bankcard_email
,b.history_overdue3_contract_cnt_bankcard_emergency
,b.history_overdue3_contract_cnt_bankcard_idcard
,b.history_overdue3_contract_cnt_bankcard_myphone
,b.history_overdue3_contract_cnt_companyphone_bankcard
,b.history_overdue3_contract_cnt_companyphone_companyphone
,b.history_overdue3_contract_cnt_companyphone_contact
,b.history_overdue3_contract_cnt_companyphone_device
,b.history_overdue3_contract_cnt_companyphone_email
,b.history_overdue3_contract_cnt_companyphone_emergency
,b.history_overdue3_contract_cnt_companyphone_idcard
,b.history_overdue3_contract_cnt_companyphone_myphone
,b.history_overdue3_contract_cnt_contact_bankcard
,b.history_overdue3_contract_cnt_contact_companyphone
,b.history_overdue3_contract_cnt_contact_contact
,b.history_overdue3_contract_cnt_contact_device
,b.history_overdue3_contract_cnt_contact_email
,b.history_overdue3_contract_cnt_contact_emergency
,b.history_overdue3_contract_cnt_contact_idcard
,b.history_overdue3_contract_cnt_contact_myphone
,b.history_overdue3_contract_cnt_device_bankcard
,b.history_overdue3_contract_cnt_device_companyphone
,b.history_overdue3_contract_cnt_device_contact
,b.history_overdue3_contract_cnt_device_device
,b.history_overdue3_contract_cnt_device_email
,b.history_overdue3_contract_cnt_device_emergency
,b.history_overdue3_contract_cnt_device_idcard
,b.history_overdue3_contract_cnt_device_myphone
,b.history_overdue3_contract_cnt_email_bankcard
,b.history_overdue3_contract_cnt_email_companyphone
,b.history_overdue3_contract_cnt_email_contact
,b.history_overdue3_contract_cnt_email_device
,b.history_overdue3_contract_cnt_email_email
,b.history_overdue3_contract_cnt_email_emergency
,b.history_overdue3_contract_cnt_email_idcard
,b.history_overdue3_contract_cnt_email_myphone
,b.history_overdue3_contract_cnt_emergency_bankcard
,b.history_overdue3_contract_cnt_emergency_companyphone
,b.history_overdue3_contract_cnt_emergency_contact
,b.history_overdue3_contract_cnt_emergency_device
,b.history_overdue3_contract_cnt_emergency_email
,b.history_overdue3_contract_cnt_emergency_emergency
,b.history_overdue3_contract_cnt_emergency_idcard
,b.history_overdue3_contract_cnt_emergency_myphone
,b.history_overdue3_contract_cnt_idcard_bankcard
,b.history_overdue3_contract_cnt_idcard_companyphone
,b.history_overdue3_contract_cnt_idcard_contact
,b.history_overdue3_contract_cnt_idcard_device
,b.history_overdue3_contract_cnt_idcard_email
,b.history_overdue3_contract_cnt_idcard_emergency
,b.history_overdue3_contract_cnt_idcard_idcard
,b.history_overdue3_contract_cnt_idcard_myphone
,b.history_overdue3_contract_cnt_myphone_bankcard
,b.history_overdue3_contract_cnt_myphone_companyphone
,b.history_overdue3_contract_cnt_myphone_contact
,b.history_overdue3_contract_cnt_myphone_device
,b.history_overdue3_contract_cnt_myphone_email
,b.history_overdue3_contract_cnt_myphone_emergency
,b.history_overdue3_contract_cnt_myphone_idcard
,b.history_overdue3_contract_cnt_myphone_myphone
,b.history_overdue30_contract_cnt
,b.history_overdue30_contract_cnt_bankcard_bankcard
,b.history_overdue30_contract_cnt_bankcard_companyphone
,b.history_overdue30_contract_cnt_bankcard_contact
,b.history_overdue30_contract_cnt_bankcard_device
,b.history_overdue30_contract_cnt_bankcard_email
,b.history_overdue30_contract_cnt_bankcard_emergency
,b.history_overdue30_contract_cnt_bankcard_idcard
,b.history_overdue30_contract_cnt_bankcard_myphone
,b.history_overdue30_contract_cnt_companyphone_bankcard
,b.history_overdue30_contract_cnt_companyphone_companyphone
,b.history_overdue30_contract_cnt_companyphone_contact
,b.history_overdue30_contract_cnt_companyphone_device
,b.history_overdue30_contract_cnt_companyphone_email
,b.history_overdue30_contract_cnt_companyphone_emergency
,b.history_overdue30_contract_cnt_companyphone_idcard
,b.history_overdue30_contract_cnt_companyphone_myphone
,b.history_overdue30_contract_cnt_contact_bankcard
,b.history_overdue30_contract_cnt_contact_companyphone
,b.history_overdue30_contract_cnt_contact_contact
,b.history_overdue30_contract_cnt_contact_device
,b.history_overdue30_contract_cnt_contact_email
,b.history_overdue30_contract_cnt_contact_emergency
,b.history_overdue30_contract_cnt_contact_idcard
,b.history_overdue30_contract_cnt_contact_myphone
,b.history_overdue30_contract_cnt_device_bankcard
,b.history_overdue30_contract_cnt_device_companyphone
,b.history_overdue30_contract_cnt_device_contact
,b.history_overdue30_contract_cnt_device_device
,b.history_overdue30_contract_cnt_device_email
,b.history_overdue30_contract_cnt_device_emergency
,b.history_overdue30_contract_cnt_device_idcard
,b.history_overdue30_contract_cnt_device_myphone
,b.history_overdue30_contract_cnt_email_bankcard
,b.history_overdue30_contract_cnt_email_companyphone
,b.history_overdue30_contract_cnt_email_contact
,b.history_overdue30_contract_cnt_email_device
,b.history_overdue30_contract_cnt_email_email
,b.history_overdue30_contract_cnt_email_emergency
,b.history_overdue30_contract_cnt_email_idcard
,b.history_overdue30_contract_cnt_email_myphone
,b.history_overdue30_contract_cnt_emergency_bankcard
,b.history_overdue30_contract_cnt_emergency_companyphone
,b.history_overdue30_contract_cnt_emergency_contact
,b.history_overdue30_contract_cnt_emergency_device
,b.history_overdue30_contract_cnt_emergency_email
,b.history_overdue30_contract_cnt_emergency_emergency
,b.history_overdue30_contract_cnt_emergency_idcard
,b.history_overdue30_contract_cnt_emergency_myphone
,b.history_overdue30_contract_cnt_idcard_bankcard
,b.history_overdue30_contract_cnt_idcard_companyphone
,b.history_overdue30_contract_cnt_idcard_contact
,b.history_overdue30_contract_cnt_idcard_device
,b.history_overdue30_contract_cnt_idcard_email
,b.history_overdue30_contract_cnt_idcard_emergency
,b.history_overdue30_contract_cnt_idcard_idcard
,b.history_overdue30_contract_cnt_idcard_myphone
,b.history_overdue30_contract_cnt_myphone_bankcard
,b.history_overdue30_contract_cnt_myphone_companyphone
,b.history_overdue30_contract_cnt_myphone_contact
,b.history_overdue30_contract_cnt_myphone_device
,b.history_overdue30_contract_cnt_myphone_email
,b.history_overdue30_contract_cnt_myphone_emergency
,b.history_overdue30_contract_cnt_myphone_idcard
,b.history_overdue30_contract_cnt_myphone_myphone
,b.imei_cnt
,b.imei_cnt_bankcard_bankcard
,b.imei_cnt_bankcard_companyphone
,b.imei_cnt_bankcard_contact
,b.imei_cnt_bankcard_device
,b.imei_cnt_bankcard_email
,b.imei_cnt_bankcard_emergency
,b.imei_cnt_bankcard_idcard
,b.imei_cnt_bankcard_myphone
,b.imei_cnt_companyphone_bankcard
,b.imei_cnt_companyphone_companyphone
,b.imei_cnt_companyphone_contact
,b.imei_cnt_companyphone_device
,b.imei_cnt_companyphone_email
,b.imei_cnt_companyphone_emergency
,b.imei_cnt_companyphone_idcard
,b.imei_cnt_companyphone_myphone
,b.imei_cnt_contact_bankcard
,b.imei_cnt_contact_companyphone
,b.imei_cnt_contact_contact
,b.imei_cnt_contact_device
,b.imei_cnt_contact_email
,b.imei_cnt_contact_emergency
,b.imei_cnt_contact_idcard
,b.imei_cnt_contact_myphone
,b.imei_cnt_device_bankcard
,b.imei_cnt_device_companyphone
,b.imei_cnt_device_contact
,b.imei_cnt_device_device
,b.imei_cnt_device_email
,b.imei_cnt_device_emergency
,b.imei_cnt_device_idcard
,b.imei_cnt_device_myphone
,b.imei_cnt_email_bankcard
,b.imei_cnt_email_companyphone
,b.imei_cnt_email_contact
,b.imei_cnt_email_device
,b.imei_cnt_email_email
,b.imei_cnt_email_emergency
,b.imei_cnt_email_idcard
,b.imei_cnt_email_myphone
,b.imei_cnt_emergency_bankcard
,b.imei_cnt_emergency_companyphone
,b.imei_cnt_emergency_contact
,b.imei_cnt_emergency_device
,b.imei_cnt_emergency_email
,b.imei_cnt_emergency_emergency
,b.imei_cnt_emergency_idcard
,b.imei_cnt_emergency_myphone
,b.imei_cnt_idcard_bankcard
,b.imei_cnt_idcard_companyphone
,b.imei_cnt_idcard_contact
,b.imei_cnt_idcard_device
,b.imei_cnt_idcard_email
,b.imei_cnt_idcard_emergency
,b.imei_cnt_idcard_idcard
,b.imei_cnt_idcard_myphone
,b.imei_cnt_myphone_bankcard
,b.imei_cnt_myphone_companyphone
,b.imei_cnt_myphone_contact
,b.imei_cnt_myphone_device
,b.imei_cnt_myphone_email
,b.imei_cnt_myphone_emergency
,b.imei_cnt_myphone_idcard
,b.imei_cnt_myphone_myphone
,b.imei_cnt1
,b.imei_cnt1_bankcard_bankcard
,b.imei_cnt1_bankcard_companyphone
,b.imei_cnt1_bankcard_contact
,b.imei_cnt1_bankcard_device
,b.imei_cnt1_bankcard_email
,b.imei_cnt1_bankcard_emergency
,b.imei_cnt1_bankcard_idcard
,b.imei_cnt1_bankcard_myphone
,b.imei_cnt1_companyphone_bankcard
,b.imei_cnt1_companyphone_companyphone
,b.imei_cnt1_companyphone_contact
,b.imei_cnt1_companyphone_device
,b.imei_cnt1_companyphone_email
,b.imei_cnt1_companyphone_emergency
,b.imei_cnt1_companyphone_idcard
,b.imei_cnt1_companyphone_myphone
,b.imei_cnt1_contact_bankcard
,b.imei_cnt1_contact_companyphone
,b.imei_cnt1_contact_contact
,b.imei_cnt1_contact_device
,b.imei_cnt1_contact_email
,b.imei_cnt1_contact_emergency
,b.imei_cnt1_contact_idcard
,b.imei_cnt1_contact_myphone
,b.imei_cnt1_device_bankcard
,b.imei_cnt1_device_companyphone
,b.imei_cnt1_device_contact
,b.imei_cnt1_device_device
,b.imei_cnt1_device_email
,b.imei_cnt1_device_emergency
,b.imei_cnt1_device_idcard
,b.imei_cnt1_device_myphone
,b.imei_cnt1_email_bankcard
,b.imei_cnt1_email_companyphone
,b.imei_cnt1_email_contact
,b.imei_cnt1_email_device
,b.imei_cnt1_email_email
,b.imei_cnt1_email_emergency
,b.imei_cnt1_email_idcard
,b.imei_cnt1_email_myphone
,b.imei_cnt1_emergency_bankcard
,b.imei_cnt1_emergency_companyphone
,b.imei_cnt1_emergency_contact
,b.imei_cnt1_emergency_device
,b.imei_cnt1_emergency_email
,b.imei_cnt1_emergency_emergency
,b.imei_cnt1_emergency_idcard
,b.imei_cnt1_emergency_myphone
,b.imei_cnt1_idcard_bankcard
,b.imei_cnt1_idcard_companyphone
,b.imei_cnt1_idcard_contact
,b.imei_cnt1_idcard_device
,b.imei_cnt1_idcard_email
,b.imei_cnt1_idcard_emergency
,b.imei_cnt1_idcard_idcard
,b.imei_cnt1_idcard_myphone
,b.imei_cnt1_myphone_bankcard
,b.imei_cnt1_myphone_companyphone
,b.imei_cnt1_myphone_contact
,b.imei_cnt1_myphone_device
,b.imei_cnt1_myphone_email
,b.imei_cnt1_myphone_emergency
,b.imei_cnt1_myphone_idcard
,b.imei_cnt1_myphone_myphone
,b.imei_cnt3
,b.imei_cnt3_bankcard_bankcard
,b.imei_cnt3_bankcard_companyphone
,b.imei_cnt3_bankcard_contact
,b.imei_cnt3_bankcard_device
,b.imei_cnt3_bankcard_email
,b.imei_cnt3_bankcard_emergency
,b.imei_cnt3_bankcard_idcard
,b.imei_cnt3_bankcard_myphone
,b.imei_cnt3_companyphone_bankcard
,b.imei_cnt3_companyphone_companyphone
,b.imei_cnt3_companyphone_contact
,b.imei_cnt3_companyphone_device
,b.imei_cnt3_companyphone_email
,b.imei_cnt3_companyphone_emergency
,b.imei_cnt3_companyphone_idcard
,b.imei_cnt3_companyphone_myphone
,b.imei_cnt3_contact_bankcard
,b.imei_cnt3_contact_companyphone
,b.imei_cnt3_contact_contact
,b.imei_cnt3_contact_device
,b.imei_cnt3_contact_email
,b.imei_cnt3_contact_emergency
,b.imei_cnt3_contact_idcard
,b.imei_cnt3_contact_myphone
,b.imei_cnt3_device_bankcard
,b.imei_cnt3_device_companyphone
,b.imei_cnt3_device_contact
,b.imei_cnt3_device_device
,b.imei_cnt3_device_email
,b.imei_cnt3_device_emergency
,b.imei_cnt3_device_idcard
,b.imei_cnt3_device_myphone
,b.imei_cnt3_email_bankcard
,b.imei_cnt3_email_companyphone
,b.imei_cnt3_email_contact
,b.imei_cnt3_email_device
,b.imei_cnt3_email_email
,b.imei_cnt3_email_emergency
,b.imei_cnt3_email_idcard
,b.imei_cnt3_email_myphone
,b.imei_cnt3_emergency_bankcard
,b.imei_cnt3_emergency_companyphone
,b.imei_cnt3_emergency_contact
,b.imei_cnt3_emergency_device
,b.imei_cnt3_emergency_email
,b.imei_cnt3_emergency_emergency
,b.imei_cnt3_emergency_idcard
,b.imei_cnt3_emergency_myphone
,b.imei_cnt3_idcard_bankcard
,b.imei_cnt3_idcard_companyphone
,b.imei_cnt3_idcard_contact
,b.imei_cnt3_idcard_device
,b.imei_cnt3_idcard_email
,b.imei_cnt3_idcard_emergency
,b.imei_cnt3_idcard_idcard
,b.imei_cnt3_idcard_myphone
,b.imei_cnt3_myphone_bankcard
,b.imei_cnt3_myphone_companyphone
,b.imei_cnt3_myphone_contact
,b.imei_cnt3_myphone_device
,b.imei_cnt3_myphone_email
,b.imei_cnt3_myphone_emergency
,b.imei_cnt3_myphone_idcard
,b.imei_cnt3_myphone_myphone
,b.imei_cnt30
,b.imei_cnt30_bankcard_bankcard
,b.imei_cnt30_bankcard_companyphone
,b.imei_cnt30_bankcard_contact
,b.imei_cnt30_bankcard_device
,b.imei_cnt30_bankcard_email
,b.imei_cnt30_bankcard_emergency
,b.imei_cnt30_bankcard_idcard
,b.imei_cnt30_bankcard_myphone
,b.imei_cnt30_companyphone_bankcard
,b.imei_cnt30_companyphone_companyphone
,b.imei_cnt30_companyphone_contact
,b.imei_cnt30_companyphone_device
,b.imei_cnt30_companyphone_email
,b.imei_cnt30_companyphone_emergency
,b.imei_cnt30_companyphone_idcard
,b.imei_cnt30_companyphone_myphone
,b.imei_cnt30_contact_bankcard
,b.imei_cnt30_contact_companyphone
,b.imei_cnt30_contact_contact
,b.imei_cnt30_contact_device
,b.imei_cnt30_contact_email
,b.imei_cnt30_contact_emergency
,b.imei_cnt30_contact_idcard
,b.imei_cnt30_contact_myphone
,b.imei_cnt30_device_bankcard
,b.imei_cnt30_device_companyphone
,b.imei_cnt30_device_contact
,b.imei_cnt30_device_device
,b.imei_cnt30_device_email
,b.imei_cnt30_device_emergency
,b.imei_cnt30_device_idcard
,b.imei_cnt30_device_myphone
,b.imei_cnt30_email_bankcard
,b.imei_cnt30_email_companyphone
,b.imei_cnt30_email_contact
,b.imei_cnt30_email_device
,b.imei_cnt30_email_email
,b.imei_cnt30_email_emergency
,b.imei_cnt30_email_idcard
,b.imei_cnt30_email_myphone
,b.imei_cnt30_emergency_bankcard
,b.imei_cnt30_emergency_companyphone
,b.imei_cnt30_emergency_contact
,b.imei_cnt30_emergency_device
,b.imei_cnt30_emergency_email
,b.imei_cnt30_emergency_emergency
,b.imei_cnt30_emergency_idcard
,b.imei_cnt30_emergency_myphone
,b.imei_cnt30_idcard_bankcard
,b.imei_cnt30_idcard_companyphone
,b.imei_cnt30_idcard_contact
,b.imei_cnt30_idcard_device
,b.imei_cnt30_idcard_email
,b.imei_cnt30_idcard_emergency
,b.imei_cnt30_idcard_idcard
,b.imei_cnt30_idcard_myphone
,b.imei_cnt30_myphone_bankcard
,b.imei_cnt30_myphone_companyphone
,b.imei_cnt30_myphone_contact
,b.imei_cnt30_myphone_device
,b.imei_cnt30_myphone_email
,b.imei_cnt30_myphone_emergency
,b.imei_cnt30_myphone_idcard
,b.imei_cnt30_myphone_myphone
,b.imei_cnt7
,b.imei_cnt7_bankcard_bankcard
,b.imei_cnt7_bankcard_companyphone
,b.imei_cnt7_bankcard_contact
,b.imei_cnt7_bankcard_device
,b.imei_cnt7_bankcard_email
,b.imei_cnt7_bankcard_emergency
,b.imei_cnt7_bankcard_idcard
,b.imei_cnt7_bankcard_myphone
,b.imei_cnt7_companyphone_bankcard
,b.imei_cnt7_companyphone_companyphone
,b.imei_cnt7_companyphone_contact
,b.imei_cnt7_companyphone_device
,b.imei_cnt7_companyphone_email
,b.imei_cnt7_companyphone_emergency
,b.imei_cnt7_companyphone_idcard
,b.imei_cnt7_companyphone_myphone
,b.imei_cnt7_contact_bankcard
,b.imei_cnt7_contact_companyphone
,b.imei_cnt7_contact_contact
,b.imei_cnt7_contact_device
,b.imei_cnt7_contact_email
,b.imei_cnt7_contact_emergency
,b.imei_cnt7_contact_idcard
,b.imei_cnt7_contact_myphone
,b.imei_cnt7_device_bankcard
,b.imei_cnt7_device_companyphone
,b.imei_cnt7_device_contact
,b.imei_cnt7_device_device
,b.imei_cnt7_device_email
,b.imei_cnt7_device_emergency
,b.imei_cnt7_device_idcard
,b.imei_cnt7_device_myphone
,b.imei_cnt7_email_bankcard
,b.imei_cnt7_email_companyphone
,b.imei_cnt7_email_contact
,b.imei_cnt7_email_device
,b.imei_cnt7_email_email
,b.imei_cnt7_email_emergency
,b.imei_cnt7_email_idcard
,b.imei_cnt7_email_myphone
,b.imei_cnt7_emergency_bankcard
,b.imei_cnt7_emergency_companyphone
,b.imei_cnt7_emergency_contact
,b.imei_cnt7_emergency_device
,b.imei_cnt7_emergency_email
,b.imei_cnt7_emergency_emergency
,b.imei_cnt7_emergency_idcard
,b.imei_cnt7_emergency_myphone
,b.imei_cnt7_idcard_bankcard
,b.imei_cnt7_idcard_companyphone
,b.imei_cnt7_idcard_contact
,b.imei_cnt7_idcard_device
,b.imei_cnt7_idcard_email
,b.imei_cnt7_idcard_emergency
,b.imei_cnt7_idcard_idcard
,b.imei_cnt7_idcard_myphone
,b.imei_cnt7_myphone_bankcard
,b.imei_cnt7_myphone_companyphone
,b.imei_cnt7_myphone_contact
,b.imei_cnt7_myphone_device
,b.imei_cnt7_myphone_email
,b.imei_cnt7_myphone_emergency
,b.imei_cnt7_myphone_idcard
,b.imei_cnt7_myphone_myphone
,b.mobile_cnt
,b.mobile_cnt_bankcard_bankcard
,b.mobile_cnt_bankcard_companyphone
,b.mobile_cnt_bankcard_contact
,b.mobile_cnt_bankcard_device
,b.mobile_cnt_bankcard_email
,b.mobile_cnt_bankcard_emergency
,b.mobile_cnt_bankcard_idcard
,b.mobile_cnt_bankcard_myphone
,b.mobile_cnt_companyphone_bankcard
,b.mobile_cnt_companyphone_companyphone
,b.mobile_cnt_companyphone_contact
,b.mobile_cnt_companyphone_device
,b.mobile_cnt_companyphone_email
,b.mobile_cnt_companyphone_emergency
,b.mobile_cnt_companyphone_idcard
,b.mobile_cnt_companyphone_myphone
,b.mobile_cnt_contact_bankcard
,b.mobile_cnt_contact_companyphone
,b.mobile_cnt_contact_contact
,b.mobile_cnt_contact_device
,b.mobile_cnt_contact_email
,b.mobile_cnt_contact_emergency
,b.mobile_cnt_contact_idcard
,b.mobile_cnt_contact_myphone
,b.mobile_cnt_device_bankcard
,b.mobile_cnt_device_companyphone
,b.mobile_cnt_device_contact
,b.mobile_cnt_device_device
,b.mobile_cnt_device_email
,b.mobile_cnt_device_emergency
,b.mobile_cnt_device_idcard
,b.mobile_cnt_device_myphone
,b.mobile_cnt_email_bankcard
,b.mobile_cnt_email_companyphone
,b.mobile_cnt_email_contact
,b.mobile_cnt_email_device
,b.mobile_cnt_email_email
,b.mobile_cnt_email_emergency
,b.mobile_cnt_email_idcard
,b.mobile_cnt_email_myphone
,b.mobile_cnt_emergency_bankcard
,b.mobile_cnt_emergency_companyphone
,b.mobile_cnt_emergency_contact
,b.mobile_cnt_emergency_device
,b.mobile_cnt_emergency_email
,b.mobile_cnt_emergency_emergency
,b.mobile_cnt_emergency_idcard
,b.mobile_cnt_emergency_myphone
,b.mobile_cnt_idcard_bankcard
,b.mobile_cnt_idcard_companyphone
,b.mobile_cnt_idcard_contact
,b.mobile_cnt_idcard_device
,b.mobile_cnt_idcard_email
,b.mobile_cnt_idcard_emergency
,b.mobile_cnt_idcard_idcard
,b.mobile_cnt_idcard_myphone
,b.mobile_cnt_myphone_bankcard
,b.mobile_cnt_myphone_companyphone
,b.mobile_cnt_myphone_contact
,b.mobile_cnt_myphone_device
,b.mobile_cnt_myphone_email
,b.mobile_cnt_myphone_emergency
,b.mobile_cnt_myphone_idcard
,b.mobile_cnt_myphone_myphone
,b.mobile_cnt1
,b.mobile_cnt1_bankcard_bankcard
,b.mobile_cnt1_bankcard_companyphone
,b.mobile_cnt1_bankcard_contact
,b.mobile_cnt1_bankcard_device
,b.mobile_cnt1_bankcard_email
,b.mobile_cnt1_bankcard_emergency
,b.mobile_cnt1_bankcard_idcard
,b.mobile_cnt1_bankcard_myphone
,b.mobile_cnt1_companyphone_bankcard
,b.mobile_cnt1_companyphone_companyphone
,b.mobile_cnt1_companyphone_contact
,b.mobile_cnt1_companyphone_device
,b.mobile_cnt1_companyphone_email
,b.mobile_cnt1_companyphone_emergency
,b.mobile_cnt1_companyphone_idcard
,b.mobile_cnt1_companyphone_myphone
,b.mobile_cnt1_contact_bankcard
,b.mobile_cnt1_contact_companyphone
,b.mobile_cnt1_contact_contact
,b.mobile_cnt1_contact_device
,b.mobile_cnt1_contact_email
,b.mobile_cnt1_contact_emergency
,b.mobile_cnt1_contact_idcard
,b.mobile_cnt1_contact_myphone
,b.mobile_cnt1_device_bankcard
,b.mobile_cnt1_device_companyphone
,b.mobile_cnt1_device_contact
,b.mobile_cnt1_device_device
,b.mobile_cnt1_device_email
,b.mobile_cnt1_device_emergency
,b.mobile_cnt1_device_idcard
,b.mobile_cnt1_device_myphone
,b.mobile_cnt1_email_bankcard
,b.mobile_cnt1_email_companyphone
,b.mobile_cnt1_email_contact
,b.mobile_cnt1_email_device
,b.mobile_cnt1_email_email
,b.mobile_cnt1_email_emergency
,b.mobile_cnt1_email_idcard
,b.mobile_cnt1_email_myphone
,b.mobile_cnt1_emergency_bankcard
,b.mobile_cnt1_emergency_companyphone
,b.mobile_cnt1_emergency_contact
,b.mobile_cnt1_emergency_device
,b.mobile_cnt1_emergency_email
,b.mobile_cnt1_emergency_emergency
,b.mobile_cnt1_emergency_idcard
,b.mobile_cnt1_emergency_myphone
,b.mobile_cnt1_idcard_bankcard
,b.mobile_cnt1_idcard_companyphone
,b.mobile_cnt1_idcard_contact
,b.mobile_cnt1_idcard_device
,b.mobile_cnt1_idcard_email
,b.mobile_cnt1_idcard_emergency
,b.mobile_cnt1_idcard_idcard
,b.mobile_cnt1_idcard_myphone
,b.mobile_cnt1_myphone_bankcard
,b.mobile_cnt1_myphone_companyphone
,b.mobile_cnt1_myphone_contact
,b.mobile_cnt1_myphone_device
,b.mobile_cnt1_myphone_email
,b.mobile_cnt1_myphone_emergency
,b.mobile_cnt1_myphone_idcard
,b.mobile_cnt1_myphone_myphone
,b.mobile_cnt3
,b.mobile_cnt3_bankcard_bankcard
,b.mobile_cnt3_bankcard_companyphone
,b.mobile_cnt3_bankcard_contact
,b.mobile_cnt3_bankcard_device
,b.mobile_cnt3_bankcard_email
,b.mobile_cnt3_bankcard_emergency
,b.mobile_cnt3_bankcard_idcard
,b.mobile_cnt3_bankcard_myphone
,b.mobile_cnt3_companyphone_bankcard
,b.mobile_cnt3_companyphone_companyphone
,b.mobile_cnt3_companyphone_contact
,b.mobile_cnt3_companyphone_device
,b.mobile_cnt3_companyphone_email
,b.mobile_cnt3_companyphone_emergency
,b.mobile_cnt3_companyphone_idcard
,b.mobile_cnt3_companyphone_myphone
,b.mobile_cnt3_contact_bankcard
,b.mobile_cnt3_contact_companyphone
,b.mobile_cnt3_contact_contact
,b.mobile_cnt3_contact_device
,b.mobile_cnt3_contact_email
,b.mobile_cnt3_contact_emergency
,b.mobile_cnt3_contact_idcard
,b.mobile_cnt3_contact_myphone
,b.mobile_cnt3_device_bankcard
,b.mobile_cnt3_device_companyphone
,b.mobile_cnt3_device_contact
,b.mobile_cnt3_device_device
,b.mobile_cnt3_device_email
,b.mobile_cnt3_device_emergency
,b.mobile_cnt3_device_idcard
,b.mobile_cnt3_device_myphone
,b.mobile_cnt3_email_bankcard
,b.mobile_cnt3_email_companyphone
,b.mobile_cnt3_email_contact
,b.mobile_cnt3_email_device
,b.mobile_cnt3_email_email
,b.mobile_cnt3_email_emergency
,b.mobile_cnt3_email_idcard
,b.mobile_cnt3_email_myphone
,b.mobile_cnt3_emergency_bankcard
,b.mobile_cnt3_emergency_companyphone
,b.mobile_cnt3_emergency_contact
,b.mobile_cnt3_emergency_device
,b.mobile_cnt3_emergency_email
,b.mobile_cnt3_emergency_emergency
,b.mobile_cnt3_emergency_idcard
,b.mobile_cnt3_emergency_myphone
,b.mobile_cnt3_idcard_bankcard
,b.mobile_cnt3_idcard_companyphone
,b.mobile_cnt3_idcard_contact
,b.mobile_cnt3_idcard_device
,b.mobile_cnt3_idcard_email
,b.mobile_cnt3_idcard_emergency
,b.mobile_cnt3_idcard_idcard
,b.mobile_cnt3_idcard_myphone
,b.mobile_cnt3_myphone_bankcard
,b.mobile_cnt3_myphone_companyphone
,b.mobile_cnt3_myphone_contact
,b.mobile_cnt3_myphone_device
,b.mobile_cnt3_myphone_email
,b.mobile_cnt3_myphone_emergency
,b.mobile_cnt3_myphone_idcard
,b.mobile_cnt3_myphone_myphone
,b.mobile_cnt30
,b.mobile_cnt30_bankcard_bankcard
,b.mobile_cnt30_bankcard_companyphone
,b.mobile_cnt30_bankcard_contact
,b.mobile_cnt30_bankcard_device
,b.mobile_cnt30_bankcard_email
,b.mobile_cnt30_bankcard_emergency
,b.mobile_cnt30_bankcard_idcard
,b.mobile_cnt30_bankcard_myphone
,b.mobile_cnt30_companyphone_bankcard
,b.mobile_cnt30_companyphone_companyphone
,b.mobile_cnt30_companyphone_contact
,b.mobile_cnt30_companyphone_device
,b.mobile_cnt30_companyphone_email
,b.mobile_cnt30_companyphone_emergency
,b.mobile_cnt30_companyphone_idcard
,b.mobile_cnt30_companyphone_myphone
,b.mobile_cnt30_contact_bankcard
,b.mobile_cnt30_contact_companyphone
,b.mobile_cnt30_contact_contact
,b.mobile_cnt30_contact_device
,b.mobile_cnt30_contact_email
,b.mobile_cnt30_contact_emergency
,b.mobile_cnt30_contact_idcard
,b.mobile_cnt30_contact_myphone
,b.mobile_cnt30_device_bankcard
,b.mobile_cnt30_device_companyphone
,b.mobile_cnt30_device_contact
,b.mobile_cnt30_device_device
,b.mobile_cnt30_device_email
,b.mobile_cnt30_device_emergency
,b.mobile_cnt30_device_idcard
,b.mobile_cnt30_device_myphone
,b.mobile_cnt30_email_bankcard
,b.mobile_cnt30_email_companyphone
,b.mobile_cnt30_email_contact
,b.mobile_cnt30_email_device
,b.mobile_cnt30_email_email
,b.mobile_cnt30_email_emergency
,b.mobile_cnt30_email_idcard
,b.mobile_cnt30_email_myphone
,b.mobile_cnt30_emergency_bankcard
,b.mobile_cnt30_emergency_companyphone
,b.mobile_cnt30_emergency_contact
,b.mobile_cnt30_emergency_device
,b.mobile_cnt30_emergency_email
,b.mobile_cnt30_emergency_emergency
,b.mobile_cnt30_emergency_idcard
,b.mobile_cnt30_emergency_myphone
,b.mobile_cnt30_idcard_bankcard
,b.mobile_cnt30_idcard_companyphone
,b.mobile_cnt30_idcard_contact
,b.mobile_cnt30_idcard_device
,b.mobile_cnt30_idcard_email
,b.mobile_cnt30_idcard_emergency
,b.mobile_cnt30_idcard_idcard
,b.mobile_cnt30_idcard_myphone
,b.mobile_cnt30_myphone_bankcard
,b.mobile_cnt30_myphone_companyphone
,b.mobile_cnt30_myphone_contact
,b.mobile_cnt30_myphone_device
,b.mobile_cnt30_myphone_email
,b.mobile_cnt30_myphone_emergency
,b.mobile_cnt30_myphone_idcard
,b.mobile_cnt30_myphone_myphone
,b.mobile_cnt7
,b.mobile_cnt7_bankcard_bankcard
,b.mobile_cnt7_bankcard_companyphone
,b.mobile_cnt7_bankcard_contact
,b.mobile_cnt7_bankcard_device
,b.mobile_cnt7_bankcard_email
,b.mobile_cnt7_bankcard_emergency
,b.mobile_cnt7_bankcard_idcard
,b.mobile_cnt7_bankcard_myphone
,b.mobile_cnt7_companyphone_bankcard
,b.mobile_cnt7_companyphone_companyphone
,b.mobile_cnt7_companyphone_contact
,b.mobile_cnt7_companyphone_device
,b.mobile_cnt7_companyphone_email
,b.mobile_cnt7_companyphone_emergency
,b.mobile_cnt7_companyphone_idcard
,b.mobile_cnt7_companyphone_myphone
,b.mobile_cnt7_contact_bankcard
,b.mobile_cnt7_contact_companyphone
,b.mobile_cnt7_contact_contact
,b.mobile_cnt7_contact_device
,b.mobile_cnt7_contact_email
,b.mobile_cnt7_contact_emergency
,b.mobile_cnt7_contact_idcard
,b.mobile_cnt7_contact_myphone
,b.mobile_cnt7_device_bankcard
,b.mobile_cnt7_device_companyphone
,b.mobile_cnt7_device_contact
,b.mobile_cnt7_device_device
,b.mobile_cnt7_device_email
,b.mobile_cnt7_device_emergency
,b.mobile_cnt7_device_idcard
,b.mobile_cnt7_device_myphone
,b.mobile_cnt7_email_bankcard
,b.mobile_cnt7_email_companyphone
,b.mobile_cnt7_email_contact
,b.mobile_cnt7_email_device
,b.mobile_cnt7_email_email
,b.mobile_cnt7_email_emergency
,b.mobile_cnt7_email_idcard
,b.mobile_cnt7_email_myphone
,b.mobile_cnt7_emergency_bankcard
,b.mobile_cnt7_emergency_companyphone
,b.mobile_cnt7_emergency_contact
,b.mobile_cnt7_emergency_device
,b.mobile_cnt7_emergency_email
,b.mobile_cnt7_emergency_emergency
,b.mobile_cnt7_emergency_idcard
,b.mobile_cnt7_emergency_myphone
,b.mobile_cnt7_idcard_bankcard
,b.mobile_cnt7_idcard_companyphone
,b.mobile_cnt7_idcard_contact
,b.mobile_cnt7_idcard_device
,b.mobile_cnt7_idcard_email
,b.mobile_cnt7_idcard_emergency
,b.mobile_cnt7_idcard_idcard
,b.mobile_cnt7_idcard_myphone
,b.mobile_cnt7_myphone_bankcard
,b.mobile_cnt7_myphone_companyphone
,b.mobile_cnt7_myphone_contact
,b.mobile_cnt7_myphone_device
,b.mobile_cnt7_myphone_email
,b.mobile_cnt7_myphone_emergency
,b.mobile_cnt7_myphone_idcard
,b.mobile_cnt7_myphone_myphone
,b.order_cnt
,b.order_cnt_bankcard_bankcard
,b.order_cnt_bankcard_companyphone
,b.order_cnt_bankcard_contact
,b.order_cnt_bankcard_device
,b.order_cnt_bankcard_email
,b.order_cnt_bankcard_emergency
,b.order_cnt_bankcard_idcard
,b.order_cnt_bankcard_myphone
,b.order_cnt_companyphone_bankcard
,b.order_cnt_companyphone_companyphone
,b.order_cnt_companyphone_contact
,b.order_cnt_companyphone_device
,b.order_cnt_companyphone_email
,b.order_cnt_companyphone_emergency
,b.order_cnt_companyphone_idcard
,b.order_cnt_companyphone_myphone
,b.order_cnt_contact_bankcard
,b.order_cnt_contact_companyphone
,b.order_cnt_contact_contact
,b.order_cnt_contact_device
,b.order_cnt_contact_email
,b.order_cnt_contact_emergency
,b.order_cnt_contact_idcard
,b.order_cnt_contact_myphone
,b.order_cnt_device_bankcard
,b.order_cnt_device_companyphone
,b.order_cnt_device_contact
,b.order_cnt_device_device
,b.order_cnt_device_email
,b.order_cnt_device_emergency
,b.order_cnt_device_idcard
,b.order_cnt_device_myphone
,b.order_cnt_email_bankcard
,b.order_cnt_email_companyphone
,b.order_cnt_email_contact
,b.order_cnt_email_device
,b.order_cnt_email_email
,b.order_cnt_email_emergency
,b.order_cnt_email_idcard
,b.order_cnt_email_myphone
,b.order_cnt_emergency_bankcard
,b.order_cnt_emergency_companyphone
,b.order_cnt_emergency_contact
,b.order_cnt_emergency_device
,b.order_cnt_emergency_email
,b.order_cnt_emergency_emergency
,b.order_cnt_emergency_idcard
,b.order_cnt_emergency_myphone
,b.order_cnt_idcard_bankcard
,b.order_cnt_idcard_companyphone
,b.order_cnt_idcard_contact
,b.order_cnt_idcard_device
,b.order_cnt_idcard_email
,b.order_cnt_idcard_emergency
,b.order_cnt_idcard_idcard
,b.order_cnt_idcard_myphone
,b.order_cnt_myphone_bankcard
,b.order_cnt_myphone_companyphone
,b.order_cnt_myphone_contact
,b.order_cnt_myphone_device
,b.order_cnt_myphone_email
,b.order_cnt_myphone_emergency
,b.order_cnt_myphone_idcard
,b.order_cnt_myphone_myphone
,b.pass_contract_cnt
,b.pass_contract_cnt_bankcard_bankcard
,b.pass_contract_cnt_bankcard_companyphone
,b.pass_contract_cnt_bankcard_contact
,b.pass_contract_cnt_bankcard_device
,b.pass_contract_cnt_bankcard_email
,b.pass_contract_cnt_bankcard_emergency
,b.pass_contract_cnt_bankcard_idcard
,b.pass_contract_cnt_bankcard_myphone
,b.pass_contract_cnt_companyphone_bankcard
,b.pass_contract_cnt_companyphone_companyphone
,b.pass_contract_cnt_companyphone_contact
,b.pass_contract_cnt_companyphone_device
,b.pass_contract_cnt_companyphone_email
,b.pass_contract_cnt_companyphone_emergency
,b.pass_contract_cnt_companyphone_idcard
,b.pass_contract_cnt_companyphone_myphone
,b.pass_contract_cnt_contact_bankcard
,b.pass_contract_cnt_contact_companyphone
,b.pass_contract_cnt_contact_contact
,b.pass_contract_cnt_contact_device
,b.pass_contract_cnt_contact_email
,b.pass_contract_cnt_contact_emergency
,b.pass_contract_cnt_contact_idcard
,b.pass_contract_cnt_contact_myphone
,b.pass_contract_cnt_device_bankcard
,b.pass_contract_cnt_device_companyphone
,b.pass_contract_cnt_device_contact
,b.pass_contract_cnt_device_device
,b.pass_contract_cnt_device_email
,b.pass_contract_cnt_device_emergency
,b.pass_contract_cnt_device_idcard
,b.pass_contract_cnt_device_myphone
,b.pass_contract_cnt_email_bankcard
,b.pass_contract_cnt_email_companyphone
,b.pass_contract_cnt_email_contact
,b.pass_contract_cnt_email_device
,b.pass_contract_cnt_email_email
,b.pass_contract_cnt_email_emergency
,b.pass_contract_cnt_email_idcard
,b.pass_contract_cnt_email_myphone
,b.pass_contract_cnt_emergency_bankcard
,b.pass_contract_cnt_emergency_companyphone
,b.pass_contract_cnt_emergency_contact
,b.pass_contract_cnt_emergency_device
,b.pass_contract_cnt_emergency_email
,b.pass_contract_cnt_emergency_emergency
,b.pass_contract_cnt_emergency_idcard
,b.pass_contract_cnt_emergency_myphone
,b.pass_contract_cnt_idcard_bankcard
,b.pass_contract_cnt_idcard_companyphone
,b.pass_contract_cnt_idcard_contact
,b.pass_contract_cnt_idcard_device
,b.pass_contract_cnt_idcard_email
,b.pass_contract_cnt_idcard_emergency
,b.pass_contract_cnt_idcard_idcard
,b.pass_contract_cnt_idcard_myphone
,b.pass_contract_cnt_myphone_bankcard
,b.pass_contract_cnt_myphone_companyphone
,b.pass_contract_cnt_myphone_contact
,b.pass_contract_cnt_myphone_device
,b.pass_contract_cnt_myphone_email
,b.pass_contract_cnt_myphone_emergency
,b.pass_contract_cnt_myphone_idcard
,b.pass_contract_cnt_myphone_myphone
,b.product_cnt
,b.product_cnt_bankcard_bankcard
,b.product_cnt_bankcard_companyphone
,b.product_cnt_bankcard_contact
,b.product_cnt_bankcard_device
,b.product_cnt_bankcard_email
,b.product_cnt_bankcard_emergency
,b.product_cnt_bankcard_idcard
,b.product_cnt_bankcard_myphone
,b.product_cnt_companyphone_bankcard
,b.product_cnt_companyphone_companyphone
,b.product_cnt_companyphone_contact
,b.product_cnt_companyphone_device
,b.product_cnt_companyphone_email
,b.product_cnt_companyphone_emergency
,b.product_cnt_companyphone_idcard
,b.product_cnt_companyphone_myphone
,b.product_cnt_contact_bankcard
,b.product_cnt_contact_companyphone
,b.product_cnt_contact_contact
,b.product_cnt_contact_device
,b.product_cnt_contact_email
,b.product_cnt_contact_emergency
,b.product_cnt_contact_idcard
,b.product_cnt_contact_myphone
,b.product_cnt_device_bankcard
,b.product_cnt_device_companyphone
,b.product_cnt_device_contact
,b.product_cnt_device_device
,b.product_cnt_device_email
,b.product_cnt_device_emergency
,b.product_cnt_device_idcard
,b.product_cnt_device_myphone
,b.product_cnt_email_bankcard
,b.product_cnt_email_companyphone
,b.product_cnt_email_contact
,b.product_cnt_email_device
,b.product_cnt_email_email
,b.product_cnt_email_emergency
,b.product_cnt_email_idcard
,b.product_cnt_email_myphone
,b.product_cnt_emergency_bankcard
,b.product_cnt_emergency_companyphone
,b.product_cnt_emergency_contact
,b.product_cnt_emergency_device
,b.product_cnt_emergency_email
,b.product_cnt_emergency_emergency
,b.product_cnt_emergency_idcard
,b.product_cnt_emergency_myphone
,b.product_cnt_idcard_bankcard
,b.product_cnt_idcard_companyphone
,b.product_cnt_idcard_contact
,b.product_cnt_idcard_device
,b.product_cnt_idcard_email
,b.product_cnt_idcard_emergency
,b.product_cnt_idcard_idcard
,b.product_cnt_idcard_myphone
,b.product_cnt_myphone_bankcard
,b.product_cnt_myphone_companyphone
,b.product_cnt_myphone_contact
,b.product_cnt_myphone_device
,b.product_cnt_myphone_email
,b.product_cnt_myphone_emergency
,b.product_cnt_myphone_idcard
,b.product_cnt_myphone_myphone
,b.q_order_cnt
,b.q_order_cnt_bankcard_bankcard
,b.q_order_cnt_bankcard_companyphone
,b.q_order_cnt_bankcard_contact
,b.q_order_cnt_bankcard_device
,b.q_order_cnt_bankcard_email
,b.q_order_cnt_bankcard_emergency
,b.q_order_cnt_bankcard_idcard
,b.q_order_cnt_bankcard_myphone
,b.q_order_cnt_companyphone_bankcard
,b.q_order_cnt_companyphone_companyphone
,b.q_order_cnt_companyphone_contact
,b.q_order_cnt_companyphone_device
,b.q_order_cnt_companyphone_email
,b.q_order_cnt_companyphone_emergency
,b.q_order_cnt_companyphone_idcard
,b.q_order_cnt_companyphone_myphone
,b.q_order_cnt_contact_bankcard
,b.q_order_cnt_contact_companyphone
,b.q_order_cnt_contact_contact
,b.q_order_cnt_contact_device
,b.q_order_cnt_contact_email
,b.q_order_cnt_contact_emergency
,b.q_order_cnt_contact_idcard
,b.q_order_cnt_contact_myphone
,b.q_order_cnt_device_bankcard
,b.q_order_cnt_device_companyphone
,b.q_order_cnt_device_contact
,b.q_order_cnt_device_device
,b.q_order_cnt_device_email
,b.q_order_cnt_device_emergency
,b.q_order_cnt_device_idcard
,b.q_order_cnt_device_myphone
,b.q_order_cnt_email_bankcard
,b.q_order_cnt_email_companyphone
,b.q_order_cnt_email_contact
,b.q_order_cnt_email_device
,b.q_order_cnt_email_email
,b.q_order_cnt_email_emergency
,b.q_order_cnt_email_idcard
,b.q_order_cnt_email_myphone
,b.q_order_cnt_emergency_bankcard
,b.q_order_cnt_emergency_companyphone
,b.q_order_cnt_emergency_contact
,b.q_order_cnt_emergency_device
,b.q_order_cnt_emergency_email
,b.q_order_cnt_emergency_emergency
,b.q_order_cnt_emergency_idcard
,b.q_order_cnt_emergency_myphone
,b.q_order_cnt_idcard_bankcard
,b.q_order_cnt_idcard_companyphone
,b.q_order_cnt_idcard_contact
,b.q_order_cnt_idcard_device
,b.q_order_cnt_idcard_email
,b.q_order_cnt_idcard_emergency
,b.q_order_cnt_idcard_idcard
,b.q_order_cnt_idcard_myphone
,b.q_order_cnt_myphone_bankcard
,b.q_order_cnt_myphone_companyphone
,b.q_order_cnt_myphone_contact
,b.q_order_cnt_myphone_device
,b.q_order_cnt_myphone_email
,b.q_order_cnt_myphone_emergency
,b.q_order_cnt_myphone_idcard
,b.q_order_cnt_myphone_myphone
,b.tnh_cnt
,b.tnh_cnt_bankcard_bankcard
,b.tnh_cnt_bankcard_companyphone
,b.tnh_cnt_bankcard_contact
,b.tnh_cnt_bankcard_device
,b.tnh_cnt_bankcard_email
,b.tnh_cnt_bankcard_emergency
,b.tnh_cnt_bankcard_idcard
,b.tnh_cnt_bankcard_myphone
,b.tnh_cnt_companyphone_bankcard
,b.tnh_cnt_companyphone_companyphone
,b.tnh_cnt_companyphone_contact
,b.tnh_cnt_companyphone_device
,b.tnh_cnt_companyphone_email
,b.tnh_cnt_companyphone_emergency
,b.tnh_cnt_companyphone_idcard
,b.tnh_cnt_companyphone_myphone
,b.tnh_cnt_contact_bankcard
,b.tnh_cnt_contact_companyphone
,b.tnh_cnt_contact_contact
,b.tnh_cnt_contact_device
,b.tnh_cnt_contact_email
,b.tnh_cnt_contact_emergency
,b.tnh_cnt_contact_idcard
,b.tnh_cnt_contact_myphone
,b.tnh_cnt_device_bankcard
,b.tnh_cnt_device_companyphone
,b.tnh_cnt_device_contact
,b.tnh_cnt_device_device
,b.tnh_cnt_device_email
,b.tnh_cnt_device_emergency
,b.tnh_cnt_device_idcard
,b.tnh_cnt_device_myphone
,b.tnh_cnt_email_bankcard
,b.tnh_cnt_email_companyphone
,b.tnh_cnt_email_contact
,b.tnh_cnt_email_device
,b.tnh_cnt_email_email
,b.tnh_cnt_email_emergency
,b.tnh_cnt_email_idcard
,b.tnh_cnt_email_myphone
,b.tnh_cnt_emergency_bankcard
,b.tnh_cnt_emergency_companyphone
,b.tnh_cnt_emergency_contact
,b.tnh_cnt_emergency_device
,b.tnh_cnt_emergency_email
,b.tnh_cnt_emergency_emergency
,b.tnh_cnt_emergency_idcard
,b.tnh_cnt_emergency_myphone
,b.tnh_cnt_idcard_bankcard
,b.tnh_cnt_idcard_companyphone
,b.tnh_cnt_idcard_contact
,b.tnh_cnt_idcard_device
,b.tnh_cnt_idcard_email
,b.tnh_cnt_idcard_emergency
,b.tnh_cnt_idcard_idcard
,b.tnh_cnt_idcard_myphone
,b.tnh_cnt_myphone_bankcard
,b.tnh_cnt_myphone_companyphone
,b.tnh_cnt_myphone_contact
,b.tnh_cnt_myphone_device
,b.tnh_cnt_myphone_email
,b.tnh_cnt_myphone_emergency
,b.tnh_cnt_myphone_idcard
,b.tnh_cnt_myphone_myphone
,b.yfq_cnt
,b.yfq_cnt_bankcard_bankcard
,b.yfq_cnt_bankcard_companyphone
,b.yfq_cnt_bankcard_contact
,b.yfq_cnt_bankcard_device
,b.yfq_cnt_bankcard_email
,b.yfq_cnt_bankcard_emergency
,b.yfq_cnt_bankcard_idcard
,b.yfq_cnt_bankcard_myphone
,b.yfq_cnt_companyphone_bankcard
,b.yfq_cnt_companyphone_companyphone
,b.yfq_cnt_companyphone_contact
,b.yfq_cnt_companyphone_device
,b.yfq_cnt_companyphone_email
,b.yfq_cnt_companyphone_emergency
,b.yfq_cnt_companyphone_idcard
,b.yfq_cnt_companyphone_myphone
,b.yfq_cnt_contact_bankcard
,b.yfq_cnt_contact_companyphone
,b.yfq_cnt_contact_contact
,b.yfq_cnt_contact_device
,b.yfq_cnt_contact_email
,b.yfq_cnt_contact_emergency
,b.yfq_cnt_contact_idcard
,b.yfq_cnt_contact_myphone
,b.yfq_cnt_device_bankcard
,b.yfq_cnt_device_companyphone
,b.yfq_cnt_device_contact
,b.yfq_cnt_device_device
,b.yfq_cnt_device_email
,b.yfq_cnt_device_emergency
,b.yfq_cnt_device_idcard
,b.yfq_cnt_device_myphone
,b.yfq_cnt_email_bankcard
,b.yfq_cnt_email_companyphone
,b.yfq_cnt_email_contact
,b.yfq_cnt_email_device
,b.yfq_cnt_email_email
,b.yfq_cnt_email_emergency
,b.yfq_cnt_email_idcard
,b.yfq_cnt_email_myphone
,b.yfq_cnt_emergency_bankcard
,b.yfq_cnt_emergency_companyphone
,b.yfq_cnt_emergency_contact
,b.yfq_cnt_emergency_device
,b.yfq_cnt_emergency_email
,b.yfq_cnt_emergency_emergency
,b.yfq_cnt_emergency_idcard
,b.yfq_cnt_emergency_myphone
,b.yfq_cnt_idcard_bankcard
,b.yfq_cnt_idcard_companyphone
,b.yfq_cnt_idcard_contact
,b.yfq_cnt_idcard_device
,b.yfq_cnt_idcard_email
,b.yfq_cnt_idcard_emergency
,b.yfq_cnt_idcard_idcard
,b.yfq_cnt_idcard_myphone
,b.yfq_cnt_myphone_bankcard
,b.yfq_cnt_myphone_companyphone
,b.yfq_cnt_myphone_contact
,b.yfq_cnt_myphone_device
,b.yfq_cnt_myphone_email
,b.yfq_cnt_myphone_emergency
,b.yfq_cnt_myphone_idcard
,b.yfq_cnt_myphone_myphone
from knowledge_graph.degree1_features_data a
left join knowledge_graph.degree2_features_data b on a.order_id_src = b.order_id_src
join fqz.fqz_contract_performance_data c on a.order_id_src = c.order_id;