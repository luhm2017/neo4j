--����������ӣ���Ʒ���࣬���Ƿ����к�����

--һ�ȹ�������
create table degree1_features(order_id_src string,cnt int) PARTITIONED BY ( title string);
create table degree2_features(order_id_src string,cnt int) PARTITIONED BY ( title string);

--����׼��
--================================================
--��ȡ��ͬ����
create table temp_contract_data as 
select a.order_id from  fqz.fqz_knowledge_graph_data_external a 
where a.type = 'pass';

--����һ��ȡ�������ݣ�����ʱ������
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

--���Դ����������ʱ�䷶Χ��չ
create table temp_degree1_relation_data_src as 
select  
tab.order_id_src,tab.apply_date_src,tab.cert_no_src,
tab.order_id_src as order_id_dst1,tab.apply_date_src as apply_date_dst1, tab.cert_no_src as cert_no_dst1  from 
(select a.order_id_src,a.apply_date_src,a.cert_no_src from temp_degree1_relation_data a group by a.order_id_src,a.apply_date_src,a.cert_no_src) tab
union all 
select a.order_id_src,a.apply_date_src,a.cert_no_src,a.order_id_dst1,a.apply_date_dst1,a.cert_no_dst1
from temp_degree1_relation_data a;

--������������  �����ӹ��������š�ʱ��
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
case when b.product_name = '�׷���' then 1 else 0 end as yfq_dst1,
case when b.product_name = '���㻹' then 1 else 0 end as tnh_dst1,
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

--��ȡ����������
--�˹����ԼӺ�����Ϊ���ԼӺ����ݺϲ�
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
union all --�ں�ͬ��ȡ 
select a.order_id as content, 'black_contract' as type from fqz.fqz_fraud_contract_data_with_attribute a
union all
select a.order_id as content,'black_contract' as type  from (
select b.apply_id from fqz.fqz_community_black_data b 
where b.type = 1 and b.origin = 0 and b.apply_id <> '' group by b.apply_id) tab
join fqz.fqz_knowledge_graph_data_external a on tab.apply_id = a.contract_no;
--���ݳ���
insert overwrite table fqz_black_attribute_data
select content,type from fqz_black_attribute_data group by content,type;

--================================================================================================

--ָ��ͳ��
--================================================================================================
--������ͬ����ָ��
FROM (select * from temp_degree1_relation_data_attribute where order_id_src <> order_id_dst1 ) a
INSERT INTO degree1_features partition (title='order_cnt')  --һ�Ⱥ�������������
SELECT a.order_id_src, count(distinct a.order_id_dst1) cnt group by  a.order_id_src 
INSERT INTO degree1_features partition (title='pass_contract_cnt')   --һ�Ⱥ�����ͨ����ͬ����
SELECT a.order_id_src, sum(a.pass_contract_dst1) cnt  group by  a.order_id_src
INSERT INTO degree1_features partition (title='q_order_cnt')   --һ�Ⱥ�����Q�궩������
SELECT a.order_id_src, sum(a.q_refuse_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='current_overdue0_contract_cnt')   --һ�Ⱥ�����ǰ�����ں�ͬ����
select a.order_id_src, sum(a.current_overdue0_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='current_overdue3_contract_cnt')   --һ�Ⱥ�����ǰ3+��ͬ����
select a.order_id_src, sum(a.current_overdue3_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='current_overdue30_contract_cnt')   --һ�Ⱥ�����ǰ30+��ͬ����
select a.order_id_src, sum(a.current_overdue30_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='history_overdue0_contract_cnt')   --һ�Ⱥ�������ʷ�����ں�ͬ����
select a.order_id_src, sum(a.history_overdue0_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='history_overdue3_contract_cnt')   --һ�Ⱥ�������ʷ3+��ͬ����
select a.order_id_src, sum(a.history_overdue3_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='history_overdue30_contract_cnt')  --һ�Ⱥ�������ʷ30+��ͬ����
select a.order_id_src, sum(a.history_overdue30_dst1) cnt group by a.order_id_src;

--������ָ�꣬�����ڶ�����ͬ����ָ�꣨����ԭʼ������
FROM (select * from temp_degree1_relation_data_attribute) a
INSERT INTO degree1_features partition (title='cid_cnt')   --һ�Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt  group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt')  --һ�Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt')  --һ�Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt')  --һ�Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt')  --һ�Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt')  --һ�Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt')  --һ�Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt')  --һ�Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src 
--�����Ʒָ��
INSERT INTO degree1_features partition (title='product_cnt')  --һ�Ⱥ������ܲ�Ʒ��
select a.order_id_src, count(distinct a.product_name_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='yfq_cnt')  --һ�Ⱥ�����yfq����
select a.order_id_src, sum(a.yfq_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='tnh_cnt')  --һ�Ⱥ�����tnh����
select a.order_id_src, sum(a.tnh_dst1) cnt group by a.order_id_src;  

--������Сͼָ�꣬��ʱ��1\3\7\30��Ƭ
--===================================================================================================
FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 1) a
INSERT INTO degree1_features partition (title='cid_cnt1')   --һ�Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt1')  --һ�Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt1')  --һ�Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt1')  --һ�Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt1')  --һ�Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt1')  --һ�Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt1')  --һ�Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt1')  --һ�Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;

FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 7) a
INSERT INTO degree1_features partition (title='cid_cnt7')   --һ�Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt7')  --һ�Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt7')  --һ�Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt7')  --һ�Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt7')  --һ�Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt7')  --һ�Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt7')  --һ�Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt7')  --һ�Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;

FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 30) a
INSERT INTO degree1_features partition (title='cid_cnt30')   --һ�Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt30')  --һ�Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt30')  --һ�Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt30')  --һ�Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt30')  --һ�Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt30')  --һ�Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt30')  --һ�Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt30')  --һ�Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;
--===================================================================================================

--���������к�ָ��
INSERT INTO degree1_features partition (title='black_cid_cnt')   --һ�Ⱥ���������֤����
select a.order_id_src,count(distinct a.cert_no_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.cert_no_dst1 = b.CONTENT
where  b.type = 'black_cid' GROUP BY a.order_id_src ;
INSERT INTO degree1_features partition (title='black_mobile_cnt')   --һ�Ⱥ�������ֻ�����
select a.order_id_src,count(distinct a.mobile_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.mobile_dst1 = b.CONTENT
where  b.type = 'black_mobile' GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_bankcard_cnt')   --һ�Ⱥ���������п�����
select a.order_id_src,count(distinct a.loan_pan_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.loan_pan_dst1 = b.CONTENT
where  b.type = 'black_bankcard' GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_imei_cnt')   --һ�Ⱥ������IMEI����
select a.order_id_src,count(distinct a.device_id_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.device_id_dst1 = b.CONTENT
where  b.type = 'black_imei' GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_email_cnt')   --һ�Ⱥ������Email����
select a.order_id_src,count(distinct a.email_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.email_dst1 = b.CONTENT
where  b.type = 'black_email' GROUP BY a.order_id_src ;
INSERT INTO degree1_features partition (title='black_company_phone_cnt')   --һ�Ⱥ�����ڵ�������
select a.order_id_src,count(distinct a.comp_phone_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.comp_phone_dst1 = b.CONTENT
where  b.type =  'black_company_phone' GROUP BY a.order_id_src; --�����Ƿ����򻯴���  
INSERT INTO degree1_features partition (title='black_contract_cnt')   --һ�Ⱥ�����ں�ͬ����
select a.order_id_src,count(distinct a.order_id_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.order_id_dst1 = b.CONTENT
where b.type = 'black_contract' GROUP BY a.order_id_src;


--================================================================================================================
--�ų������ָ��
FROM (select * from temp_degree1_relation_data_attribute where order_id_src <> order_id_dst1 and cert_no_src <> cert_no_dst1) a
INSERT INTO degree1_features partition (title='order_cnt_exception_self')  --һ���ų�������������
SELECT a.order_id_src, count(distinct a.order_id_dst1) cnt group by  a.order_id_src 
INSERT INTO degree1_features partition (title='pass_contract_cnt_exception_self')   --һ���ų�����ͨ����ͬ����
SELECT a.order_id_src, sum(a.pass_contract_dst1) cnt  group by  a.order_id_src
INSERT INTO degree1_features partition (title='q_order_cnt_exception_self')   --һ���ų�����Q�궩������
SELECT a.order_id_src, sum(a.q_refuse_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='current_overdue0_contract_cnt_exception_self')   --һ���ų�����ǰ�����ں�ͬ����
select a.order_id_src, sum(a.current_overdue0_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='current_overdue3_contract_cnt_exception_self')   --һ���ų�����ǰ3+��ͬ����
select a.order_id_src, sum(a.current_overdue3_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='current_overdue30_contract_cnt_exception_self')   --һ���ų�����ǰ30+��ͬ����
select a.order_id_src, sum(a.current_overdue30_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='history_overdue0_contract_cnt_exception_self')   --һ���ų�������ʷ�����ں�ͬ����
select a.order_id_src, sum(a.history_overdue0_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='history_overdue3_contract_cnt_exception_self')   --һ���ų�������ʷ3+��ͬ����
select a.order_id_src, sum(a.history_overdue3_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='history_overdue30_contract_cnt_exception_self')  --һ���ų�������ʷ30+��ͬ����
select a.order_id_src, sum(a.history_overdue30_dst1) cnt group by a.order_id_src;

--������ָ�꣬�����ڶ�����ͬ����ָ�꣨����ԭʼ������
FROM (select * from temp_degree1_relation_data_attribute where and cert_no_src <> cert_no_dst1) a
INSERT INTO degree1_features partition (title='cid_cnt_exception_self')   --һ���ų��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt  group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt_exception_self')  --һ���ų������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt_exception_self')  --һ���ų��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt_exception_self')  --һ���ų�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt_exception_self')  --һ���ų�����Email����
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt_exception_self')  --һ���ų�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt_exception_self')  --һ���ų���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt_exception_self')  --һ���ų�����������
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src 
--�����Ʒָ��
INSERT INTO degree1_features partition (title='product_cnt_exception_self')  --һ���ų������ܲ�Ʒ��
select a.order_id_src, count(distinct a.product_name_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='yfq_cnt_exception_self')  --һ���ų�����yfq����
select a.order_id_src, sum(a.yfq_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='tnh_cnt_exception_self')  --һ���ų�����tnh����
select a.order_id_src, sum(a.tnh_dst1) cnt group by a.order_id_src ; 

--������Сͼָ�꣬��ʱ��1\3\7\30��Ƭ
--===================================================================================================
FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 1 and cert_no_src <> cert_no_dst1) a
INSERT INTO degree1_features partition (title='cid_cnt1_exception_self')   --һ���ų��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt1_exception_self')  --һ���ų������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt1_exception_self')  --һ���ų��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt1_exception_self')  --һ���ų�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt1_exception_self')  --һ���ų�����Email����
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt1_exception_self')  --һ���ų�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt1_exception_self')  --һ���ų���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt1_exception_self')  --һ���ų�����������
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;

FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 3 and cert_no_src <> cert_no_dst1) a
INSERT INTO degree1_features partition (title='cid_cnt3_exception_self')   --һ���ų��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt3_exception_self')  --һ���ų������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt3_exception_self')  --һ���ų��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt3_exception_self')  --һ���ų�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt3_exception_self')  --һ���ų�����Email����
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt3_exception_self')  --һ���ų�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt3_exception_self')  --һ���ų���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt3_exception_self')  --һ���ų�����������
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;

FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 7 and cert_no_src <> cert_no_dst1) a
INSERT INTO degree1_features partition (title='cid_cnt7_exception_self')   --һ���ų��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt7_exception_self')  --һ���ų������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt7_exception_self')  --һ���ų��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt7_exception_self')  --һ���ų�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt7_exception_self')  --һ���ų�����Email����
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt7_exception_self')  --һ���ų�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt7_exception_self')  --һ���ų���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt7_exception_self')  --һ���ų�����������
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;

FROM (select * from temp_degree1_relation_data_attribute where datediff(apply_date_src,apply_date_dst1) <= 30 and cert_no_src <> cert_no_dst1) a
INSERT INTO degree1_features partition (title='cid_cnt30_exception_self')   --һ���ų��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst1) cnt group by  a.order_id_src
INSERT INTO degree1_features partition (title='mobile_cnt30_exception_self')  --һ���ų������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst1) cnt group by a.order_id_src 
INSERT INTO degree1_features partition (title='bankcard_cnt30_exception_self')  --һ���ų��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='imei_cnt30_exception_self')  --һ���ų�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='email_cnt30_exception_self')  --һ���ų�����Email����
select a.order_id_src, count(distinct a.email_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='contact_mobile_cnt30_exception_self')  --һ���ų�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='emergency_mobile_cnt30_exception_self')  --һ���ų���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst1) cnt group by a.order_id_src
INSERT INTO degree1_features partition (title='company_phone_cnt30_exception_self')  --һ���ų�����������
select a.order_id_src, count(distinct a.comp_phone_dst1) cnt group by a.order_id_src;
--===================================================================================================

--���������к�ָ��
INSERT INTO degree1_features partition (title='black_cid_cnt_exception_self')   --һ���ų���������֤����
select a.order_id_src,count(distinct a.cert_no_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.cert_no_dst1 = b.CONTENT
where  b.type = 'black_cid' and a.cert_no_src <> a.cert_no_dst1  GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_mobile_cnt_exception_self')   --һ���ų�������ֻ�����
select a.order_id_src,count(distinct a.mobile_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.mobile_dst1 = b.CONTENT
where  b.type = 'black_mobile' and a.cert_no_src <> a.cert_no_dst1  GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_bankcard_cnt_exception_self')   --һ���ų���������п�����
select a.order_id_src,count(distinct a.loan_pan_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.loan_pan_dst1 = b.CONTENT
where  b.type = 'black_bankcard' and a.cert_no_src <> a.cert_no_dst1  GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_imei_cnt_exception_self')   --һ���ų������IMEI����
select a.order_id_src,count(distinct a.device_id_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.device_id_dst1 = b.CONTENT
where  b.type = 'black_imei' and a.cert_no_src <> a.cert_no_dst1  GROUP BY a.order_id_src;
INSERT INTO degree1_features partition (title='black_email_cnt_exception_self')   --һ���ų������Email����
select a.order_id_src,count(distinct a.email_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.email_dst1 = b.CONTENT
where  b.type = 'black_email' and a.cert_no_src <> a.cert_no_dst1  GROUP BY a.order_id_src; 
INSERT INTO degree1_features partition (title='black_company_phone_cnt_exception_self')   --һ���ų�����ڵ�������
select a.order_id_src,count(distinct a.comp_phone_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.comp_phone_dst1 = b.CONTENT
where  b.type =  'black_company_phone' and a.cert_no_src <> a.cert_no_dst1   GROUP BY a.order_id_src; --�����Ƿ����򻯴���  
INSERT INTO degree1_features partition (title='black_contract_cnt_exception_self')   --һ���ų�����ں�ͬ����
select a.order_id_src,count(distinct a.order_id_dst1) as cnt from temp_degree1_relation_data_attribute a 
join fqz_black_attribute_data b on a.order_id_dst1 = b.CONTENT
where b.type = 'black_contract' and a.cert_no_src <> a.cert_no_dst1  GROUP BY a.order_id_src;


--====================================================================================================
--���ȹ���
--���ݶ���ȡ�������ݣ�����ʱ������ , ͨ���ͳһ�滻������
--������̫�󣬽��в��
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

--���Դ����������ʱ�䷶Χ��չ
create table temp_degree2_relation_data_src as
select
tab.order_id_src,tab.apply_date_src,tab.cert_no_src,
tab.order_id_src as order_id_dst2,tab.apply_date_src as apply_date_dst2, tab.cert_no_src as cert_no_dst2  from
(select a.order_id_src,a.apply_date_src,a.cert_no_src from temp_degree2_relation_data a group by a.order_id_src,a.apply_date_src,cert_no_src) tab
union all
select a.order_id_src,a.apply_date_src,a.cert_no_src,a.order_id_dst2,a.apply_date_dst2,a.cert_no_dst2
from temp_degree2_relation_data a;

--������������  �����ӹ��������š�ʱ��
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
case when b.product_name = '�׷���' then 1 else 0 end as yfq_dst2,
case when b.product_name = '���㻹' then 1 else 0 end as tnh_dst2,
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

--ָ��ͳ��
--================================================================================================
--������ͬ����ָ��
FROM (select * from temp_degree2_relation_data_attribute where order_id_src <> order_id_dst2 ) a
INSERT INTO degree2_features partition (title='order_cnt')  --���Ⱥ�������������
SELECT a.order_id_src, count(distinct a.order_id_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='pass_contract_cnt')   --���Ⱥ�����ͨ����ͬ����
SELECT a.order_id_src, sum(a.pass_contract_dst2) cnt  group by  a.order_id_src
INSERT INTO degree2_features partition (title='q_order_cnt')   --���Ⱥ�����Q�궩������
SELECT a.order_id_src, sum(a.q_refuse_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='current_overdue0_contract_cnt')   --���Ⱥ�����ǰ�����ں�ͬ����
select a.order_id_src, sum(a.current_overdue0_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='current_overdue3_contract_cnt')   --���Ⱥ�����ǰ3+��ͬ����
select a.order_id_src, sum(a.current_overdue3_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='current_overdue30_contract_cnt')   --���Ⱥ�����ǰ30+��ͬ����
select a.order_id_src, sum(a.current_overdue30_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='history_overdue0_contract_cnt')   --���Ⱥ�������ʷ�����ں�ͬ����
select a.order_id_src, sum(a.history_overdue0_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='history_overdue3_contract_cnt')   --���Ⱥ�������ʷ3+��ͬ����
select a.order_id_src, sum(a.history_overdue3_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='history_overdue30_contract_cnt')  --���Ⱥ�������ʷ30+��ͬ����
select a.order_id_src, sum(a.history_overdue30_dst2) cnt group by a.order_id_src;

--������ָ�꣬�����ڶ�����ͬ����ָ�꣨����ԭʼ������
FROM (select * from temp_degree2_relation_data_attribute) a
INSERT INTO degree2_features partition (title='cid_cnt')   --���Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt  group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt')  --���Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='bankcard_cnt')  --���Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt')  --���Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt')  --���Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt')  --���Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt')  --���Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt')  --���Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src
--�����Ʒָ��
INSERT INTO degree2_features partition (title='product_cnt')  --���Ⱥ������ܲ�Ʒ��
select a.order_id_src, count(distinct a.product_name_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='yfq_cnt')  --���Ⱥ�����yfq����
select a.order_id_src, sum(a.yfq_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='tnh_cnt')  --���Ⱥ�����tnh����
select a.order_id_src, sum(a.tnh_dst2) cnt group by a.order_id_src;

--������Сͼָ�꣬��ʱ��1\3\7\30��Ƭ
--===================================================================================================
FROM (select * from temp_degree2_relation_data_attribute where datediff(apply_date_src,apply_date_dst2) <= 1) a
INSERT INTO degree2_features partition (title='cid_cnt1')   --���Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt1')  --���Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='bankcard_cnt1')  --���Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt1')  --���Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt1')  --���Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt1')  --���Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt1')  --���Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt1')  --���Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

FROM (select * from temp_degree2_relation_data_attribute where datediff(apply_date_src,apply_date_dst2) <= 3) a
INSERT INTO degree2_features partition (title='cid_cnt3')   --���Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt3')  --���Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='bankcard_cnt3')  --���Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt3')  --���Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt3')  --���Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt3')  --���Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt3')  --���Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt3')  --���Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

FROM (select * from temp_degree2_relation_data_attribute where datediff(apply_date_src,apply_date_dst2) <= 7) a
INSERT INTO degree2_features partition (title='cid_cnt7')   --���Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt  group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt7')  --���Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='bankcard_cnt7')  --���Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt7')  --���Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt7')  --���Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt7')  --���Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt7')  --���Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt7')  --���Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

FROM (select * from temp_degree2_relation_data_attribute where datediff(apply_date_src,apply_date_dst2) <= 30) a
INSERT INTO degree2_features partition (title='cid_cnt30')   --���Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt30')  --���Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='bankcard_cnt30')  --���Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt30')  --���Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt30')  --���Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt30')  --���Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt30')  --���Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt30')  --���Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

--���������к�ָ��
INSERT INTO degree2_features partition (title='black_cid_cnt')   --���Ⱥ���������֤����
select a.order_id_src,count(distinct a.cert_no_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.cert_no_dst2 = b.CONTENT
where  b.type = 'black_cid' group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_mobile_cnt')   --���Ⱥ�������ֻ�����
select a.order_id_src,count(distinct a.mobile_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.mobile_dst2 = b.CONTENT
where  b.type = 'black_mobile' group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_bankcard_cnt')   --���Ⱥ���������п�����
select a.order_id_src,count(distinct a.loan_pan_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.loan_pan_dst2 = b.CONTENT
where  b.type = 'black_bankcard' group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_imei_cnt')   --���Ⱥ������IMEI����
select a.order_id_src,count(distinct a.device_id_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.device_id_dst2 = b.CONTENT
where  b.type = 'black_imei' group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_email_cnt')   --���Ⱥ������Email����
select a.order_id_src,count(distinct a.email_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.email_dst2 = b.CONTENT
where  b.type = 'black_email'  group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_company_phone_cnt')   --���Ⱥ�����ڵ�������
select a.order_id_src,count(distinct a.comp_phone_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.comp_phone_dst2 = b.CONTENT
where  b.type =  'black_company_phone' group by a.order_id_src; --�����Ƿ����򻯴���
INSERT INTO degree2_features partition (title='black_contract_cnt')   --���Ⱥ�����ں�ͬ����
select a.order_id_src,count(distinct a.order_id_dst2) as cnt from temp_degree2_relation_data_attribute a
join fqz_black_attribute_data b on a.order_id_dst2 = b.CONTENT
where b.type = 'black_contract' group by a.order_id_src;

--����թ���������
--==============================================================================
--�Ȼ���ȫ�����붩������ϴ�˵�����

--����һ��Ȧ���칲�������
