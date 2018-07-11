use knowledge_graph;

--���ݶ���ȡ�������ݣ�����ʱ������ , ͨ���ͳһ�滻������
create table temp_degree2_relation_data_$edge1_$edge2 as 
SELECT a.order_id_src,
a.apply_date_src ,
a.cert_no_src,
a.order_id_dst2, 
a.apply_date_dst2,
a.cert_no_dst2
FROM fqz.fqz_relation_degree2  a 
join temp_contract_data b on a.order_id_src = b.order_id
where edg_type_src1 = '$Bedge1' and edg_type_src2 = '$Bedge2'
--and a.cert_no_src <> a.cert_no_dst2
GROUP BY 
a.order_id_src,
apply_date_src,
a.cert_no_src,
a.order_id_dst2, 
apply_date_dst2,
a.cert_no_dst2;

--���Դ����������ʱ�䷶Χ��չ
create table temp_degree2_relation_data_src_$edge1_$edge2 as 
select  
tab.order_id_src,tab.apply_date_src,tab.cert_no_src,
tab.order_id_src as order_id_dst2,tab.apply_date_src as apply_date_dst2, tab.cert_no_src as cert_no_dst2  from 
(select a.order_id_src,a.apply_date_src,a.cert_no_src from temp_degree2_relation_data_$edge1_$edge2 a group by a.order_id_src,a.apply_date_src,a.cert_no_src) tab
union all 
select a.order_id_src,a.apply_date_src,a.cert_no_src,a.order_id_dst2,a.apply_date_dst2,a.cert_no_dst2
from temp_degree2_relation_data_$edge1_$edge2 a;

--������������  �����ӹ��������š�ʱ��
create table temp_degree2_relation_data_attribute_$edge1_$edge2 as 
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
from temp_degree2_relation_data_src_$edge1_$edge2 a 
join fqz.fqz_knowledge_graph_data_external b on a.order_id_dst2 = b.order_id;

--================================================================================================

--ָ��ͳ��
--================================================================================================
--������ͬ����ָ��
FROM (select * from temp_degree2_relation_data_attribute_$edge1_$edge2 where order_id_src <> order_id_dst2 ) a
INSERT INTO degree2_features partition (title='order_cnt_$edge1_$edge2')  --���Ⱥ�������������
SELECT a.order_id_src, count(distinct a.order_id_dst2) cnt group by  a.order_id_src 
INSERT INTO degree2_features partition (title='pass_contract_cnt_$edge1_$edge2')   --���Ⱥ�����ͨ����ͬ����
SELECT a.order_id_src, sum(a.pass_contract_dst2) cnt  group by  a.order_id_src
INSERT INTO degree2_features partition (title='q_order_cnt_$edge1_$edge2')   --���Ⱥ�����Q�궩������
SELECT a.order_id_src, sum(a.q_refuse_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='current_overdue0_contract_cnt_$edge1_$edge2')   --���Ⱥ�����ǰ�����ں�ͬ����
select a.order_id_src, sum(a.current_overdue0_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='current_overdue3_contract_cnt_$edge1_$edge2')   --���Ⱥ�����ǰ3+��ͬ����
select a.order_id_src, sum(a.current_overdue3_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='current_overdue30_contract_cnt_$edge1_$edge2')   --���Ⱥ�����ǰ30+��ͬ����
select a.order_id_src, sum(a.current_overdue30_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='history_overdue0_contract_cnt_$edge1_$edge2')   --���Ⱥ�������ʷ�����ں�ͬ����
select a.order_id_src, sum(a.history_overdue0_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='history_overdue3_contract_cnt_$edge1_$edge2')   --���Ⱥ�������ʷ3+��ͬ����
select a.order_id_src, sum(a.history_overdue3_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='history_overdue30_contract_cnt_$edge1_$edge2')  --���Ⱥ�������ʷ30+��ͬ����
select a.order_id_src, sum(a.history_overdue30_dst2) cnt group by a.order_id_src;

--������ָ�꣬�����ڶ�����ͬ����ָ�꣨����ԭʼ������
FROM (select * from temp_degree2_relation_data_attribute_$edge1_$edge2) a
INSERT INTO degree2_features partition (title='cid_cnt_$edge1_$edge2')   --���Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt  group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt_$edge1_$edge2')  --���Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src 
INSERT INTO degree2_features partition (title='bankcard_cnt_$edge1_$edge2')  --���Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt_$edge1_$edge2')  --���Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt_$edge1_$edge2')  --���Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt_$edge1_$edge2')  --���Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt_$edge1_$edge2')  --���Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt_$edge1_$edge2')  --���Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src ;
--�����Ʒָ��
INSERT INTO degree2_features partition (title='product_cnt_$edge1_$edge2')  --���Ⱥ������ܲ�Ʒ��
select a.order_id_src, count(distinct a.product_name_dst2) cnt group by a.order_id_src 
INSERT INTO degree2_features partition (title='yfq_cnt_$edge1_$edge2')  --���Ⱥ�����yfq����
select a.order_id_src, sum(a.yfq_dst2) cnt group by a.order_id_src 
INSERT INTO degree2_features partition (title='tnh_cnt_$edge1_$edge2')  --���Ⱥ�����tnh����
select a.order_id_src, sum(a.tnh_dst2) cnt group by a.order_id_src  ;

--������Сͼָ�꣬��ʱ��1\3\7\30��Ƭ
--===================================================================================================
FROM (select * from temp_degree2_relation_data_attribute_$edge1_$edge2 where datediff(apply_date_src,apply_date_dst2) <= 1) a
INSERT INTO degree2_features partition (title='cid_cnt1_$edge1_$edge2')   --���Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt1_$edge1_$edge2')  --���Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src 
INSERT INTO degree2_features partition (title='bankcard_cnt1_$edge1_$edge2')  --���Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt1_$edge1_$edge2')  --���Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt1_$edge1_$edge2')  --���Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt1_$edge1_$edge2')  --���Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt1_$edge1_$edge2')  --���Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt1_$edge1_$edge2')  --���Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

FROM (select * from temp_degree2_relation_data_attribute_$edge1_$edge2 where datediff(apply_date_src,apply_date_dst2) <= 3) a
INSERT INTO degree2_features partition (title='cid_cnt3_$edge1_$edge2')   --���Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt3_$edge1_$edge2')  --���Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src 
INSERT INTO degree2_features partition (title='bankcard_cnt3_$edge1_$edge2')  --���Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt3_$edge1_$edge2')  --���Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt3_$edge1_$edge2')  --���Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt3_$edge1_$edge2')  --���Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt3_$edge1_$edge2')  --���Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt3_$edge1_$edge2')  --���Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

FROM (select * from temp_degree2_relation_data_attribute_$edge1_$edge2 where datediff(apply_date_src,apply_date_dst2) <= 7) a
INSERT INTO degree2_features partition (title='cid_cnt7_$edge1_$edge2')   --���Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt  group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt7_$edge1_$edge2')  --���Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src 
INSERT INTO degree2_features partition (title='bankcard_cnt7_$edge1_$edge2')  --���Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt7_$edge1_$edge2')  --���Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt7_$edge1_$edge2')  --���Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt7_$edge1_$edge2')  --���Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt7_$edge1_$edge2')  --���Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt7_$edge1_$edge2')  --���Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

FROM (select * from temp_degree2_relation_data_attribute_$edge1_$edge2 where datediff(apply_date_src,apply_date_dst2) <= 30) a
INSERT INTO degree2_features partition (title='cid_cnt30_$edge1_$edge2')   --���Ⱥ��������֤����
SELECT a.order_id_src, count(distinct a.cert_no_dst2) cnt group by  a.order_id_src
INSERT INTO degree2_features partition (title='mobile_cnt30_$edge1_$edge2')  --���Ⱥ������ֻ�������
select a.order_id_src, count(distinct a.mobile_dst2) cnt group by a.order_id_src 
INSERT INTO degree2_features partition (title='bankcard_cnt30_$edge1_$edge2')  --���Ⱥ��������п�����
select a.order_id_src, count(distinct a.loan_pan_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='imei_cnt30_$edge1_$edge2')  --���Ⱥ�����IMEI����
select a.order_id_src, count(distinct a.device_id_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='email_cnt30_$edge1_$edge2')  --���Ⱥ�����Email����
select a.order_id_src, count(distinct a.email_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='contact_mobile_cnt30_$edge1_$edge2')  --���Ⱥ�������ϵ���ֻ�����
select a.order_id_src, count(distinct a.contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='emergency_mobile_cnt30_$edge1_$edge2')  --���Ⱥ���������ֻ�����
select a.order_id_src, count(distinct a.emergency_contact_mobile_dst2) cnt group by a.order_id_src
INSERT INTO degree2_features partition (title='company_phone_cnt30_$edge1_$edge2')  --���Ⱥ�����������
select a.order_id_src, count(distinct a.comp_phone_dst2) cnt group by a.order_id_src;

--���������к�ָ��
INSERT INTO degree2_features partition (title='black_cid_cnt_$edge1_$edge2')   --���Ⱥ���������֤����
select a.order_id_src,count(distinct a.cert_no_dst2) as cnt from temp_degree2_relation_data_attribute_$edge1_$edge2 a 
join fqz_black_attribute_data b on a.cert_no_dst2 = b.CONTENT
where  b.type = 'black_cid' group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_mobile_cnt_$edge1_$edge2')   --���Ⱥ�������ֻ�����
select a.order_id_src,count(distinct a.mobile_dst2) as cnt from temp_degree2_relation_data_attribute_$edge1_$edge2 a 
join fqz_black_attribute_data b on a.mobile_dst2 = b.CONTENT
where  b.type = 'black_mobile' group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_bankcard_cnt_$edge1_$edge2')   --���Ⱥ���������п�����
select a.order_id_src,count(distinct a.loan_pan_dst2) as cnt from temp_degree2_relation_data_attribute_$edge1_$edge2 a 
join fqz_black_attribute_data b on a.loan_pan_dst2 = b.CONTENT
where  b.type = 'black_bankcard' group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_imei_cnt_$edge1_$edge2')   --���Ⱥ������IMEI����
select a.order_id_src,count(distinct a.device_id_dst2) as cnt from temp_degree2_relation_data_attribute_$edge1_$edge2 a 
join fqz_black_attribute_data b on a.device_id_dst2 = b.CONTENT
where  b.type = 'black_imei' group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_email_cnt_$edge1_$edge2')   --���Ⱥ������Email����
select a.order_id_src,count(distinct a.email_dst2) as cnt from temp_degree2_relation_data_attribute_$edge1_$edge2 a 
join fqz_black_attribute_data b on a.email_dst2 = b.CONTENT
where  b.type = 'black_email'  group by a.order_id_src;
INSERT INTO degree2_features partition (title='black_company_phone_cnt_$edge1_$edge2')   --���Ⱥ�����ڵ�������
select a.order_id_src,count(distinct a.comp_phone_dst2) as cnt from temp_degree2_relation_data_attribute_$edge1_$edge2 a 
join fqz_black_attribute_data b on a.comp_phone_dst2 = b.CONTENT
where  b.type =  'black_company_phone' group by a.order_id_src; --�����Ƿ����򻯴���  
INSERT INTO degree2_features partition (title='black_contract_cnt_$edge1_$edge2')   --���Ⱥ�����ں�ͬ����
select a.order_id_src,count(distinct a.order_id_dst2) as cnt from temp_degree2_relation_data_attribute_$edge1_$edge2 a 
join fqz_black_attribute_data b on a.order_id_dst2 = b.CONTENT
where b.type = 'black_contract' group by a.order_id_src;