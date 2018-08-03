
--按边类型分组
create table temp_cert_no_edge_data as
select
a.cert_no_src,
a.edge_src,
a.edge_dst,
count(distinct a.cert_no_dst) as cnt
from fqz_degree1_relation_clean a 
group by a.cert_no_src,a.edge_src,a.edge_dst;

--关联边特征
create table temp_cert_no_degree1_feature as
select
a.cert_no_src,
max(case when a.edge_src = 'MYPHONE' and a.edge_dst = 'MYPHONE' then a.cnt else 0 end)  as mobile_mobile_cnt,
max(case when a.edge_src = 'MYPHONE' and a.edge_dst = 'CONTACT' then a.cnt else 0 end)  as mobile_contact_mobile_cnt,
max(case when a.edge_src = 'MYPHONE' and a.edge_dst = 'EMERGENCY' then a.cnt else 0 end)  as mobile_emergency_mobile_cnt,
max(case when a.edge_src = 'CONTACT' and a.edge_dst = 'MYPHONE' then a.cnt else 0 end)  as contact_mobile_mobile_cnt,
max(case when a.edge_src = 'CONTACT' and a.edge_dst = 'CONTACT' then a.cnt else 0 end)  as contact_mobile_contact_mobile_cnt,
max(case when a.edge_src = 'CONTACT' and a.edge_dst = 'EMERGENCY' then a.cnt else 0 end)  as contact_mobile_emergency_mobile_cnt,
max(case when a.edge_src = 'EMERGENCY' and a.edge_dst = 'MYPHONE' then a.cnt else 0 end)  as emergency_mobile_mobile_cnt,
max(case when a.edge_src = 'EMERGENCY' and a.edge_dst = 'CONTACT' then a.cnt else 0 end)  as emergency_mobile_contact_mobile_cnt,
max(case when a.edge_src = 'EMERGENCY' and a.edge_dst = 'EMERGENCY' then a.cnt else 0 end)  as emergency_mobile_emergency_mobile_cnt,
max(case when a.edge_src = 'EMAIL' and a.edge_dst = 'EMAIL' then a.cnt else 0 end)  as email_email_cnt,
max(case when a.edge_src = 'BANKCARD' and a.edge_dst = 'BANKCARD' then a.cnt else 0 end)  as bankcard_bankcard_cnt,
max(case when a.edge_src = 'DEVICE' and a.edge_dst = 'DEVICE' then a.cnt else 0 end)  as imei_imei_cnt,
max(case when a.edge_src = 'COMPANYPHONE' and a.edge_dst = 'COMPANYPHONE' then a.cnt else 0 end)  as company_phone_company_phone_cnt
from temp_cert_no_edge_data a
group by cert_no_src;

--按边类型分组，命中黑名单
create table temp_cert_no_edge_data_black as
select a.cert_no_src,
a.edge_src,
a.edge_dst,
count(distinct a.cert_no_dst) as cnt
from fqz_degree1_relation_clean a
join fqz_black_attribute_data b on a.cert_no_dst = b.content
where b.type = 'black_cid'
group by a.cert_no_src,a.edge_src,a.edge_dst;

--关联边命中黑名单特征
create table temp_cert_no_degree1_black_feature as
select
a.cert_no_src,
max(case when a.edge_src = 'MYPHONE' and a.edge_dst = 'MYPHONE' then a.cnt else 0 end)  as mobile_mobile_cnt_black,
max(case when a.edge_src = 'MYPHONE' and a.edge_dst = 'CONTACT' then a.cnt else 0 end)  as mobile_contact_mobile_cnt_black,
max(case when a.edge_src = 'MYPHONE' and a.edge_dst = 'EMERGENCY' then a.cnt else 0 end)  as mobile_emergency_mobile_cnt_black,
max(case when a.edge_src = 'CONTACT' and a.edge_dst = 'MYPHONE' then a.cnt else 0 end)  as contact_mobile_mobile_cnt_black,
max(case when a.edge_src = 'CONTACT' and a.edge_dst = 'CONTACT' then a.cnt else 0 end)  as contact_mobile_contact_mobile_cnt_black,
max(case when a.edge_src = 'CONTACT' and a.edge_dst = 'EMERGENCY' then a.cnt else 0 end)  as contact_mobile_emergency_mobile_cnt_black,
max(case when a.edge_src = 'EMERGENCY' and a.edge_dst = 'MYPHONE' then a.cnt else 0 end)  as emergency_mobile_mobile_cnt_black,
max(case when a.edge_src = 'EMERGENCY' and a.edge_dst = 'CONTACT' then a.cnt else 0 end)  as emergency_mobile_contact_mobile_cnt_black,
max(case when a.edge_src = 'EMERGENCY' and a.edge_dst = 'EMERGENCY' then a.cnt else 0 end)  as emergency_mobile_emergency_mobile_cnt_black,
max(case when a.edge_src = 'EMAIL' and a.edge_dst = 'EMAIL' then a.cnt else 0 end)  as email_email_cnt_black,
max(case when a.edge_src = 'BANKCARD' and a.edge_dst = 'BANKCARD' then a.cnt else 0 end)  as bankcard_bankcard_cnt_black,
max(case when a.edge_src = 'DEVICE' and a.edge_dst = 'DEVICE' then a.cnt else 0 end)  as imei_imei_cnt_black,
max(case when a.edge_src = 'COMPANYPHONE' and a.edge_dst = 'COMPANYPHONE' then a.cnt else 0 end)  as company_phone_company_phone_cnt_black
from temp_cert_no_edge_data_black a
group by cert_no_src;


--统计属性情况
create table temp_cert_no_degree1_data as
select
a.cert_no_src,
a.edge_src,
count(distinct a.content_key_mid) as cnt
from cert_no_fqz_degree1_relation a
group by a.cert_no_src,a.edge_src;

--关联属性特征
create table temp_cert_No_degree1_attr_features as
  select
a.cert_no_src,
max(case when a.edge_src = 'MYPHONE'  then a.cnt else 0 end)  as mobile_cnt,
max(case when a.edge_src = 'CONTACT'  then a.cnt else 0 end)  as contact_mobile_cnt,
max(case when a.edge_src = 'EMERGENCY'  then a.cnt else 0 end)  as emergency_mobile_cnt,
max(case when a.edge_src = 'EMAIL'  then a.cnt else 0 end)  as email_cnt,
max(case when a.edge_src = 'BANKCARD'  then a.cnt else 0 end)  as bankcard_cnt,
max(case when a.edge_src = 'DEVICE'  then a.cnt else 0 end)  as imei_cnt,
max(case when a.edge_src = 'COMPANYPHONE' then a.cnt else 0 end)  as company_phone_cnt
from temp_cert_no_degree1_data a
group by cert_no_src;

--统计属性命中黑名单
INSERT INTO cert_no_degree1_features partition (title='mobile_cnt_black')
select  a.cert_no_src,count(distinct a.content_key_mid) as cnt
from cert_no_fqz_degree1_relation  a
join fqz_black_attribute_data b on a.content_key_mid = b.content
where b.type = 'black_mobile' and a.edge_src = 'MYPHONE'
group by a.cert_no_src;
INSERT INTO cert_no_degree1_features partition (title='contact_mobile_cnt_black')
select  a.cert_no_src,count(distinct a.content_key_mid) as cnt
from cert_no_fqz_degree1_relation  a
join fqz_black_attribute_data b on a.content_key_mid = b.content
where b.type = 'black_mobile' and a.edge_src = 'CONTACT'
group by a.cert_no_src;
INSERT INTO cert_no_degree1_features partition (title='emergency_mobile_cnt_black')
select  a.cert_no_src,count(distinct a.content_key_mid) as cnt
from cert_no_fqz_degree1_relation  a
join fqz_black_attribute_data b on a.content_key_mid = b.content
where b.type = 'black_mobile' and a.edge_src = 'EMERGENCY'
group by a.cert_no_src;
INSERT INTO cert_no_degree1_features partition (title='company_phone_cnt_black')
select  a.cert_no_src,count(distinct a.content_key_mid) as cnt
from cert_no_fqz_degree1_relation  a
join fqz_black_attribute_data b on a.content_key_mid = b.content
where b.type = 'black_company_phone' and a.edge_src = 'COMPANYPHONE'
group by a.cert_no_src;
INSERT INTO cert_no_degree1_features partition (title='email_cnt_black')
select  a.cert_no_src,count(distinct a.content_key_mid) as cnt
from cert_no_fqz_degree1_relation  a
join fqz_black_attribute_data b on a.content_key_mid = b.content
where b.type = 'black_email' and a.edge_src = 'EMAIL'
group by a.cert_no_src;
INSERT INTO cert_no_degree1_features partition (title='bankcard_cnt_black')
select  a.cert_no_src,count(distinct a.content_key_mid) as cnt
from cert_no_fqz_degree1_relation  a
join fqz_black_attribute_data b on a.content_key_mid = b.content
where b.type = 'black_bankcard' and a.edge_src = 'BANKCARD'
group by a.cert_no_src;
INSERT INTO cert_no_degree1_features partition (title='imei_cnt_black')
select  a.cert_no_src,count(distinct a.content_key_mid) as cnt
from cert_no_fqz_degree1_relation  a
join fqz_black_attribute_data b on a.content_key_mid = b.content
where b.type = 'black_imei' and a.edge_src = 'DEVICE'
group by a.cert_no_src;

--统计黑属性
create table temp_cert_No_degree1_attr_black_features as
select
a.order_id_src as cert_no_src,
sum(case when a.title = 'mobile_cnt_black' then nvl(a.cnt,0) end) as mobile_cnt_black,
sum(case when a.title = 'contact_mobile_cnt_black' then nvl(a.cnt,0) end) as contact_mobile_cnt_black,
sum(case when a.title = 'emergency_mobile_cnt_black' then nvl(a.cnt,0) end) as emergency_mobile_cnt_black,
sum(case when a.title = 'email_cnt_black' then nvl(a.cnt,0) end) as email_cnt_black,
sum(case when a.title = 'bankcard_cnt_black' then nvl(a.cnt,0) end) as bankcard_cnt_black,
sum(case when a.title = 'imei_cnt_black' then nvl(a.cnt,0) end) as imei_cnt_black,
sum(case when a.title = 'company_phone_cnt_black' then nvl(a.cnt,0) end) as company_phone_cnt_black
from cert_no_degree1_features a
group by a.order_id_src;

--合并所有特征变量
create table cert_no_relation_features as
select
t.label	,
t.cert_no,
t.insert_time,
c.mobile_mobile_cnt	,	--关联统计
c.mobile_contact_mobile_cnt	,
c.mobile_emergency_mobile_cnt	,
c.contact_mobile_mobile_cnt	,
c.contact_mobile_contact_mobile_cnt	,
c.contact_mobile_emergency_mobile_cnt	,
c.emergency_mobile_mobile_cnt	,
c.emergency_mobile_contact_mobile_cnt	,
c.emergency_mobile_emergency_mobile_cnt	,
c.email_email_cnt	,
c.bankcard_bankcard_cnt	,
c.imei_imei_cnt	,
c.company_phone_company_phone_cnt	,
d.mobile_mobile_cnt_black	,	 --关联命中黑统计
d.mobile_contact_mobile_cnt_black	,
d.mobile_emergency_mobile_cnt_black	,
d.contact_mobile_mobile_cnt_black	,
d.contact_mobile_contact_mobile_cnt_black	,
d.contact_mobile_emergency_mobile_cnt_black	,
d.emergency_mobile_mobile_cnt_black	,
d.emergency_mobile_contact_mobile_cnt_black	,
d.emergency_mobile_emergency_mobile_cnt_black	,
d.email_email_cnt_black	,
d.bankcard_bankcard_cnt_black	,
d.imei_imei_cnt_black	,
a.mobile_cnt	,	--属性统计
a.contact_mobile_cnt	,
a.emergency_mobile_cnt	,
a.email_cnt	,
a.bankcard_cnt	,
a.imei_cnt	,
a.company_phone_cnt	,
b.mobile_cnt_black	,	--属性命中黑统计
b.contact_mobile_cnt_black	,
b.emergency_mobile_cnt_black	,
b.email_cnt_black	,
b.bankcard_cnt_black	,
b.imei_cnt_black	,
b.company_phone_cnt_black
from temp_fqz_sample_source t
left join temp_cert_No_degree1_attr_features a on t.cert_no = a.cert_no_src
left join temp_cert_No_degree1_attr_black_features b on t.cert_no = b.cert_no_src
left join temp_cert_no_degree1_feature c on t.cert_no = c.cert_no_src
left join temp_cert_no_degree1_black_feature d on t.cert_no = d.cert_no_src; 

--样本数据筛选
select * from fqz_degree1_relation_clean


--样本筛选
create table temp_sample_bad as
SELECT 1 as label ,c.insert_time, a.* FROM cert_no_relation_features a
join (SELECT  cert_no_src FROM knowledge_graph.fqz_degree1_relation_clean group by cert_no_src )  b on a.cert_no_src = b.cert_no_src
join fqz_cert_no_newest_sample_black c on a.cert_no_src = c.cert_no;

create table temp_sample_good as
SELECT 0 as label ,c.insert_time, a.* FROM cert_no_relation_features a
join (SELECT  cert_no_src FROM knowledge_graph.fqz_degree1_relation_clean group by cert_no_src )  b on a.cert_no_src = b.cert_no_src
join fqz_cert_no_newest_sample_good c on a.cert_no_src = c.cert_no;