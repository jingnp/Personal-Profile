with  DEFINE_ACTION_I AS  (  

select "PNO" ,"DTL__CAPXACTION", "MDM_BATCH_CNTL","DTL__CAPXTIMESTAMP",  

RANK() OVER(PARTITION BY TRIM("PNO","DTL__CAPXACTION")  ORDER by "DTL__CAPXTIMESTAMP") AS "RNK"  

from "Landing_RLS_Temp"."LFPAPPL"   

where "DTL__CAPXACTION" <> 'D'    

and  "MDM_BATCH_CNTL"   = '"+context.max_batch_id+"' 

) 
, LFPAPPL_VIEW AS  (  

select   

app."PNO" , 
app."PEMAL",
app."DTL__CI_PEMAL",
app."PIEMAL",
app."DTL__CI_PIEMAL",
app."PTEL1",  
app."DTL__CI_PTEL1",
app."PTEL2",  
app."DTL__CI_PTEL2" ,
app."PMFXNO",  
app."DTL__CI_PMFXNO"  ,
app."PMOMBNO",  
app."DTL__CI_PMOMBNO"  ,
app."PMMBNO",  
app."DTL__CI_PMMBNO"  ,
case when app."DTL__CAPXACTION"  = 'I' and aci."RNK" <> 1 then 'U' else app."DTL__CAPXACTION" end "DTL__CAPXACTION" ,  

app."DTL__CAPXTIMESTAMP",  

case when app."POWNER" = 'null' then null
when app."POWNER" ='' then null else app."POWNER" end as "POWNER", 

app."PNAME", 

app."MDM_BATCH_CNTL"  

from DEFINE_ACTION_I aci  

inner join "Landing_RLS_Temp"."LFPAPPL"  app  

on aci."PNO" = app."PNO"   and aci."DTL__CAPXTIMESTAMP" = app."DTL__CAPXTIMESTAMP"  

where app."MDM_BATCH_CNTL"  = '"+context.max_batch_id+"' 

order by app."PNO",app."DTL__CAPXTIMESTAMP"   


) 
select
X.PKEY_SRC,
X.PARTY_PKEY_SRC,
'LTH' AS ENTITY_CD,
null as AREA_CD,
null as county,
X.TELEPHONE_ADDR_TYPE_CD,
case when X.TELEPHONE_NO ='' then null
when X.TELEPHONE_NO = 'null' then null
else X.TELEPHONE_NO end as TELEPHONE_NO,
null as ext,
'' as del_flg,
null as sou_rowid,
null as src_rowid,
'RLS' AS SRC_SYSTEM_CD,
X.TRANSACTION_FLAG,
X.BATCH_ID,
X.LAST_UPDATED_DATE
FROM
(--INSURED
	select
	distinct 
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'INS'||':'||'HOM' AS PKEY_SRC,
		LTRIM(RTRIM(A."PNO"))  AS PNO,
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'INS'  AS PARTY_PKEY_SRC,
		'HOM' AS TELEPHONE_ADDR_TYPE_CD,
		case when A."DTL__CAPXACTION" ='U' and TRIM(A."DTL__CI_PTEL1")='Y' and LTRIM(RTRIM(cast(A."PTEL1" as varchar))) ='' then null else LTRIM(RTRIM(cast(A."PTEL1" as varchar))) end AS TELEPHONE_NO, --MODIFIED
		(A."DTL__CAPXACTION") AS TRANSACTION_FLAG,
		A."DTL__CAPXTIMESTAMP" AS LAST_UPDATED_DATE,
		A."MDM_BATCH_CNTL" AS BATCH_ID
	FROM LFPAPPL_VIEW A
--	WHERE 
--	 TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(cast(A."PTEL1" as varchar)))<>'I:'
	
	UNION ALL
	SELECT 
	distinct 
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'INS'||':'||'MOB' AS PKEY_SRC,
		LTRIM(RTRIM(A."PNO"))  AS PNO,
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'INS'  AS PARTY_PKEY_SRC,
		'MOB' AS TELEPHONE_ADDR_TYPE_CD,
case 
		when A."PMMBNO" ='' and (A."POWNER" =''or A."POWNER" = 'null' or A."POWNER" is null) then A."PMOMBNO" 
		else A."PMMBNO" end AS TELEPHONE_NO, 
		(A."DTL__CAPXACTION") AS TRANSACTION_FLAG,
		A."DTL__CAPXTIMESTAMP" AS LAST_UPDATED_DATE,
		A."MDM_BATCH_CNTL" AS BATCH_ID
	FROM LFPAPPL_VIEW A
	where  case when (A."PMMBNO" ='' or A."PMMBNO" is null) then 
    case when (A."POWNER"='' or A."POWNER"is null) 
    then true else false end else true end	
	UNION ALL
	SELECT 
	distinct 
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'INS'||':'||'OFF' AS PKEY_SRC,
		LTRIM(RTRIM(A."PNO"))  AS PNO,
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'INS'  AS PARTY_PKEY_SRC,
		'OFF' AS TELEPHONE_ADDR_TYPE_CD,
		LTRIM(RTRIM(cast(A."PTEL2" as varchar))) AS TELEPHONE_NO, --MODIFIED
		(A."DTL__CAPXACTION") AS TRANSACTION_FLAG,
		A."DTL__CAPXTIMESTAMP" AS LAST_UPDATED_DATE,
		A."MDM_BATCH_CNTL" AS BATCH_ID
	FROM LFPAPPL_VIEW A
--	WHERE TRIM(A."DTL__CAPXACTION")||':'||cast(A."PTEL2" as varchar)<>'I:'
	UNION all
	
	select
	distinct 
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'INS'||':'||'FAX' AS PKEY_SRC,
		LTRIM(RTRIM(A."PNO"))  AS PNO,
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'INS'  AS PARTY_PKEY_SRC,
		'FAX' AS TELEPHONE_ADDR_TYPE_CD,
		LTRIM(RTRIM(cast(A."PMFXNO"as varchar))) AS TELEPHONE_NO, --MODIFIED
		(A."DTL__CAPXACTION") AS TRANSACTION_FLAG,
		A."DTL__CAPXTIMESTAMP" AS LAST_UPDATED_DATE,
		A."MDM_BATCH_CNTL" AS BATCH_ID
	FROM LFPAPPL_VIEW A
--	WHERE  TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(cast(A."PMFXNO"as varchar)))<>'I:'

	UNION ALL
	SELECT 
	distinct 
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'OWN'||':'||'MOB' AS PKEY_SRC,
		LTRIM(RTRIM(A."PNO"))  AS PNO,
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'OWN'  AS PARTY_PKEY_SRC,
		'MOB' AS TELEPHONE_ADDR_TYPE_CD,
		LTRIM(RTRIM(cast(A."PMOMBNO" as varchar))) AS TELEPHONE_NO, --MODIFIED
		(A."DTL__CAPXACTION") AS TRANSACTION_FLAG,
		A."DTL__CAPXTIMESTAMP" AS LAST_UPDATED_DATE,
		A."MDM_BATCH_CNTL" AS BATCH_ID
	FROM LFPAPPL_VIEW A
	WHERE 
		  TRIM(A."POWNER") IS not NULL and TRIM(A."POWNER") <>'' 
--AND TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(A."POMBNO"))<>'I:'

) X

WHERE X.TRANSACTION_FLAG  <>'U'
