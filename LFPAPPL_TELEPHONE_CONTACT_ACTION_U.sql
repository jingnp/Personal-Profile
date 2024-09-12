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
coalesce(app."PEMAL",'null') as "PEMAL",
case when app."DTL__CAPXACTION"  = 'U'  then app."DTL__CI_PEMAL"
     when app."DTL__CAPXACTION"  = 'I'  and aci."RNK" <> 1  then (
          case when COALESCE(app."PEMAL" , 'null' )  <>  coalesce(LAG(app."PEMAL") OVER(PARTITION BY TRIM(app."PNO")  ORDER BY app."PNO",app."DTL__CAPXTIMESTAMP"),'null') then 'Y' else 'N' end )
else 'null' end as "DTL__CI_PEMAL",
coalesce(app."PIEMAL",'null') as "PIEMAL",
case when app."DTL__CAPXACTION"  = 'U'  then app."DTL__CI_PIEMAL"
     when app."DTL__CAPXACTION"  = 'I'  and aci."RNK" <> 1  then (
          case when COALESCE(app."PIEMAL" , 'null' )  <>  coalesce(LAG(app."PIEMAL") OVER(PARTITION BY TRIM(app."PNO")  ORDER BY app."PNO",app."DTL__CAPXTIMESTAMP"),'null') then 'Y' else 'N' end )
else 'null' end as "DTL__CI_PIEMAL",
app."PNAME",
case when app."DTL__CAPXACTION"  = 'U'  then app."DTL__CI_PNAME"
     when app."DTL__CAPXACTION"  = 'I'  and aci."RNK" <> 1  then (
          case when COALESCE(app."PNAME" , 'null' )  <>  coalesce(LAG(app."PNAME") OVER(PARTITION BY TRIM(app."PNO")  ORDER BY app."PNO",app."DTL__CAPXTIMESTAMP"),'null') then 'Y' else 'N' end )
else 'null' end as "DTL__CI_PNAME", app."PTEL1",  
case when app."DTL__CAPXACTION"  = 'U'  then app."DTL__CI_PTEL1"
     when app."DTL__CAPXACTION"  = 'I'  and aci."RNK" <> 1  then (
          case when COALESCE(app."PTEL1" , 'null' )  <>  COALESCE(LAG(app."PTEL1") OVER(PARTITION BY TRIM(app."PNO")  ORDER BY app."PNO",app."DTL__CAPXTIMESTAMP"), 'null' )  then 'Y' else 'N' end )
else 'null' end as "DTL__CI_PTEL1",
app."PTEL2",  
case when app."DTL__CAPXACTION"  = 'U'  then app."DTL__CI_PTEL2"
     when app."DTL__CAPXACTION"  = 'I'  and aci."RNK" <> 1  then (
          case when COALESCE(app."PTEL2" , 'null' )  <>  COALESCE(LAG(app."PTEL2") OVER(PARTITION BY TRIM(app."PNO")  ORDER BY app."PNO",app."DTL__CAPXTIMESTAMP"), 'null' )  then 'Y' else 'N' end )
else 'null' end as "DTL__CI_PTEL2",  
app."PMFXNO",  
case when app."DTL__CAPXACTION"  = 'U'  then app."DTL__CI_PMFXNO"
     when app."DTL__CAPXACTION"  = 'I'  and aci."RNK" <> 1  then (
          case when COALESCE(app."PMFXNO" , 'null' )  <>  COALESCE(LAG(app."PMFXNO") OVER(PARTITION BY TRIM(app."PNO")  ORDER BY app."PNO",app."DTL__CAPXTIMESTAMP"), 'null' ) then 'Y' else 'N' end )
else 'null' end as "DTL__CI_PMFXNO",  
app."POWNER",
case when app."DTL__CAPXACTION"  = 'U'  then app."DTL__CI_POWNER"
     when app."DTL__CAPXACTION"  = 'I'  and aci."RNK" <> 1  then (
          case when COALESCE(app."POWNER" , 'null' )  <>  coalesce(LAG(app."POWNER") OVER(PARTITION BY TRIM(app."PNO")  ORDER BY app."PNO",app."DTL__CAPXTIMESTAMP"),'null') then 'Y' else 'N' end )
else 'null' end as "DTL__CI_POWNER",
case when app."DTL__CAPXACTION"  = 'I' and aci."RNK" <> 1 then 'U' else app."DTL__CAPXACTION" end "DTL__CAPXACTION" ,
app."DTL__CAPXTIMESTAMP",
app."PMMBNO",  
case when app."DTL__CAPXACTION"  = 'U'  then app."DTL__CI_PMMBNO"
     when app."DTL__CAPXACTION"  = 'I'  and aci."RNK" <> 1  then (
          case when COALESCE(app."PMMBNO" , 'null' )  <>  COALESCE(LAG(app."PMMBNO") OVER(PARTITION BY TRIM(app."PNO")  ORDER BY app."PNO",app."DTL__CAPXTIMESTAMP"), 'null' )  then 'Y' else 'N' end )
else 'null' end as "DTL__CI_PMMBNO",  
app."PMOMBNO",  
case when app."DTL__CAPXACTION"  = 'U'  then app."DTL__CI_PMOMBNO"
when app."DTL__CAPXACTION"  = 'I'  and aci."RNK" <> 1  then (
case when COALESCE(app."PMOMBNO" , 'null' )  <>  COALESCE(LAG(app."PMOMBNO") OVER(PARTITION BY TRIM(app."PNO")  ORDER BY app."PNO",app."DTL__CAPXTIMESTAMP"), 'null' )  then 'Y' else 'N' end )
else 'null' end as "DTL__CI_PMOMBNO",  
app."MDM_BATCH_CNTL"  
from DEFINE_ACTION_I aci
inner join "Landing_RLS_Temp"."LFPAPPL"  app
on aci."PNO" = app."PNO"   and aci."DTL__CAPXTIMESTAMP" = app."DTL__CAPXTIMESTAMP"
where app."MDM_BATCH_CNTL" =  '"+context.max_batch_id+"' 
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
	WHERE 
(case when TRIM(A."DTL__CAPXACTION")||':'||TRIM(A."DTL__CI_PTEL1")='U:N' then FALSE
		when 	TRIM(A."DTL__CAPXACTION")||':'||TRIM(A."DTL__CI_PTEL1")='U:null' then FALSE
else TRUE end
		)			
	UNION ALL
	SELECT 
	distinct 
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'INS'||':'||'MOB' AS PKEY_SRC,
		LTRIM(RTRIM(A."PNO"))  AS PNO,
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'INS'  AS PARTY_PKEY_SRC,
		'MOB' AS TELEPHONE_ADDR_TYPE_CD,
		case when A."DTL__CAPXACTION" = 'I' then (case
		when A."PMMBNO"=''   then A."PMOMBNO"
		else A."PMMBNO" end)
		when A."DTL__CAPXACTION" = 'U' then (case
		when A."DTL__CI_PMMBNO"='N' then (case when A."DTL__CI_PMOMBNO" = 'Y' then A."PMOMBNO" else null end)
		else A."PMMBNO" end)
		else A."PMMBNO" end AS TELEPHONE_NO, --MODIFIED
		(A."DTL__CAPXACTION") AS TRANSACTION_FLAG,
		A."DTL__CAPXTIMESTAMP" AS LAST_UPDATED_DATE,
		A."MDM_BATCH_CNTL" AS BATCH_ID
	FROM LFPAPPL_VIEW A
		where
	case when TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(A."DTL__CI_PMMBNO")) = 'U:N' 
       then case when (A."POWNER" =''or A."POWNER" = 'null' or A."POWNER" is null)
		then true 
		else false end
		else true end
		AND (case when TRIM(A."DTL__CAPXACTION")||':'||TRIM(A."DTL__CI_PMMBNO")||':'||TRIM(A."DTL__CI_PMOMBNO")='U:N:N' then false else true end)
		AND (case when TRIM(A."DTL__CAPXACTION")||':'||TRIM(A."DTL__CI_PMMBNO")||':'||TRIM(A."DTL__CI_PMOMBNO")='U:null:null' then false else true end)	
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
	WHERE 
(case when
	TRIM(A."DTL__CAPXACTION")||':'||TRIM(A."DTL__CI_PTEL2") = 'U:N' then FALSE
	when
	TRIM(A."DTL__CAPXACTION")||':'||TRIM(A."DTL__CI_PTEL2") = 'U:null' then FALSE
	else TRUE end
	)
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
	WHERE  TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(cast(A."PMFXNO"as varchar)))<>'U:'
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
		  TRIM(A."POWNER") IS not NULL and TRIM(A."POWNER") <>'' AND TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(A."DTL__CI_PMOMBNO"))<>'U:N'
) X
WHERE  X.TRANSACTION_FLAG  <>'I'