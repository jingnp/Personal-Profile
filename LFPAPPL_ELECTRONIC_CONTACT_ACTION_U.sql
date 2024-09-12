with  DEFINE_ACTION_I AS  ( 

select distinct "PNO" ,"DTL__CAPXACTION", "MDM_BATCH_CNTL","DTL__CAPXTIMESTAMP", 

RANK() OVER(PARTITION BY TRIM("PNO","DTL__CAPXACTION")  ORDER by "DTL__CAPXTIMESTAMP") AS "RNK" 

from "Landing_RLS_Temp"."LFPAPPL"  

where "DTL__CAPXACTION" <> 'D'  

and  "MDM_BATCH_CNTL"  = '"+context.max_batch_id+"'  


) 
,
LFPAPPL_VIEW AS  ( 

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

else 'null' end as "DTL__CI_PNAME", 

case when app."POWNER" = '' then null 
     when app."POWNER" = 'null' then null else app."POWNER" end as "POWNER",

case when app."DTL__CAPXACTION"  = 'U'  then app."DTL__CI_POWNER" 

     when app."DTL__CAPXACTION"  = 'I'  and aci."RNK" <> 1  then (  

          case when COALESCE(app."POWNER" , 'null' )  <>  coalesce(LAG(app."POWNER") OVER(PARTITION BY TRIM(app."PNO")  ORDER BY app."PNO",app."DTL__CAPXTIMESTAMP"),'null') then 'Y' else 'N' end ) 

else 'null' end as "DTL__CI_POWNER",

case when app."DTL__CAPXACTION"  = 'I' and aci."RNK" <> 1 then 'U' else app."DTL__CAPXACTION" end "DTL__CAPXACTION" , 

app."DTL__CAPXTIMESTAMP",
app."MDM_BATCH_CNTL" 

from DEFINE_ACTION_I aci 

inner join "Landing_RLS_Temp"."LFPAPPL"  app 

on aci."PNO" = app."PNO"   and aci."DTL__CAPXTIMESTAMP" = app."DTL__CAPXTIMESTAMP" 

where app."MDM_BATCH_CNTL" = '"+context.max_batch_id+"' 


order by app."PNO",app."DTL__CAPXTIMESTAMP" 
)
select
X.PKEY_SRC,
X.PARTY_PKEY_SRC,
'LTH' AS ENTITY_CD,
X.ELECTRONIC_ADDR_TYPE_CD,
case when X.ELECTRONIC_ADDR_VALUE = '' then null
else X.ELECTRONIC_ADDR_VALUE end as ELECTRONIC_ADDR_VALUE,
null as DEL_FLG,
null as SOURCE_ROWID,
null as SRC_ROWID,
'RLS' AS SRC_SYSTEM_CD,
X.TRANSACTION_FLAG,
X.BATCH_ID,
--TO_TIMESTAMP(X.LAST_UPDATED_DATE,'YYYY-MM-DD HH24:MI:SS')
X.LAST_UPDATED_DATE
FROM
( 
	SELECT 
		distinct 
		'RLS'||':'||A."PNO"||':'||'OWN'||':'||'EML' AS PKEY_SRC,
		A."PNO" AS PNO,
		'RLS'||':'||A."PNO"||':'||'OWN' AS PARTY_PKEY_SRC,
		'EML' AS ELECTRONIC_ADDR_TYPE_CD,
		A."PEMAL"AS ELECTRONIC_ADDR_VALUE, --MODIFIED
	    (A."DTL__CAPXACTION") AS TRANSACTION_FLAG,
	    A."DTL__CAPXTIMESTAMP" AS LAST_UPDATED_DATE,
	    A."POWNER" AS NAME,
	   -- A."DTL__CI_PEMAL",
	    "MDM_BATCH_CNTL" as BATCH_ID,
	    ROW_NUMBER() OVER (PARTITION BY A."PNO" ORDER BY "DTL__CAPXTIMESTAMP" DESC) RNO
	    FROM LFPAPPL_VIEW A
	    where  TRIM(A."DTL__CAPXACTION") = 'U' 
	    and TRIM(A."POWNER") is not null
	    and  TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(A."DTL__CI_PEMAL"))<>'U:N'
	    --and A."PEMAL" <> ''
	    /*
	    TRIM(A."POWNER") <>'null' and TRIM(A."POWNER") <>''
		AND TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(A."PEMAL"))<>'I:'
	    AND (case when TRIM(A."DTL__CAPXACTION") = 'U' then (case when TRIM(A."DTL__CI_PEMAL") = 'N' then false else true end) else true end )
	    AND (case when TRIM(A."DTL__CAPXACTION") = 'U' then (case when TRIM(A."DTL__CI_PEMAL") = 'null' then false else true end) else true end )
        and  TRIM(A."POWNER") is not null  */
		union all 
		SELECT 
		distinct 
		'RLS'||':'||A."PNO"||':'||'INS'||':'||'EML'  AS PKEY_SRC,
		A."PNO" AS PNO,
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'INS' AS PARTY_PKEY_SRC,
		'EML' AS ELECTRONIC_ADDR_TYPE_CD,
		case when A."DTL__CI_PIEMAL" = 'Y' and A."PIEMAL" <> ''  then A."PIEMAL" 
			 when A."DTL__CI_PIEMAL" = 'Y' and A."PIEMAL" = ''   then A."PIEMAL" 
		     when A."DTL__CI_PIEMAL" = 'N' and A."POWNER" is null and A."DTL__CI_PEMAL" = 'Y'  then A."PEMAL" end AS ELECTRONIC_ADDR_VALUE, --MODIFIED
	    A."DTL__CAPXACTION" AS TRANSACTION_FLAG,
	    A."DTL__CAPXTIMESTAMP" AS LAST_UPDATED_DATE,
	    A."PNAME" AS NAME,
	   	--A."DTL__CI_PIEMAL",
	 	--A."PIEMAL",
		--A."PEMAL" ,
		--A."DTL__CI_PEMAL",
		--A."POWNER",
	    "MDM_BATCH_CNTL" as BATCH_ID,
	    ROW_NUMBER() OVER (PARTITION BY A."PNO" ORDER BY "DTL__CAPXTIMESTAMP" DESC) RNO
	    FROM LFPAPPL_VIEW A
	    where TRIM(A."DTL__CAPXACTION") = 'U' and
--	    and (case when A."DTL__CI_PIEMAL" = 'Y' and A."PIEMAL" <> ''  then A."PIEMAL" 
--			 when A."DTL__CI_PIEMAL" = 'Y' and A."PIEMAL" = ''   then A."PIEMAL" end) <>''
		--and  (case when A."DTL__CI_PIEMAL" = 'N' and A."POWNER" is null and A."DTL__CI_PEMAL" = 'Y' then true else false end)
	    		    case when TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(A."DTL__CI_PIEMAL")) = 'U:N' 
   	then case when (A."POWNER" <>'') OR A."POWNER" <> 'null' or A."POWNER" is not null 
		then false 
		else true end
		else true end
		AND ( TRIM(A."DTL__CAPXACTION")||':'||TRIM(A."DTL__CI_PIEMAL")||':'||TRIM(A."DTL__CI_PEMAL")not in ('U:N:N','U:null:null') )
		
) X
