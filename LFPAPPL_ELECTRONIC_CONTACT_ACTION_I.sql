with  DEFINE_ACTION_I AS  (  

select "PNO" ,"DTL__CAPXACTION", "MDM_BATCH_CNTL","DTL__CAPXTIMESTAMP",  

RANK() OVER(PARTITION BY TRIM("PNO","DTL__CAPXACTION")  ORDER by "DTL__CAPXTIMESTAMP") AS "RNK"  

from "Landing_RLS_Temp"."LFPAPPL"   

where "DTL__CAPXACTION" <> 'D'    

and  "MDM_BATCH_CNTL"  = '"+context.max_batch_id+"'  

) 
, LFPAPPL_VIEW AS  (  

select   

app."PNO" , 
app."PEMAL",
app."DTL__CI_PEMAL",
app."PIEMAL",
app."DTL__CI_PIEMAL",
case when app."DTL__CAPXACTION"  = 'I' and aci."RNK" <> 1 then 'U' else app."DTL__CAPXACTION" end "DTL__CAPXACTION" ,  
app."DTL__CAPXTIMESTAMP",  
case when app."POWNER" = '' then null 
     when app."POWNER" = 'null' then null else  app."POWNER" end as "POWNER", 
app."PNAME", 
app."MDM_BATCH_CNTL"  

from DEFINE_ACTION_I aci  

inner join "Landing_RLS_Temp"."LFPAPPL"  app  

on aci."PNO" = app."PNO"   and aci."DTL__CAPXTIMESTAMP" = app."DTL__CAPXTIMESTAMP"  

where app."MDM_BATCH_CNTL" = '"+context.max_batch_id+"'  

order by app."PNO",app."DTL__CAPXTIMESTAMP"   


) 
select
X.PKEY_SRC,
coalesce(X.PARTY_PKEY_SRC,'null') as PARTY_PKEY_SRC,
'LTH' AS ENTITY_CD,
X.ELECTRONIC_ADDR_TYPE_CD,
case when X.ELECTRONIC_ADDR_VALUE = '' then null
when X.ELECTRONIC_ADDR_VALUE ='null' then null
else trim(X.ELECTRONIC_ADDR_VALUE) end as ELECTRONIC_ADDR_VALUE,
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
	    "MDM_BATCH_CNTL" as BATCH_ID,
	    ROW_NUMBER() OVER (PARTITION BY A."PNO" ORDER BY "DTL__CAPXTIMESTAMP" DESC) RNO
	    FROM LFPAPPL_VIEW A
	    where A."DTL__CAPXACTION" = 'I' 
--	    and TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(A."PEMAL"))<>'I:'  
	    and TRIM(A."POWNER") is not null  and TRIM(A."POWNER") <>''
		
		union all 
		SELECT 
		distinct 
		'RLS'||':'||A."PNO"||':'||'INS'||':'||'EML'  AS PKEY_SRC,
		A."PNO" AS PNO,
		'RLS'||':'||LTRIM(RTRIM(A."PNO"))||':'||'INS' AS PARTY_PKEY_SRC,
		'EML' AS ELECTRONIC_ADDR_TYPE_CD,
		case when A."PIEMAL" ='' and A."POWNER" is null and A."PEMAL" <>'' then A."PEMAL" else A."PIEMAL" end AS ELECTRONIC_ADDR_VALUE, --MODIFIED
	    A."DTL__CAPXACTION" AS TRANSACTION_FLAG,
	    A."DTL__CAPXTIMESTAMP" AS LAST_UPDATED_DATE,
	    A."PNAME" ,
	    --A."PIEMAL",
	  --  A."PEMAL" ,
	   -- A."POWNER",
	    "MDM_BATCH_CNTL" as BATCH_ID,
	    ROW_NUMBER() OVER (PARTITION BY A."PNO" ORDER BY "DTL__CAPXTIMESTAMP" DESC) RNO
	    FROM LFPAPPL_VIEW A
where A."DTL__CAPXACTION" ='I' 
--and (case when A."PIEMAL" ='' and A."POWNER" is null and A."PEMAL" <>'' then A."PEMAL" else A."PIEMAL" end  ) <> ''
	/*	(case when TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(A."PIEMAL"))='I:' 
		then case when (A."POWNER" <>'') then false
		else true end
		else true end) or 
				(case when TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(A."PIEMAL"))='I:'
		then case when (A."POWNER" <> 'null') then false
		else true end
		else true end)or  
				(case when TRIM(A."DTL__CAPXACTION")||':'||LTRIM(RTRIM(A."PIEMAL"))='I:'
		then case when (A."POWNER" is not null) then false
		else true end
		else true end)

WHERE X.TRANSACTION_FLAG  <>'D' and X.TRANSACTION_FLAG  <>'U'
and TRIM(X.TRANSACTION_FLAG)||':'||LTRIM(RTRIM(X.ELECTRONIC_ADDR_VALUE))<>'I:' */
and case when (A."PIEMAL" ='' or A."PIEMAL" is null) then 
    case when (A."POWNER"='' or A."POWNER"is null) 
    then true else false end else true end
) X 
