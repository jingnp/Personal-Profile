select
sum(case when not(cltphone01::text ~ '^\d{10}$') and cltphone01<>'' then 1 else 0 end) as cltphone01
,sum(case when not(cltphone02::text ~ '^\d{09}$') and cltphone02<>'' then 1 else 0 end) as cltphone02

from "L_TH_DATACACHE"."cl_clntpf"

union
select 
sum(case when cltphone01<>'' then 1 else 0 end) as cltphone01
,sum(case when cltphone02<>'' then 1 else 0 end) as cltphone02

from "L_TH_DATACACHE"."cl_clntpf"