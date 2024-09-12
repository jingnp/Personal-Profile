SELECT
   unnest(array['rrn','clntpfx','clntcoy','clntnum','rdidtelno','rmblphone','rpager','faxno','rinternet','rtaxidnum','rstaflag','splindic','zspecind','oldidno','user_profile','job_name','datime','clntpfx_bi','clntcoy_bi','clntnum_bi','rdidtelno_bi','rmblphone_bi','rpager_bi','faxno_bi','rinternet_bi','rtaxidnum_bi','rstaflag_bi','splindic_bi','zspecind_bi','oldidno_bi','user_profile_bi','job_name_bi','datime_bi','clntpfx_ci','clntcoy_ci','clntnum_ci','rdidtelno_ci','rmblphone_ci','rpager_ci','faxno_ci','rinternet_ci','rtaxidnum_ci','rstaflag_ci','splindic_ci','zspecind_ci','oldidno_ci','user_profile_ci','job_name_ci','datime_ci','ci_action','etl_batch_id','cl_tms','capxtimestamp','zwhidnum','zwhidnum_bi','zwhidnum_ci','ods_lth_batch_id','validflag','validflag_bi','validflag_ci','mdm_batch_cntl'
]) AS "Columns",
   unnest(array[aaa.rrn, aaa.clntpfx, aaa.clntcoy, aaa.clntnum, aaa.rdidtelno, aaa.rmblphone, aaa.rpager, aaa.faxno, aaa.rinternet, aaa.rtaxidnum, aaa.rstaflag, aaa.splindic, aaa.zspecind, aaa.oldidno, aaa.user_profile, aaa.job_name, aaa.datime, aaa.clntpfx_bi, aaa.clntcoy_bi, aaa.clntnum_bi, aaa.rdidtelno_bi, aaa.rmblphone_bi, aaa.rpager_bi, aaa.faxno_bi, aaa.rinternet_bi, aaa.rtaxidnum_bi, aaa.rstaflag_bi, aaa.splindic_bi, aaa.zspecind_bi, aaa.oldidno_bi, aaa.user_profile_bi, aaa.job_name_bi, aaa.datime_bi, aaa.clntpfx_ci, aaa.clntcoy_ci, aaa.clntnum_ci, aaa.rdidtelno_ci, aaa.rmblphone_ci, aaa.rpager_ci, aaa.faxno_ci, aaa.rinternet_ci, aaa.rtaxidnum_ci, aaa.rstaflag_ci, aaa.splindic_ci, aaa.zspecind_ci, aaa.oldidno_ci, aaa.user_profile_ci, aaa.job_name_ci, aaa.datime_ci, aaa.ci_action, aaa.etl_batch_id, aaa.cl_tms, aaa.capxtimestamp, aaa.zwhidnum, aaa.zwhidnum_bi, aaa.zwhidnum_ci, aaa.ods_lth_batch_id, aaa.validflag, aaa.validflag_bi, aaa.validflag_ci, aaa.mdm_batch_cntl]) AS "count zero/null/blank"
FROM (
select  COUNT(CASE WHEN rrn IS NULL or length(cast(rrn as varchar) )= 0 or cast(rrn as varchar) = '0' then 1 else null end ) as rrn
, COUNT(CASE WHEN clntpfx IS NULL or length(cast(clntpfx as varchar) )= 0 or cast(clntpfx as varchar) = '0' then 1 else null end ) as clntpfx
, COUNT(CASE WHEN clntcoy IS NULL or length(cast(clntcoy as varchar) )= 0 or cast(clntcoy as varchar) = '0' then 1 else null end ) as clntcoy
, COUNT(CASE WHEN clntnum IS NULL or length(cast(clntnum as varchar) )= 0 or cast(clntnum as varchar) = '0' then 1 else null end ) as clntnum
, COUNT(CASE WHEN rdidtelno IS NULL or length(cast(rdidtelno as varchar) )= 0 or cast(rdidtelno as varchar) = '0' then 1 else null end ) as rdidtelno
, COUNT(CASE WHEN rmblphone IS NULL or length(cast(rmblphone as varchar) )= 0 or cast(rmblphone as varchar) = '0' then 1 else null end ) as rmblphone
, COUNT(CASE WHEN rpager IS NULL or length(cast(rpager as varchar) )= 0 or cast(rpager as varchar) = '0' then 1 else null end ) as rpager
, COUNT(CASE WHEN faxno IS NULL or length(cast(faxno as varchar) )= 0 or cast(faxno as varchar) = '0' then 1 else null end ) as faxno
, COUNT(CASE WHEN rinternet IS NULL or length(cast(rinternet as varchar) )= 0 or cast(rinternet as varchar) = '0' then 1 else null end ) as rinternet
, COUNT(CASE WHEN rtaxidnum IS NULL or length(cast(rtaxidnum as varchar) )= 0 or cast(rtaxidnum as varchar) = '0' then 1 else null end ) as rtaxidnum
, COUNT(CASE WHEN rstaflag IS NULL or length(cast(rstaflag as varchar) )= 0 or cast(rstaflag as varchar) = '0' then 1 else null end ) as rstaflag
, COUNT(CASE WHEN splindic IS NULL or length(cast(splindic as varchar) )= 0 or cast(splindic as varchar) = '0' then 1 else null end ) as splindic
, COUNT(CASE WHEN zspecind IS NULL or length(cast(zspecind as varchar) )= 0 or cast(zspecind as varchar) = '0' then 1 else null end ) as zspecind
, COUNT(CASE WHEN oldidno IS NULL or length(cast(oldidno as varchar) )= 0 or cast(oldidno as varchar) = '0' then 1 else null end ) as oldidno
, COUNT(CASE WHEN user_profile IS NULL or length(cast(user_profile as varchar) )= 0 or cast(user_profile as varchar) = '0' then 1 else null end ) as user_profile
, COUNT(CASE WHEN job_name IS NULL or length(cast(job_name as varchar) )= 0 or cast(job_name as varchar) = '0' then 1 else null end ) as job_name
, COUNT(CASE WHEN datime IS NULL or length(cast(datime as varchar) )= 0 or cast(datime as varchar) = '0' then 1 else null end ) as datime
, COUNT(CASE WHEN clntpfx_bi IS NULL or length(cast(clntpfx_bi as varchar) )= 0 or cast(clntpfx_bi as varchar) = '0' then 1 else null end ) as clntpfx_bi
, COUNT(CASE WHEN clntcoy_bi IS NULL or length(cast(clntcoy_bi as varchar) )= 0 or cast(clntcoy_bi as varchar) = '0' then 1 else null end ) as clntcoy_bi
, COUNT(CASE WHEN clntnum_bi IS NULL or length(cast(clntnum_bi as varchar) )= 0 or cast(clntnum_bi as varchar) = '0' then 1 else null end ) as clntnum_bi
, COUNT(CASE WHEN rdidtelno_bi IS NULL or length(cast(rdidtelno_bi as varchar) )= 0 or cast(rdidtelno_bi as varchar) = '0' then 1 else null end ) as rdidtelno_bi
, COUNT(CASE WHEN rmblphone_bi IS NULL or length(cast(rmblphone_bi as varchar) )= 0 or cast(rmblphone_bi as varchar) = '0' then 1 else null end ) as rmblphone_bi
, COUNT(CASE WHEN rpager_bi IS NULL or length(cast(rpager_bi as varchar) )= 0 or cast(rpager_bi as varchar) = '0' then 1 else null end ) as rpager_bi
, COUNT(CASE WHEN faxno_bi IS NULL or length(cast(faxno_bi as varchar) )= 0 or cast(faxno_bi as varchar) = '0' then 1 else null end ) as faxno_bi
, COUNT(CASE WHEN rinternet_bi IS NULL or length(cast(rinternet_bi as varchar) )= 0 or cast(rinternet_bi as varchar) = '0' then 1 else null end ) as rinternet_bi
, COUNT(CASE WHEN rtaxidnum_bi IS NULL or length(cast(rtaxidnum_bi as varchar) )= 0 or cast(rtaxidnum_bi as varchar) = '0' then 1 else null end ) as rtaxidnum_bi
, COUNT(CASE WHEN rstaflag_bi IS NULL or length(cast(rstaflag_bi as varchar) )= 0 or cast(rstaflag_bi as varchar) = '0' then 1 else null end ) as rstaflag_bi
, COUNT(CASE WHEN splindic_bi IS NULL or length(cast(splindic_bi as varchar) )= 0 or cast(splindic_bi as varchar) = '0' then 1 else null end ) as splindic_bi
, COUNT(CASE WHEN zspecind_bi IS NULL or length(cast(zspecind_bi as varchar) )= 0 or cast(zspecind_bi as varchar) = '0' then 1 else null end ) as zspecind_bi
, COUNT(CASE WHEN oldidno_bi IS NULL or length(cast(oldidno_bi as varchar) )= 0 or cast(oldidno_bi as varchar) = '0' then 1 else null end ) as oldidno_bi
, COUNT(CASE WHEN user_profile_bi IS NULL or length(cast(user_profile_bi as varchar) )= 0 or cast(user_profile_bi as varchar) = '0' then 1 else null end ) as user_profile_bi
, COUNT(CASE WHEN job_name_bi IS NULL or length(cast(job_name_bi as varchar) )= 0 or cast(job_name_bi as varchar) = '0' then 1 else null end ) as job_name_bi
, COUNT(CASE WHEN datime_bi IS NULL or length(cast(datime_bi as varchar) )= 0 or cast(datime_bi as varchar) = '0' then 1 else null end ) as datime_bi
, COUNT(CASE WHEN clntpfx_ci IS NULL or length(cast(clntpfx_ci as varchar) )= 0 or cast(clntpfx_ci as varchar) = '0' then 1 else null end ) as clntpfx_ci
, COUNT(CASE WHEN clntcoy_ci IS NULL or length(cast(clntcoy_ci as varchar) )= 0 or cast(clntcoy_ci as varchar) = '0' then 1 else null end ) as clntcoy_ci
, COUNT(CASE WHEN clntnum_ci IS NULL or length(cast(clntnum_ci as varchar) )= 0 or cast(clntnum_ci as varchar) = '0' then 1 else null end ) as clntnum_ci
, COUNT(CASE WHEN rdidtelno_ci IS NULL or length(cast(rdidtelno_ci as varchar) )= 0 or cast(rdidtelno_ci as varchar) = '0' then 1 else null end ) as rdidtelno_ci
, COUNT(CASE WHEN rmblphone_ci IS NULL or length(cast(rmblphone_ci as varchar) )= 0 or cast(rmblphone_ci as varchar) = '0' then 1 else null end ) as rmblphone_ci
, COUNT(CASE WHEN rpager_ci IS NULL or length(cast(rpager_ci as varchar) )= 0 or cast(rpager_ci as varchar) = '0' then 1 else null end ) as rpager_ci
, COUNT(CASE WHEN faxno_ci IS NULL or length(cast(faxno_ci as varchar) )= 0 or cast(faxno_ci as varchar) = '0' then 1 else null end ) as faxno_ci
, COUNT(CASE WHEN rinternet_ci IS NULL or length(cast(rinternet_ci as varchar) )= 0 or cast(rinternet_ci as varchar) = '0' then 1 else null end ) as rinternet_ci
, COUNT(CASE WHEN rtaxidnum_ci IS NULL or length(cast(rtaxidnum_ci as varchar) )= 0 or cast(rtaxidnum_ci as varchar) = '0' then 1 else null end ) as rtaxidnum_ci
, COUNT(CASE WHEN rstaflag_ci IS NULL or length(cast(rstaflag_ci as varchar) )= 0 or cast(rstaflag_ci as varchar) = '0' then 1 else null end ) as rstaflag_ci
, COUNT(CASE WHEN splindic_ci IS NULL or length(cast(splindic_ci as varchar) )= 0 or cast(splindic_ci as varchar) = '0' then 1 else null end ) as splindic_ci
, COUNT(CASE WHEN zspecind_ci IS NULL or length(cast(zspecind_ci as varchar) )= 0 or cast(zspecind_ci as varchar) = '0' then 1 else null end ) as zspecind_ci
, COUNT(CASE WHEN oldidno_ci IS NULL or length(cast(oldidno_ci as varchar) )= 0 or cast(oldidno_ci as varchar) = '0' then 1 else null end ) as oldidno_ci
, COUNT(CASE WHEN user_profile_ci IS NULL or length(cast(user_profile_ci as varchar) )= 0 or cast(user_profile_ci as varchar) = '0' then 1 else null end ) as user_profile_ci
, COUNT(CASE WHEN job_name_ci IS NULL or length(cast(job_name_ci as varchar) )= 0 or cast(job_name_ci as varchar) = '0' then 1 else null end ) as job_name_ci
, COUNT(CASE WHEN datime_ci IS NULL or length(cast(datime_ci as varchar) )= 0 or cast(datime_ci as varchar) = '0' then 1 else null end ) as datime_ci
, COUNT(CASE WHEN ci_action IS NULL or length(cast(ci_action as varchar) )= 0 or cast(ci_action as varchar) = '0' then 1 else null end ) as ci_action
, COUNT(CASE WHEN etl_batch_id IS NULL or length(cast(etl_batch_id as varchar) )= 0 or cast(etl_batch_id as varchar) = '0' then 1 else null end ) as etl_batch_id
, COUNT(CASE WHEN cl_tms IS NULL or length(cast(cl_tms as varchar) )= 0 or cast(cl_tms as varchar) = '0' then 1 else null end ) as cl_tms
, COUNT(CASE WHEN capxtimestamp IS NULL or length(cast(capxtimestamp as varchar) )= 0 or cast(capxtimestamp as varchar) = '0' then 1 else null end ) as capxtimestamp
, COUNT(CASE WHEN zwhidnum IS NULL or length(cast(zwhidnum as varchar) )= 0 or cast(zwhidnum as varchar) = '0' then 1 else null end ) as zwhidnum
, COUNT(CASE WHEN zwhidnum_bi IS NULL or length(cast(zwhidnum_bi as varchar) )= 0 or cast(zwhidnum_bi as varchar) = '0' then 1 else null end ) as zwhidnum_bi
, COUNT(CASE WHEN zwhidnum_ci IS NULL or length(cast(zwhidnum_ci as varchar) )= 0 or cast(zwhidnum_ci as varchar) = '0' then 1 else null end ) as zwhidnum_ci
, COUNT(CASE WHEN ods_lth_batch_id IS NULL or length(cast(ods_lth_batch_id as varchar) )= 0 or cast(ods_lth_batch_id as varchar) = '0' then 1 else null end ) as ods_lth_batch_id
, COUNT(CASE WHEN validflag IS NULL or length(cast(validflag as varchar) )= 0 or cast(validflag as varchar) = '0' then 1 else null end ) as validflag
, COUNT(CASE WHEN validflag_bi IS NULL or length(cast(validflag_bi as varchar) )= 0 or cast(validflag_bi as varchar) = '0' then 1 else null end ) as validflag_bi
, COUNT(CASE WHEN validflag_ci IS NULL or length(cast(validflag_ci as varchar) )= 0 or cast(validflag_ci as varchar) = '0' then 1 else null end ) as validflag_ci
, COUNT(CASE WHEN mdm_batch_cntl IS NULL or length(cast(mdm_batch_cntl as varchar) )= 0 or cast(mdm_batch_cntl as varchar) = '0' then 1 else null end ) as mdm_batch_cntl
FROM "L_TH_DATACACHE".cl_clexpf
 ) aaa