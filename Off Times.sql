DECLARE @start_date DATETIME = :start_date,
		@end_date DATETIME = :end_date,
		@line VARCHAR(5) = :line;

with offs as (
	(SELECT
	jp.line
	,jp.start_time
	,jp.finish_time
	,datediff(minute,jp.start_time,jp.finish_time) as total_off_time
	,datediff(minute,jp.start_time,dateadd(hour,0,hr.time_hour)) as test
	,IIF((datediff(minute,jp.start_time,dateadd(hour,0,hr.time_hour)))<60, 
				--second look at the case that the off time started and finished in the hour
				IIF((datediff(minute,jp.start_time,dateadd(hour,0,hr.time_hour)))>60,datediff(minute,jp.start_time,dateadd(hour,0,hr.time_hour))-60,(datediff(minute,jp.start_time,dateadd(hour,0,hr.time_hour)))), 
				-- lastly look at the case that the job finished in the hour
				IIF(datediff(minute,dateadd(minute,-60,hr.time_hour),jp.finish_time)>60,60,IIF(datediff(minute,dateadd(minute,-60,hr.time_hour),ISNULL(jp.finish_time,dateadd(minute,690,GETDATE())))<60,datediff(minute,dateadd(minute,-60,hr.time_hour),ISNULL(jp.finish_time,dateadd(minute,690,GETDATE()))),60))) as hour_off
	
	,hrly.t_stamp	
	,hr.time_hour
	FROM vinpac_production.FSR_Hourly as hrly

	left join (
		select t_stamp,dateadd(hour,0,convert(datetime,round(convert(float,t_stamp)*24,0)/24)) as time_hour
		from vinpac_production.FSR_Hourly
		) as hr
		on hrly.t_stamp=hr.t_stamp
		
	left join (select * from vinpac_production.Job_performance 
	where job=0) as jp
		on hrly.t_stamp between jp.start_time and dateadd(hour,1,ISNULL(jp.finish_time,dateadd(hour, 60,jp.start_time)))  
		
	WHERE 

	 hrly.t_stamp between  @start_date  and  @end_date 
		)
	)
select 
	min(time_hour) as t_stamp
	,IIF(0=1,0,sum(IIF(line=1,IIF(hour_off>0,hour_off/4.,0),0))) as 'Line 1'
	,IIF(0=1,0,sum(IIF(line=2,IIF(hour_off>0,hour_off/4.,0),0))) as 'Line 2'
	,IIF(0=1,0,sum(IIF(line=8,IIF(hour_off>0,hour_off/4.,0),0))) as 'Line 8'
	,IIF(0=1,0,sum(IIF(line=9,IIF(hour_off>0,hour_off/4.,0),0))) as 'Line 9'
	,count(hour_off)
	,sum(test)/4
from offs
group by time_hour
order by time_hour
