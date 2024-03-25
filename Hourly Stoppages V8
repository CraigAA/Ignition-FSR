with jobs as (
    select 
        jp.job
        ,erp.run_rate
        ,jp.line
        ,setup.date_end
        ,hrly.start_hour as hour
        ,(datediff(second,start_time,dateadd(minute,0,finish_time))+180)/60 as run_time
        ,hrly."24_7"    
        ,hrly.timeHour
        ,jp.start_time
        ,jp.setup_time
        ,
        IIF((datediff(minute,jp.start_time,hrly.timeHour))<60, 

                IIF(jp.active_time/60. between 1 and 60,datediff(minute,jp.start_time,jp.finish_time),(datediff(minute,jp.start_time,hrly.timeHour))), 

                IIF(datediff(minute,dateadd(minute,-60,hrly.timeHour),ISNULL(jp.finish_time,dateadd(hour,11,GETDATE())))>60,60,datediff(minute,dateadd(minute,-60,hrly.timeHour),ISNULL(jp.finish_time,dateadd(hour,11,GETDATE()))))) as job_running
            
            
        ,erp.run_rate*erp.pack_size/60. as sch_job_rate
            
--        IF the job start time is after the hour start time or before
            ,IIF(datediff(second,dateadd(minute,-60,hrly.timeHour),jp.start_time)/60.0>0,
			-- IF the setup occured in the hour
                IIF(datediff(second,hrly.timeHour,setup.date_end)/60.0<0,
					datediff(second,jp.start_time,setup.date_end)/60.0,
					--ELSE	
					60-datediff(second,dateadd(minute,-60,hrly.timeHour),jp.start_time)/60.0),
					
--  	The job start time is before the hour start time				
                IIF(datediff(second,dateadd(minute,-60,hrly.timeHour),setup.date_end )/60.0>60,60,
					--ELSE
					IIF(setup.date_end>jp.finish_time, datediff(second,dateadd(minute,-60,hrly.timeHour),jp.finish_time)/60.0,
						--ELSE
	                    IIF(datediff(second,dateadd(minute,-60,hrly.timeHour),setup.date_end )/60.0>0,
	                        datediff(second,dateadd(minute,-60,hrly.timeHour),setup.date_end)/60.0,
						--ELSE
						0))
                    ))
--)
		as time_sch_setup
        ,CNT
        ,ISNULL(pl.Count,0) as Warehouse
        ,(TIME_FLTSTP/60.0) as 'TIME_FLTSTP'
        ,(TIME_INFSTP/60.0) as 'TIME_INFSTP'
        ,(TIME_OUTSTP/60.0) as 'TIME_OUTSTP'
        ,(TIME_SAFSTP/60.0) as 'TIME_SAFSTP'
        ,(TIME_SETUP/60.0) as 'TIME_SETUP'
        ,(TIME_MISCSTP/60.0) as 'TIME_MISCSTP'
        ,(TIME_USRSTP/60.0) as 'TIME_USRSTP'
        ,(TIME_RUNOUT/60.0) as 'TIME_RO'
        ,(TIME_BREAK/60.0) as 'TIME_BREAK'
        ,(TIME_STP/60.0) as 'TIME_STP'
        ,60-TIME_STP/60.0-TIME_SETUP/60.0-TIME_BREAK/60.0 as 'Running'
        ,60 as 'Total'
        ,CNT/(60.0-TIME_BREAK/60.0) as 'Actual_Rate'    
        ,erp.duration         


        from  vinpac_production.job_performance jp              
        inner join vinpac_erp.production as erp on erp.job=jp.job and jp.line like :line

        outer apply (
                        select       IIF(ISNULL(:break1_date,0) between jp.start_time and dateadd(minute,jp.setup_time,jp.start_time),dateadd(minute,:break1_duration+jp.setup_time,jp.start_time),
                            IIF(ISNULL(:break2_date,0) between jp.start_time and dateadd(minute,jp.setup_time,jp.start_time),dateadd(minute,:break2_duration+jp.setup_time,jp.start_time),
                                IIF(ISNULL(:break3_date,0) between jp.start_time and dateadd(minute,jp.setup_time,jp.start_time),dateadd(minute,:break3_duration+jp.setup_time,jp.start_time),    
                                    IIF(dateadd(minute,:break1_duration,:break1_date) between jp.start_time and dateadd(minute,jp.setup_time,jp.start_time),dateadd(minute,datediff(minute,jp.start_time,dateadd(minute,:break1_duration,:break1_date))+jp.setup_time,jp.start_time),
                                        IIF(dateadd(minute,:break1_duration,:break1_date) between jp.start_time and dateadd(minute,jp.setup_time,jp.start_time),dateadd(minute,datediff(minute,jp.start_time,dateadd(minute,:break1_duration,:break1_date))+jp.setup_time,jp.start_time),
                                            IIF(dateadd(minute,:break2_duration,:break2_date) between jp.start_time and dateadd(minute,jp.setup_time,jp.start_time),dateadd(minute,datediff(minute,jp.start_time,dateadd(minute,:break2_duration,:break2_date))+jp.setup_time,jp.start_time),
                                                IIF(dateadd(minute,:break3_duration,:break3_date) between jp.start_time and dateadd(minute,jp.setup_time,jp.start_time),dateadd(minute,datediff(minute,jp.start_time,dateadd(minute,:break3_duration,:break3_date))+jp.setup_time,jp.start_time),
                                                dateadd(minute,jp.setup_time,jp.start_time)))))))) as 'date_end'
        ) as setup
       
    left JOIN    (
            SELECT *,iif(Hour!=0,Hour-1,23) as Start_Hour
            FROM Vinpac_Production.FSR_Hourly
            where line = :line
            and t_stamp>= dateadd(minute,60,:start_date)
            and t_stamp< dateadd(minute,60,:end_date)                 
        ) as hrly

    on hrly.t_stamp >= dateadd(hour, datediff(hour,0, jp.start_time)+1, 0) 
       AND hrly.t_stamp <  dateadd(hour, /* Add an hour instead of rounding down t_stamp */ datediff(hour,0, ISNULL(jp.finish_time,dateadd(hour, 60,jp.start_time)))+2, 0)

   
    full join (
            select 
                sum(erp_quantity) as 'Count'
                ,IIF(datepart(hour,man_t_stamp)+1=24,0,datepart(hour,man_t_stamp)+1) as Hour
                from vinpac_printapply.print_log
            WHERE

                    man_t_stamp >= dateadd(hour,-1, :start_date )
                AND man_t_stamp <  :end_date
                AND Line like :line
            group by datepart(hour,man_t_stamp),datepart(day,man_t_stamp),datepart(month,man_t_stamp),datepart(year,man_t_stamp)
    ) as pl
    on hrly.hour=pl.Hour   
	where hrly.hour is not null
    )
select 
    Hour
    ,min(duration) as duration
    ,sum(job_running) as total_running
    ,sum((job_running)*sch_job_rate/60.) as sch_bottles_per_minute
    ,count(job) as number_of_jobs
    ,min(timeHour) as t_stamp
    ,max(start_time) as start_time
    ,max(date_end) as setup_date
    ,IIF(sum(time_sch_setup)>0,sum(iif(job!=0,time_sch_setup,0)),0) as time_sch_setup
    ,min(CNT) as CNT
    ,min(TIME_FLTSTP) as TIME_FLTSTP 
    ,min(TIME_INFSTP) as TIME_INFSTP 
    ,min(TIME_OUTSTP) as TIME_OUTSTP
    ,min(TIME_SAFSTP) as TIME_SAFSTP
    ,min(TIME_SETUP) as TIME_SETUP
    ,min(TIME_OUTSTP) as TIME_OUTSTP
    ,min(TIME_RO) as TIME_RO
    ,min(TIME_MISCSTP) as TIME_MISCSTP
    ,min(TIME_USRSTP) as TIME_USRSTP
    ,min(TIME_BREAK) as TIME_BREAK
    ,min(TIME_STP) as TIME_STP
    ,min(Running) as Running 
    ,min(Total) as Total 
    ,max(Actual_Rate) as 'Actual Rate'
    ,max(Warehouse) as Warehouse
    ,convert(bit,min(convert(int,"24_7")))  as '24_7'
from jobs
    where hour is not null
    group by hour
    order by min(timeHour)
