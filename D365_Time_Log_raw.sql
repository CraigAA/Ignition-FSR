SELECT 
	prodid
	,description
	,cast(cast(round(hours*60.0,0) as int) as varchar(5)) as Duration
	,DATENAME(dw,:date)+ ' ' +convert(varchar,dateadd(second,starttime,:date)) as 'Start Time'
	,DATENAME(dw,:date)+ ' ' +convert(varchar,dateadd(second,endtime,:date)) as 'End Time'
	,concat(problemdescription,problemdetails,actiondescription,maintenanceName) as 'Combined Comments'
	
	FROM PROD.ProductionTimeLog
	WHERE lineid = 'L' + :line
	AND transdate = :date
	AND shiftid = 'SHIFT-' + :shift
	order by starttime
