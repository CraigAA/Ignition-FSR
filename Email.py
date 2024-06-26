body=['<hmtl> FSR Report Generated By Ignition <br><a href="https://vinangprod01.cellarmasters.local:8043/data/perspective/client/Production/FSR_Report/1">Interactive Ignition Page</a><br>']
date=system.date.format(system.date.addHours(system.date.now(),-72),'yyyy-MM-dd')
time=system.date.getHour24(system.date.now())

if time<8:
	shift=3
elif time>15:
	shift=2
else:
	shift=1
break1_dur=10
break2_dur=10

if shift==1:
	start=date+' 00:00:00'
	end=date+' 08:00:00'
	break1_dur=20
	break1=date+' 03:30:00'
	break2=date+' 06:00:00'
	break3=date+' 00:00:00'
elif shift==2:
	start=date+' 08:00:00'
	end=date+' 16:00:00'
	break2_dur=20
	break1=date+' 10:00:00'
	break2=date+' 12:30:00'
	break3=date+' 00:00:00'
else:
	start=date+' 16:00:00'
	end=date+' 23:59:59'
	break1_dur=20
	break1=date+' 19:30:00'
	break2=date+' 22:00:00'
	break3=date+' 00:00:00'

for line in ['8']:
	params={
		'start_date':start, 'end_date':end, 'line':line, 
		'break1_date':break1,'break2_date':break2,'break3_date':break3,
		'break1_duration':break1_dur,'break2_duration':break2_dur,'break3_duration':0}
	value=system.dataset.toPyDataSet(system.db.runNamedQuery('FSR/Hourly_Stoppages v8', params))
	index=['Line ' +line]
	headers=['header Hours']
	filler=['header Filler Bottles']
	warehouse=['header Warehouse Bottles']
	scheduled=['header Scheduled Bottles']
	sch_setup=['header Scheduled Setup Time']
	bpm=['header Filler BPM']
	sch_bpm=['header Avg Scheduled BPM']
	downtime_label=['']
	setups=['header Setup Recorded']
	breaks=['header Break Recorded']	
	stops=['header Stopped Recorded']	
	running=['header Running Recorded']	
	total=['header Total Time']
	runout=['header Runout']
	off=['header Off Time']
	test=[]
	
	params={'start_date':start, 'end_date':end, 'line':line}
	off_data=system.dataset.toPyDataSet(system.db.runNamedQuery('FSR/Off_Times', params))
	
	for row in range(0,len(value)):
		off.append(str(int(off_data.getValueAt(row,'Line '+line))))
	
	NewDict = [
	   {
	        colName: value
	        for colName, value in zip(value.columnNames, list(row))
	    }
	    for i, row in enumerate(value)
	]
	
	for i,row in enumerate(NewDict):
		index.append((i))
		row['Hour']=str(row['Hour']) + '-' + str(row['Hour']+1)
		headers.append(row['Hour'])
		filler.append(str(row['CNT']))
		warehouse.append(str(row['Warehouse']))
		if row['Actual Rate']!=None:
			bpm.append(str(int((round(row['Actual Rate'])))))
		else:
			bpm.append('0')
		sch_setup.append(str(int((round(row['time_sch_setup'])))))
		off_break=int(float(off[i+1]))-int(row['TIME_BREAK']) if int(float(off[i+1]))!=0 else 0
	
		sch_count=str(int(round(row['sch_bottles_per_minute']))*(int(round(row['Running']))+int(round(row['TIME_STP']))+int(round(row['TIME_SETUP'])-int((round(row['time_sch_setup'])))-int(float(off[i+1])))))
		if int(sch_count)<0:
			sch_count=str(int(round(row['sch_bottles_per_minute']))*(int(round(row['Running']))+int(round(row['TIME_STP']))+int(round(row['TIME_SETUP'])+int(round(row['TIME_BREAK']))-int((round(row['time_sch_setup'])))-int(float(off[i+1])))))
		
		if int(sch_count)<0:
			sch_count='0     '
		scheduled.append(sch_count)
		sch_bpm.append(str(int(round(row['sch_bottles_per_minute']))))
		downtime_label.append('')
		setups.append(str(int(round(row['TIME_SETUP']))))
		breaks.append(str(int(round(row['TIME_BREAK']))))
		stops.append(str(int(round(row['TIME_STP']))))
		running.append(str(int(round(row['Running']))))
		total.append(str(int(round(row['Total']))))
		runout.append(str(int(round(row['TIME_RO']))))
	####SUM#####	
	index.append('Sum')
	headers.append('Sum')	
	filler.append(str(sum([int(val) for val in filler[1:]])))
	warehouse.append(str(sum([int(val) for val in warehouse[1:]])))
	scheduled.append(str(sum([int(val) for val in scheduled[1:]])))
	bpm.append(str(sum([int(val) for val in bpm[1:]])))
	sch_bpm.append('')
	sch_setup.append(str(sum([int(val) for val in sch_setup[1:]])))
	downtime_label.append('')
	setups.append(str(sum([int(val) for val in setups[1:]])))
	breaks.append(str(sum([int(val) for val in breaks[1:]])))
	stops.append(str(sum([int(val) for val in stops[1:]])))
	running.append(str(sum([int(val) for val in running[1:]])))
	total.append(str(sum([int(val) for val in total[1:]])))
	runout.append(str(sum([int(val) for val in runout[1:]])))
	off.append(str(sum([int(float(val)) for val in off[1:]])))
	headers=['topStyle'+str(cell) for cell in headers] 
	
	data=system.dataset.toDataSet(index,[headers]+[scheduled]+[warehouse]+[sch_setup]+[downtime_label]+[setups]+[breaks]+[stops]+[running]+[total]+[runout]+[off]+[sch_bpm])	

	
	html= '<b> Line '+ line + ': </b><br>' + system.dataset.dataSetToHTML(False, data, 'FSR').replace('header ',"").replace("'>topStyle","', style='font-weight: bold'>") + '<br>'
	body.append(html.replace("border='1'","border='1', style='table-layout:fixed; width:1200px'").replace('left','center'))
	
	#Time Log
	params={'date':date, 'line':line, 'shift':shift}
	time_log=system.dataset.toPyDataSet(system.db.runNamedQuery('FSR/D365_Time_Log_raw', params))
	if len(time_log)!=0:		
		html= system.dataset.dataSetToHTML(False, time_log, 'Time Log').replace('>PRO0',", style='width:200px'>PRO0") + '<br>'
		html=html.replace('https://vinangprod01.cellarmasters.local:8043/data/perspective/client/Angaston_Production/Breakdown_Event/','<html><a href="https://vinangprod01.cellarmasters.local:8043/data/perspective/client/Angaston_Production/Breakdown_Event/" target="_blank" >Event = </a></html>')
		id_event=html.split('Angaston_Production/')
		for event in id_event:
			if event[54:58]!='ABLE':
				event_new=event.replace('Breakdown_Event/','Breakdown_Event/'+str(event[54:58].replace('</','')))
				html=html.replace(event,event_new)
		body.append(html.replace("border='1'","border='1', style='table-layout:fixed; width:1200px; white-space:pre-wrap; word-wrap:break-word'").replace('left','center').replace('right','center'))
recipients=['craig.atkinson@vinpac.com.au']

header = 'Ignition FSR for Shift: ' + start
#+system.date.format(system.date.now(),'dd/MM/yy')
body.append('<br>If you notice any issues with the above information, please contact engineering-team@vinpac.com.au')
# Send Email ##
system.net.sendEmail('Enter Email Congfiguration')
