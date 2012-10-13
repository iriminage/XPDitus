# kron : processes the eyeCubed 'events' table
# V1.0 April 2006 Mike Amoore
# ----------------------------------
# V1.01 March 2007 Don Chen
# control live iPackager 192.168.23.3, STOP and PLAY trigger
# ----------------------------------
# V1.02 July 2007 Mike Amoore
# prepare for testing

from datetime import datetime, timedelta
import os, random, time
import socket, sys, thread, urllib

os.environ['DJANGO_SETTINGS_MODULE'] = 'hodie.settings'
from hodie.iads.models import *

# Set the iPackager IP address (comment out whichever not in use)
iPackagerIP = "192.168.101.111"     #iPackager 1
#iPackagerIP = "192.168.101.112"     #iPackager 2

# --------------------------------------------------------------------
# function : db_save
# database save with locking to avoid MySQL 2013 error
def db_save(obj):
        db_lock.acquire()
        obj.save()
        db_lock.release()

# --------------------------------------------------------------------
# function : save_log
# make an entry in the log table
def save_log(type, severity, text):
    new_log = Log( log_date_time = datetime.now(),
                    log_type = type,
                    log_severity = severity,
                    log_text = text)
    db_save(new_log)
    

# --------------------------------------------------------------------
# function : urlopen_log(urlstr):
# openurl and save raw urlstr string to a file everyday
# added by Steve Ma @ 27 April 2010
def urlopen_log(urlstr):
    logfolder='raw_url_log'
    try:
      if not os.path.exists(logfolder): os.makedirs(logfolder)
      filename='%s/rawurl_%s.txt'%(logfolder,time.strftime('%d%b%Y',time.localtime()))
    
      open(filename,'a').write('%s\x09%s\n'%(time.ctime(),urlstr))
    except:
      save_log ('kron',3,'urlopen_log():'+str(sys.exc_info()[1])) #type maximum length=10
      
    return urllib.urlopen(urlstr)

# --------------------------------------------------------------------
# thread function : heartbeat
# ping the iPackager every hour to keep the connection alive
def thread_heartbeat():

    #print 'Heartbeat started'
    while 1:
        #url = "http://192.168.23.3/iPackagerWeb/Publish.asp?"   #target iPackager

        #create stop instruction for iPackager
        #url = "http://192.168.23.3/iPackagerWeb/Publish.asp?"   #target iPackager
        url = "http://" + iPackagerIP + "/iPackagerWeb/Publish.asp?"   #target iPackager
        url = url + "action=Stop"                               #action
        url = url + "&appname=XXX&iTVID=XXX"                    #trigger details
        url = url + "&Player=1"                                 #player 1 = broadcast
        url = url + "&Platform=1"                               #platform 1 = OpenTV


        start_time = datetime.now()
        #while 1:
        f = urlopen_log(url)
        s = f.read()
        print datetime.now(),'heartbeat got', s
        #if s == 'OK': break
        #print 'Heartbeat got OK after ', time.time() - start_time, 'seconds'                
        f.close()
        time.sleep (15 * 60)


# --------------------------------------------------------------------
# thread function : rules master
# start individual rules threads on kron restart
def start_rule_threads():

	# read list of active rules
	rule_list = Rule.objects.filter(active = True)
	# kick off a thread for each rule
	for rule in rule_list:
		#start each rule thread
		thread.start_new_thread(rule_thread,(rule,))    

        logmsg = 'Rules master processed %(num_rules)d rule(s)' % \
            {'num_rules': len(rule_list),}
        save_log ('kron', 3, logmsg)

# --------------------------------------------------------------------
# thread function : rules
# perform the defined function at the correct time
def rule_thread(rule):

	import datetime
	from dateutil.relativedelta import relativedelta, MO, TU, WE, TH, FR, SA, SU
	
	#import the code for the rule to be run
	rule_func = rule.func_name	
	#exec('import '+ rule_func)
	exec('from '+ rule_func + ' import *')

        logmsg = 'Rule thread started for: %(rule_name)s' % \
            {'rule_name': rule.name,}
        save_log ('kron', 3, logmsg)
        
        day_code = { 'Monday':MO, 'Tuesday':TU, 'Wednesday':WE, 'Thursday':TH, 'Friday':FR, 'Saturday':SA, 'Sunday':SU }

	# infinite loop
	while 1:

		#find next rule execution date/time
		
		now = datetime.datetime.now()
		time_now = now.time()
		day_now = now.date()	
		rule_day = rule.day_of_week		#only for WEEKLY
		rule_time = rule.time_of_day		#for all but EVERY
		rule_date = rule.day_of_month		#only for MONTHLY
		exec_dt = now
		exec_date = day_now
		exec_time = rule_time
		
		#rule for EVERY
		if rule.frequency == 'every':
			
			# add delay to time now to get execution time
			exec_time = now + datetime.timedelta( minutes=rule.minutes )
			exec_date = exec_time.date()
					
		#rule for DAILY
		if rule.frequency == 'daily':
		
			#check if it was earlier today -> jump ahead a day
			if rule_time < time_now:
				exec_date = day_now.replace( day= day_now.day+1 )
				
		#rule for WEEKLY
		if rule.frequency == 'weekly':

			#find date of next weekday specified by rule
			exec_date = day_now + relativedelta(weekday=day_code[rule_day])

			#check if it was earlier today -> jump ahead a week
			if exec_date == day_now and rule_time <= time_now:
				exec_date = day_now + relativedelta(days=+1, weekday=day_code[rule_day])				
											
		#rule for MONTHLY
		if rule.frequency == 'monthly':

			exec_date = day_now.replace( day= rule_date )

			#check if it was earlier today or earlier in the month -> jump ahead a month
			if (exec_date == day_now and rule_time <= time_now) or (exec_date < day_now):
				exec_date = day_now + relativedelta(months=+1)								

		#construct the datetime to execute
		exec_dt = now.replace( year= exec_date.year, 
					month= exec_date.month,
					day= exec_date.day,
					hour= exec_time.hour, 
					minute= exec_time.minute, 
					second= exec_time.second )
					
		#sleep until due
		print rule.frequency + ' exec dt =' + exec_dt.strftime("%d:%m:%y %I:%M:%S %p")
		sleep_til_due = exec_dt - datetime.datetime.now()        
		if sleep_til_due.seconds > 0:
			time.sleep(sleep_til_due.seconds)
		
		#perform the rule function
		try:
			eval(rule_func + '()')
			logmsg = 'Rule thread %(rule_name)s executed rule function %(rule_func)s' % \
			    {'rule_name': rule.name,'rule_func': rule_func,}
			save_log ('kron', 3, logmsg)
		except:
			logmsg = 'Rule thread %(rule_name)s failed on rule function %(rule_func)s' % \
			    {'rule_name': rule.name,'rule_func': rule_func,}
			save_log ('kron', 3, logmsg)

		#sleep for a minute to put you past this scheduled time
		if rule.frequency != 'every':time.sleep(60)
    
# --------------------------------------------------------------------
# thread function : timer
# return the message to the port at the time specified
def thread_timer(end_time, port, message, event):

    global thread_cancel_check

    print 'timer thread started for port ', port, ' end time ', end_time
    # sleep until just before event is due
    sleep_til_due = end_time - datetime.now()        
    sleep_til_due_secs = sleep_til_due.seconds - thread_cancel_check

    # check that the event has not already been triggered
    event_details = event.event_louth_signal_set.all()
    duration = event_details[0].signal_duration


    # creep up to scheduled time
    if sleep_til_due_secs > 0:
        time.sleep(sleep_til_due_secs)

    # if event has not already been triggered then ...
    print 'timer thread about to expire at ', datetime.now()
    print 'event triggered status = ', event_details[0].ec_event_triggered
    if event_details[0].ec_event_triggered == False:
        # open socket to main event thread  
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('192.168.194.16', port))

            # wait until event is due
            while 1:
                if datetime.now() >= end_time: 
                    break

            # send message
            s.send(message)
            #totalsent = 0
            #while totalsent < 7:
            #    sent = s.send(message[totalsent:])
            #    if sent == 0:
            #        raise RuntimeError, "socket connection broken"
            #    totalsent = totalsent + sent
            s.close()
            print 'timer sent message ', message, ' to port ', port
        except:
            #do nothing if thread is no longer listening
            s.close()
            print 'timer thread failed'
        
# --------------------------------------------------------------------            
# thread function : external trigger
def thread_external_trigger(inst):
    
    global current_event_list 
    global thread_cancel_check

    logmsg = 'Event %(id)d is a Louth external triggered event' % {'id': inst.id}
    save_log ('kron', 3, logmsg)
    
    #check that event is not past due 
    if inst.ec_event_date_time + timedelta(minutes = louth_trigger_timeout) > datetime.now():

        # find the detail record in Event_Louth_Signal
        event_details = inst.event_louth_signal_set.all()
        this_event_details = event_details[0]
        duration = this_event_details.signal_duration

        # find the iad and channel from the booking
        iappbooking = IappBooking.objects.get(id = this_event_details.id)
        iapp = iappbooking.iapp
        channel = iappbooking.chan_id
        logmsg = 'Event %(id)d is iad %(iad)s on channel %(chan)s at %(when)s duration %(dur)ds' % \
            {'id': inst.id, 'iad': iapp.name, 'chan': channel.name, \
             'when': iappbooking.scheduled_date_time.strftime("%d/%m/%y %I:%M:%S %p"), \
             'dur': duration }
        save_log ('kron', 3, logmsg)

        # get the event's allocated port number
        port = inst.ec_event_socket

        # find the right trigger file for the channel and iAd
        # first look for native applications on the channel (e.g. SKYbet)
        button_red = channel.button_red
        button_blue = channel.button_blue
        button_green = channel.button_green
        button_yellow = channel.button_yellow
        # update the button list from the iAd button details
	if iapp.key_colour == 'red': button_red = iapp
	if iapp.key_colour == 'blue': button_blue = iapp
	if iapp.key_colour == 'green': button_green = iapp
	if iapp.key_colour == 'yellow': button_yellow = iapp        
	# find the matching trigger
	trigger_list = Trigger_file.objects.filter(
		channel__exact=channel,
		button_red__exact=button_red,
		button_blue__exact=button_blue,
		button_green__exact=button_green,
		button_yellow__exact=button_yellow,)
	if len(trigger_list) == 0:
		logmsg = 'Event %(id)d cannot find a matching start trigger' % {'id': inst.id,}
		save_log ('kron', 1, logmsg)
        	return
	if len(trigger_list) > 1:
		logmsg = 'Event %(id)d found %(num)d matching start trigger(s)' % {'id': inst.id, 'num': len(trigger_list),}
		save_log ('kron', 2, logmsg)
	start_trigger = trigger_list[0]
                     
	#check to see if a 24x7 app is running on the channel (e.g. SKYbet) 
	if (channel.button_red == None and 
		channel.button_blue == None and 
		channel.button_green == None and 
		channel.button_yellow == None):
		background_app = False
	else:
		background_app = True

		# find the stop trigger file 
		if iapp.key_colour == 'red': button_red = None
		if iapp.key_colour == 'blue': button_blue = None
		if iapp.key_colour == 'green': button_green = None
		if iapp.key_colour == 'yellow': button_yellow = None        
		# find the matching trigger
		trigger_list = Trigger_file.objects.filter(
			button_red__exact=button_red,
			button_blue__exact=button_blue,
			button_green__exact=button_green,
			button_yellow__exact=button_yellow,)
		if len(trigger_list) == 0:
			logmsg = 'Event %(id)d cannot find a matching stop trigger' % {'id': inst.id,}
			save_log ('kron', 1, logmsg)
			return
		if len(trigger_list) > 1:
			logmsg = 'Event %(id)d found %(num)d matching stop trigger(s)' % {'id': inst.id, 'num': len(trigger_list),}
			save_log ('kron', 2, logmsg)
		stop_trigger = trigger_list[0]
        
        # start a timer thread for signal timeout       
        end_time = iappbooking.scheduled_date_time + timedelta(minutes = louth_trigger_timeout)
        thread.start_new_thread(thread_timer,(end_time, port, 'timeout', inst))

        # listen on the thread's socket (blocking)    
        thread_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #thread_socket.bind(('', port))
        #thread_socket.bind((socket.gethostname(), port))
        thread_socket.bind(('192.168.194.16', port))
        thread_socket.listen(1)
        (clientsocket, address) = thread_socket.accept()

        # connection has been made : get the message
        incoming = ""
        while len (incoming) < 7:
            chunk = clientsocket.recv (7 - len(incoming))
            print 'chunk = ', chunk, '(', len(chunk),')'
            if chunk == '':
                    raise RuntimeError, "socket connection broken"
            incoming = incoming + chunk
	clientsocket.close
	
        # print "louth string received", incoming, "(", len(incoming), ")"
        # thread times out before louth signal received...
        if incoming == 'timeout':
            logmsg = 'Event %(id)d timed out at %(when)s after %(dur)d minute(s)' % \
                 {'id': inst.id, \
                 'when': datetime.now().strftime("%d/%m/%y %I:%M:%S %p"), \
                 'dur': louth_trigger_timeout }
            save_log ('kron' , 2, logmsg)
            inst.ec_event_completed = True

        # schedule is updated so event thread must be cancelled...
        if incoming == 'cancelx':
            logmsg = 'Event %(id)d cancelled at %(when)s ' % \
                 {'id': inst.id, \
                 'when': datetime.now().strftime("%d/%m/%y %I:%M:%S %p"), \
                 }
            save_log ('kron' , 2, logmsg)
            inst.ec_event_cancelled = True
            inst.ec_event_completed = True

        # received a message from louth...
        if incoming == 'started':

            msg_rcv_time = time.time()
            print 'Start message received at', datetime.now()
            logmsg = "Event %(id)d received message '%(message)s' at %(when)s " % \
                 {'id': inst.id, \
                 'when': datetime.now().strftime("%d/%m/%y %I:%M:%S %p"), \
                 'message': chunk }
            save_log ('kron' , 3, logmsg)

            #create play instruction for iPackager
            url = "http://" + iPackagerIP + "/iPackagerWeb/Publish.asp?"   	#target iPackager
            url = url + "action=PartialPublishPlayEpisode"             		#action
            url = url + "&appname=" + start_trigger.app_id                   	#app_id
            url = url + "&iTVID=" + start_trigger.event_id                   	#iTV_id
            url = url + "&Player=1"                                 		#player 1 = broadcast
            url = url + "&Platform=1"                               		#platform 1 = OpenTV
	    url = url + "&File=http://" + iPackagerIP + "/Triggers/"		#start of trigger file name
	    url = url + channel.trigger_dir_name + "/XML/"+ "TriggerAppDef_" 	#channel trigger dir name
	    url = url + start_trigger.trigger_file_name + ".xml"		#trigger file name

            #send play instruction to iPackager - loop until get 'OK' back
            while 1:
                print 'Sent PLAY after ', time.time() - msg_rcv_time, 'seconds'
                f = urlopen_log(url)
                s = f.read()
                print 'Read return after ', time.time() - msg_rcv_time, 'seconds'               
                if s == 'OK': break
            delay = time.time() - msg_rcv_time
            print 'Got OK after ', delay, 'seconds'                
            logmsg = "OK received from iPackager after %(time)f second(s)" % \
                 {'time': delay }
            save_log ('kron' , 3, logmsg)
            logmsg = "Event %(id)d trigger enabled at %(when)s " % \
                 {'id': inst.id, \
                 'when': datetime.now().strftime("%d/%m/%y %I:%M:%S %p") }
            save_log ('kron' , 3, logmsg)

            # mark event as triggered
            this_event_details.ec_event_triggered = True
            this_event_details.signal_date_time = datetime.now()
            db_save(this_event_details)            

	    if background_app == True:
	    
		    #create stop instruction for iPackager
		    url = "http://" + iPackagerIP + "/iPackagerWeb/Publish.asp?"   	#target iPackager
		    url = url + "action=PartialPublishPlayEpisode"             		#action
		    url = url + "&appname=" + stop_trigger.app_id                   	#app_id
		    url = url + "&iTVID=" + stop_trigger.event_id                   	#iTV_id
		    url = url + "&Player=1"                                 		#player 1 = broadcast
		    url = url + "&Platform=1"                               		#platform 1 = OpenTV
		    url = url + "&File=http://" + iPackagerIP + "/Triggers/"		#start of trigger file name
		    url = url + channel.trigger_dir_name + "/XML/"+ "TriggerAppDef_" 	#channel trigger dir name
		    url = url + stop_trigger.trigger_file_name + ".xml"			#trigger file name
	    	    
	    else:

		    # need only stop the trigger playout
		    url = "http://" + iPackagerIP + "/iPackagerWeb/Publish.asp?"   	#target iPackager
		    url = url + "action=Stop"                               		#action
		    url = url + "&appname=" + start_trigger.app_id                   	#app_id
            	    url = url + "&iTVID=" + start_trigger.event_id                   	#iTV_id
		    url = url + "&Player=1"                                 		#player 1 = broadcast
		    url = url + "&Platform=1"                               		#platform 1 = OpenTV

            # wait out the duration of the ad
            time.sleep(duration)

            #send stop instruction to iPackager - loop until get 'OK' back
            while 1:
                f = urlopen_log(url)
                s = f.read()
                if s == 'OK': break
            logmsg = "Event %(id)d trigger disabled at %(when)s " % \
                 {'id': inst.id, \
                 'when': datetime.now().strftime("%d/%m/%y %I:%M:%S %p") }
            save_log ('kron' , 3, logmsg)

            f.close()

            #change event status
            inst.ec_event_completed = True
            logmsg = 'Event %(id)d completed at %(when)s' % \
                 {'id': inst.id, \
                 'when': datetime.now().strftime("%d/%m/%y %I:%M:%S %p") }
            save_log ('kron' , 3, logmsg)
            
    else:
        # event is past due
        logmsg = 'Event %(id)d is cancelled as it is past due' % {'id': inst.id}
        save_log ('kron', 2, logmsg)
        inst.ec_event_cancelled = True
        # db_save(inst)


    # event processing complete
    db_save(inst)
           
# --------------------------------------------------------------------            
# thread function : no trigger
#
# NB: no_trigger events start straight away when picked up (event_window=5, so 5 mins before) and sleep for 10 mins
# before trigger is stopped

def thread_no_trigger(inst):
    
    global current_event_list 
    global thread_cancel_check

    logmsg = 'Event %(id)d is a non-triggered event' % {'id': inst.id}
    save_log ('kron', 3, logmsg)
    
    #check that event is not past due 
    if inst.ec_event_date_time + timedelta(minutes = louth_trigger_timeout) > datetime.now():

        # find the detail record in Event_Louth_Signal
        event_details = inst.event_louth_signal_set.all()
        this_event_details = event_details[0]
        duration = this_event_details.signal_duration

        # find the iad and channel from the booking
        iappbooking = IappBooking.objects.get(id = this_event_details.id)
        iapp = iappbooking.iapp
        channel = iappbooking.chan_id
        logmsg = 'Event %(id)d is iad %(iad)s on channel %(chan)s at %(when)s duration %(dur)ds' % \
            {'id': inst.id, 'iad': iapp.name, 'chan': channel.name, \
             'when': iappbooking.scheduled_date_time.strftime("%d/%m/%y %I:%M:%S %p"), \
             'dur': duration }
        save_log ('kron', 3, logmsg)

        # get the event's allocated port number
        port = inst.ec_event_socket

        # find the right trigger file for the channel and iAd
        # first look for native applications on the channel (e.g. SKYbet)
        button_red = channel.button_red
        button_blue = channel.button_blue
        button_green = channel.button_green
        button_yellow = channel.button_yellow
        
        # update the button list from the iAd button details
	if iapp.key_colour == 'red': button_red = iapp
	if iapp.key_colour == 'blue': button_blue = iapp
	if iapp.key_colour == 'green': button_green = iapp
	if iapp.key_colour == 'yellow': button_yellow = iapp        

	# find the matching trigger
        # normal filter on button states did not work!! had to use 'if' instead of filter!! MA 280410
        trigger_list = []
        for trigger in Trigger_file.objects.filter(channel__exact=channel):
            if ((trigger.button_red == button_red) and
                (trigger.button_blue == button_blue) and
                (trigger.button_green == button_green) and
                (trigger.button_yellow == button_yellow)):
                    trigger_list.append(trigger)
	if len(trigger_list) == 0:
		logmsg = 'Event %(id)d cannot find a matching start trigger' % {'id': inst.id,}
		save_log ('kron', 1, logmsg)
		return
	if len(trigger_list) > 1:
		logmsg = 'Event %(id)d found %(num)d matching start trigger(s)' % {'id': inst.id, 'num': len(trigger_list),}
		save_log ('kron', 2, logmsg)
	start_trigger = trigger_list[0]

	#check to see if a 24x7 app is running on the channel (e.g. SKYbet) 
	if (channel.button_red == None and 
		channel.button_blue == None and 
		channel.button_green == None and 
		channel.button_yellow == None):
		background_app = False
	else:
		background_app = True

                # find the stop trigger file (only needed for background app channels)
                if iapp.key_colour == 'red': button_red = None
                if iapp.key_colour == 'blue': button_blue = None
                if iapp.key_colour == 'green': button_green = None
                if iapp.key_colour == 'yellow': button_yellow = None        
               
                # find the matching trigger
                # normal filter on button states did not work!! had to use 'if' instead of filter!! MA 280410
                stop_trigger_list = []
                for trigger in Trigger_file.objects.filter(channel__exact=channel):
                    if ((trigger.button_red == button_red) and
                        (trigger.button_blue == button_blue) and
                        (trigger.button_green == button_green) and
                        (trigger.button_yellow == button_yellow)):
                            stop_trigger_list.append(trigger)
                            
                if len(stop_trigger_list) == 0:
                        logmsg = 'Event %(id)d cannot find a matching stop trigger' % {'id': inst.id,}
                        save_log ('kron', 1, logmsg)
                        return
                if len(stop_trigger_list) > 1:
                        logmsg = 'Event %(id)d found %(num)d matching stop trigger(s)' % {'id': inst.id, 'num': len(stop_trigger_list),}
                        save_log ('kron', 2, logmsg)
                stop_trigger = stop_trigger_list[0]

	#create play instruction for iPackager
	url = "http://" + iPackagerIP + "/iPackagerWeb/Publish.asp?"   	#target iPackager
	url = url + "action=PartialPublishPlayEpisode"             		#action
	url = url + "&appname=" + start_trigger.app_id                   	#app_id
	url = url + "&iTVID=" + start_trigger.event_id                   	#iTV_id
	url = url + "&Player=1"                                 		#player 1 = broadcast
	url = url + "&Platform=1"                               		#platform 1 = OpenTV
	url = url + "&File=http://" + iPackagerIP + "/Triggers/"		#start of trigger file name
	url = url + channel.trigger_dir_name + "/XML/"+ "TriggerAppDef_" 	#channel trigger dir name
	url = url + start_trigger.trigger_file_name + ".xml"		#trigger file name

	#send play instruction to iPackager - loop until get 'OK' back
	while 1:
		f = urlopen_log(url)
		s = f.read()
		if s == 'OK': break
	logmsg = "OK received from iPackager" 
	save_log ('kron' , 3, logmsg)
	logmsg = "Event %(id)d trigger enabled at %(when)s " % \
		 {'id': inst.id, \
		 'when': datetime.now().strftime("%d/%m/%y %I:%M:%S %p") }
	save_log ('kron' , 3, logmsg)

	# mark event as triggered
	# this_event_details.ec_event_triggered = True
	# this_event_details.signal_date_time = datetime.now()
	# db_save(this_event_details)            

	if background_app == True:

		# create stop instruction for iPackager
		url = "http://" + iPackagerIP + "/iPackagerWeb/Publish.asp?"   	#target iPackager
		url = url + "action=PartialPublishPlayEpisode"             		#action
		url = url + "&appname=" + stop_trigger.app_id                   	#app_id
		url = url + "&iTVID=" + stop_trigger.event_id                   	#iTV_id
		url = url + "&Player=1"                                 		#player 1 = broadcast
		url = url + "&Platform=1"                               		#platform 1 = OpenTV
		url = url + "&File=http://" + iPackagerIP + "/Triggers/"		#start of trigger file name
		url = url + channel.trigger_dir_name + "/XML/"+ "TriggerAppDef_" 	#channel trigger dir name
		url = url + stop_trigger.trigger_file_name + ".xml"			#trigger file name

	else:

		# need only stop the trigger playout
		url = "http://" + iPackagerIP + "/iPackagerWeb/Publish.asp?"   	#target iPackager
		url = url + "action=Stop"                               		#action
		url = url + "&appname=" + start_trigger.app_id                   	#app_id
		url = url + "&iTVID=" + start_trigger.event_id                   	#iTV_id
		url = url + "&Player=1"                                 		#player 1 = broadcast
		url = url + "&Platform=1"                               		#platform 1 = OpenTV

	# wait out the duration of the ad window 

        # start a timer thread for signal timeout (5 minutes after scheduled time)       
        end_time = inst.ec_event_date_time + timedelta(minutes = 5)
        thread.start_new_thread(thread_timer,(end_time, port, 'timeout', inst))
        print 'no trigger timeout started for timeout at ', end_time, 'on port ', port

        # listen on the thread's socket (blocking)    
        print 'listening socket number = ', port
        thread_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        thread_socket.bind(('192.168.194.16', port))
        thread_socket.listen(5)
        (clientsocket, address) = thread_socket.accept()

        # connection has been made : get the message
        incoming = ""
        while len (incoming) < 7:
            chunk = clientsocket.recv (7 - len(incoming))
            print 'chunk = ', chunk, '(', len(chunk),')'
            if chunk == '':
                    raise RuntimeError, "socket connection broken"
            incoming = incoming + chunk
	clientsocket.close
	
        # end of fixed time window...
        if incoming == 'timeout':
            logmsg = 'Event %(id)d end of fixed time window at %(when)s' % \
                 {'id': inst.id, \
                 'when': datetime.now().strftime("%d/%m/%y %I:%M:%S %p")
                  }
            save_log ('kron' , 2, logmsg)
            inst.ec_event_completed = True

        # schedule is updated so event thread must be cancelled...
        if incoming == 'cancelx':
            logmsg = 'Event %(id)d cancelled at %(when)s ' % \
                 {'id': inst.id, \
                 'when': datetime.now().strftime("%d/%m/%y %I:%M:%S %p"), \
                 }
            save_log ('kron' , 2, logmsg)
            inst.ec_event_cancelled = True
            inst.ec_event_completed = True

	#send stop instruction to iPackager - loop until get 'OK' back
	while 1:
		f = urlopen_log(url)
		s = f.read()
		if s == 'OK': break
	logmsg = "Event %(id)d trigger disabled at %(when)s " % \
		 {'id': inst.id, \
		 'when': datetime.now().strftime("%d/%m/%y %I:%M:%S %p") }
	save_log ('kron' , 3, logmsg)

	f.close()

	#change event status
	inst.ec_event_completed = True
	logmsg = 'Event %(id)d completed at %(when)s' % \
		 {'id': inst.id, \
		 'when': datetime.now().strftime("%d/%m/%y %I:%M:%S %p") }
	save_log ('kron' , 3, logmsg)
            
    else:
        # event is past due
        logmsg = 'Event %(id)d is cancelled as it is past due' % {'id': inst.id}
        save_log ('kron', 2, logmsg)
        inst.ec_event_cancelled = True
        # db_save(inst)


    # event processing complete
    db_save(inst)
           

# --------------------------------------------------------------------
# main loop : kron

# operational values initialisation
update_interval = 10        # default update interval = 30 seconds
event_window = 5            # default look-ahead = how early to deal with events before they are scheduled
trigger_preroll = 0         # trigger fudge = 0 seconds
louth_trigger_timeout = 20   # trigger wait window = how long after scheduled time to wait
louth_trigger_preroll = 10   # trigger preroll window = how long before scheduled time to look for signal
thread_cancel_check = 2     # sleep up to 2 seconds from event

db_lock = thread.allocate_lock()
save_log ('kron', 1, 'Kron has been restarted')
logmsg = 'Kron runs every %(value1)d seconds and looks ahead %(value2)d minutes for new events' % {'value1': update_interval, 'value2': event_window}
save_log ('kron', 2, logmsg)
logmsg = 'Kron looks for triggers %(value)d minutes before the event is scheduled' % {'value': louth_trigger_preroll}
save_log ('kron', 2, logmsg)
logmsg = 'Kron looks for triggers %(value)d minutes after the event is scheduled' % {'value': louth_trigger_timeout}
save_log ('kron', 2, logmsg)

#thread.start_new_thread(thread_heartbeat,())    #start iPackager heartbeat thread
thread.start_new_thread(start_rule_threads,())    #start master rules thread

#clean up open events left after kron was shut down: reset to unprocessed

old_event_list = Ec_event.objects.filter(
    ec_event_processed = True,
    ec_event_completed = False,
    )
for event in old_event_list:
    event.ec_event_processed = False
    db_save(event)    

# infinite loop
while 1:

    # housekeeping starting each loop
    loop_start_time = datetime.now()

    # find new events
    new_event_list = []
    #processing_window = datetime.now() + timedelta(minutes = event_window) + timedelta(minutes = louth_trigger_preroll)
    processing_window = datetime.now() + timedelta(minutes = event_window) 
    new_event_list = Ec_event.objects.filter(
        ec_event_processed = False,
        ec_event_date_time__lt=processing_window,
        )

    # process each new event
    if len(new_event_list) <> 0:
        for this_inst in new_event_list:

            #mark the event as processed
            this_inst.ec_event_processed = True
            db_save(this_inst)
            
            # handle the event

	    # find a free socket for this event
	    active_event_list = Ec_event.objects.filter(
	        ec_event_processed = True,
	        ec_event_cancelled = False,
	        ec_event_completed = False,
	        )
	    port = 3000
	    while 1:
	        port_used = False
	        for active_event in active_event_list:
	            if active_event.ec_event_socket == port:
		        port_used = True
		        break
	        if port_used == True:
		    port = port + 1
	        else:
		    if port_used == False:
		        break

	    # save the socket number for the event
	    logmsg = 'Event %(id)d has been allocated port %(port)d' % {'id': this_inst.id, 'port': port}
	    print logmsg
	    save_log ('kron', 3, logmsg)
	    this_inst.ec_event_socket = port
	    db_save(this_inst)

            if this_inst.ec_event_type == 'louth': # external trigger from louth
                # spawn a thread to handle the louth trigger event
                thread.start_new_thread(thread_external_trigger,(this_inst,))

            else:
	        if this_inst.ec_event_type == 'no_trigger': # no trigger available
                    # spawn a thread to handle the no trigger event
                    thread.start_new_thread(thread_no_trigger,(this_inst,))

                else:
                    logmsg = 'Event %(id)d has an unidentified event type' % {'id': this_inst.id}
                    save_log ('kron', 1, logmsg)
                    
    # sleep until next loop is due
    loop_end_time = datetime.now()
    # print datetime.now() 
    loop_duration = loop_end_time - loop_start_time
    loop_duration_secs = loop_duration.seconds
    if update_interval > loop_duration_secs:
        time.sleep(update_interval - loop_duration_secs)    
    
    





