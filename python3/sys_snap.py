#!/bin/python3

#use this executable to run /tools/python/virtual/dba/bin/python

##########################
##
##
##
##
##################################


###########################
##
## Import
###
#####################################

import os, re, sys, getopt, operator, time, cx_Oracle, getpass
from functools import reduce
sys.path.append(os.path.abspath('bin'))
import orautility


##############################
##
## Functions
##
#######################################


class color:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

##
## usage
##
def usage_exit(message):
    print(message)
    print("Usage:")
    print(os.path.abspath( __file__ ), '\n \
    Options: \n\n \
        [ -c CONNECTION_STRING ] \n \
             Connection String Format:  SID:Hostname[:User:Password] \n\n \
        [ -f SNAPSHOT[:LINES],... ] \n \
             Snapshot Options: SESS \n \
                               GSESS \n \
                               STAT \n \
                               EVENT \n \
                               FILE_IO \n \
                               TEMP \n \
                               UNDO \n \
                               SGA \n \
                               PGA \n \
                               METRIC \n \
                               LATCH \n \n')

    sys.exit(2)


def max_length(a, b):
    c = len(str(a))
    d = len(str(b))
    if c > d:       return a
    else:           return b


##
## main
##
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:s:f:")
    except getopt.GetoptError as err:
        print(str(err))
        usage_exit('getopt error:')

    connect    = None
    format     = None
    ORACLE_SID = None

    for o, a in opts:
        if o in ('-c'):
            connect = a
        elif o in ('-f'):
            format = a
        elif o in ('-s'):
            ORACLE_SID = a
        else:
            assert False, "unhandled option"

        
    if format is None:
        format = 'METRIC,TEMP,UNDO,STAT,EVENT,FILE_IO,SESS:15'

    conn = orautility.createOraConnection(connect)

    my_snap = System_Snap(conn, format)

    while 1 == 1:
        try:
            my_snap.create_snapshot()
        except KeyboardInterrupt:
            print('Exiting...')
            sys.exit(0)

        
    

   
class System_Snap:
 
    def __init__(self, 
                 conn, 
                 display_items):
    
    
        self.delimiter                 = ' '
        self.column_format_01           = '{:<40} {:<2}'

        self.sleep_time                = 5
        self.db                        = conn
        self.sys                       = {}
        self.display_items             = []

        for i in display_items.upper().split(','):
            j = i.split(':')
            self.display_items.append( j[0] )

            if j[0] == 'GSESS':
                if len(j) > 1:       self.print_global_sess_lines = int( j[1] )
                else:                self.print_global_sess_lines = 5
            if j[0] == 'SESS':
                if len(j) > 1:       self.print_sess_lines = int( j[1] )
                else:                self.print_sess_lines = 5
            if j[0] == 'SEGMENT_STAT':
                if len(j) > 1:       self.print_stat_lines = int( j[1] )
                else:                self.print_stat_lines = 5
            if j[0] == 'STAT':
                if len(j) > 1:       self.print_stat_lines = int( j[1] )
                else:                self.print_stat_lines = 5
            if j[0] == 'TEMP':
                if len(j) > 1:       self.print_temp_usage_lines = int( j[1] )
                else:                self.print_temp_usage_lines = 5
            if j[0] == 'EVENT':
                if len(j) > 1:       self.print_event_lines = int( j[1] )
                else:                self.print_event_lines = 5
            if j[0] == 'UNDO':
                if len(j) > 1:       self.print_undo_usage_lines = int( j[1] )
                else:                self.print_undo_usage_lines = 5
            if j[0] == 'LATCH':
                if len(j) > 1:       self.print_latch_lines = int( j[1] )
                else:                self.print_latch_lines = 5
            if j[0] == 'FILE_IO':
                if len(j) > 1:       self.print_file_io_lines = int( j[1] )
                else:                self.print_file_io_lines = 5


    
    def create_snapshot(self):

        self.snapshot_switch = { 'STAT':   self.get_stats_snapshot,
                                 'SEGMENT_STAT':  self.get_segment_stat_snapshot,
                                 'EVENT':  self.get_events_snapshot,
                                 'GSESS':  self.get_global_sess_snapshot,
                                 'SESS':   self.get_sess_snapshot,
                                 'METRIC': self.get_sys_metrics_snapshot,
                                 'TEMP':   self.get_temp_usage,
                                 'UNDO':   self.get_undo_usage,
                                 'SGA':    self.get_sgainfo,
                                 'PGA':    self.get_pgastat,
                                 'LATCH':  self.get_latch_snapshot,
                                 'FILE_IO':  self.get_file_io_snapshot,
                               }

        self.print_switch = {    'STAT':   self.print_stats,
                                 'SEGMENT_STAT':  self.print_segment_stats,
                                 'EVENT':  self.print_events,
                                 'GSESS':  self.print_global_sessions,
                                 'SESS':   self.print_sessions,
                                 'METRIC': self.print_sys_metrics,
                                 'TEMP':   self.print_temp_usage,
                                 'UNDO':   self.print_undo_usage,
                                 'SGA':    self.print_sgainfo,
                                 'PGA':    self.print_pgastat,
                                 'LATCH':  self.print_latches,
                                 'FILE_IO':  self.print_file_io,
                            }


        for i in self.display_items:
            self.snapshot_switch[i](1)
    
        time.sleep(self.sleep_time)

        for i in self.display_items:
            self.snapshot_switch[i](2)

        self.get_db_info()
 
        os.system('clear')
    
        self.print_db_info()             
        for i in self.display_items:
            self.print_switch[i]()




    def print_latches(self):

        print_lines = self.print_latch_lines
        s           = self.sys
        line_format = '{:<30} {:<10s} {:<10} {:<10}'

        if len( s['latch']['sorted_delta']) < print_lines:
            start = 0
        else:
            start = len( s['latch']['sorted_delta']) - print_lines

        for i in range(start, len( s['latch']['sorted_delta']) ):
            name = s['latch']['sorted_delta'][i][0]
            if i == start:
                print(color.BOLD + '\nLatches ' + self.delimiter + color.END )
                print(line_format.format('Latch', 'Gets', 'Misses', 'Sleeps'))

            print(line_format.format(name, s['latch']['run_data'][name]['delta']['get'] , s['latch']['run_data'][name]['delta']['miss'], s['latch']['run_data'][name]['delta']['sleep'] ))


        line_format = '{:<30} {:<40} {:<10} {:<10} {:<10}'

        if len( s['latch_miss']['sorted_delta']) < print_lines:
            start = 0
        else:
            start = len( s['latch_miss']['sorted_delta']) - print_lines

        for i in range(start, len( s['latch_miss']['sorted_delta']) ):
            name = s['latch_miss']['sorted_delta'][i][0]
            if i == start:
                print('\nLatch Misses ' + self.delimiter)
                print(line_format.format('Latch', 'Location', 'NW Fail', 'Sleeps', 'Wtr Sleeps'))

            print(line_format.format( s['latch_miss']['run_data'][name]['run_02']['parent_name'], s['latch_miss']['run_data'][name]['run_02']['location'],   
                                  s['latch_miss']['run_data'][name]['delta']['nwfail_count'] , s['latch_miss']['run_data'][name]['delta']['sleep_count'], 
                                  s['latch_miss']['run_data'][name]['delta']['wtr_slp_count'] ))


    def print_events(self):

        print_lines = self.print_event_lines
        s           = self.sys

        if len( s['event']['delta']) < print_lines:
            start = 0 
        else:   
            start = len( s['event']['delta']) - print_lines
    
        for i in range(start, len( s['event']['delta']) ):
            if i == start:
                print(color.BOLD + '\nEvents ' + self.delimiter + color.END )
                print(color.BOLD + '{:<50} {:>15} {:>20s}'.format('Event', 'Delta', 'Rate') + color.END )
    
            print('{:<50s} {:>15n} {:>20s}'.format(s['event']['delta'][i][0], round(s['event']['delta'][i][1]), str(round(s['event']['delta'][i][1]/self.sleep_time)) + '/Sec' ))
    

    def print_file_io(self):

        print_lines = self.print_file_io_lines
        s           = self.sys
        line_format = '{:<45s} {:>15n} {:>15n} {:>15n} {:>15n} {:>15n} {:>15n} {:>15n} {:>15n}'
        head_format = '{:<45s} {:>15s} {:>15s} {:>15s} {:>15s} {:>15s} {:>15s} {:>15s} {:>15s}'

        if len( s['file_io']['sorted_delta']) < print_lines:
            start = 0
        else:
            start = len( s['file_io']['sorted_delta']) - print_lines

        for i in range(start, len( s['file_io']['sorted_delta']) ):
            name = s['file_io']['sorted_delta'][i][0]
            if i == start:
                print( color.BOLD + '\nFile I/O ' + self.delimiter + color.END )
                print( color.BOLD + head_format.format('File', 'Phy Rds', 'Phy Wrts', 'Phy Blk Rd', 'Phy Blk Wrt', 'Sgl Blk Rds', 'Rd Tm', 'Wrt Tm', 'Sgl Blk Rd Tim' ) + color.END )

            print(line_format.format(name, s['file_io']['run_data'][name]['delta']['phyrds'] , 
                                       s['file_io']['run_data'][name]['delta']['phywrts'], 
                                       s['file_io']['run_data'][name]['delta']['phyblkrd'] ,
                                       s['file_io']['run_data'][name]['delta']['phyblkwrt'] ,
                                       s['file_io']['run_data'][name]['delta']['singleblkrds'] ,
                                       s['file_io']['run_data'][name]['delta']['readtim'] ,
                                       s['file_io']['run_data'][name]['delta']['writetim'] ,
                                       s['file_io']['run_data'][name]['delta']['singleblkrdtim'] ))





    def print_stats(self):

        s           = self.sys
        print_lines = self.print_stat_lines


        if len( s['stat']['delta']) < print_lines:
            start = 0
        else:
            start = len( s['stat']['delta']) - print_lines 
    
        for i in range(start, len( s['stat']['delta']) ):
            if i == start:
                print(color.BOLD + '\nStatistics ' + self.delimiter + color.END )
                print(color.BOLD + '{:<50s} {:>15s} {:>20s}'.format('Statistic', 'Delta', 'Rate') + color.END )
    
            print('{:<50s} {:>15n} {:>20s}'.format(s['stat']['delta'][i][0], s['stat']['delta'][i][1], str(round(s['stat']['delta'][i][1]/self.sleep_time)) + '/Sec' ))
    

    def print_segment_stats(self):

        s           = self.sys
        print_lines = self.print_stat_lines


        if len( s['segment_stat']['delta']) < print_lines:
            start = 0
        else:
            start = len( s['segment_stat']['delta']) - print_lines

        for i in range(start, len( s['segment_stat']['delta']) ):
            if i == start:
                print('color.BOLD + \nSegment Statistics ' + self.delimiter + color.END )
                print('{:<80s} {:>15s} {:>20s}'.format('Segment Statistic', 'Delta', 'Rate'))

            print('{:<80s} {:>15n} {:>20s}'.format(s['segment_stat']['delta'][i][0], round(s['segment_stat']['delta'][i][1]), str(round(s['segment_stat']['delta'][i][1]/self.sleep_time)) +  '/Sec' ))




    def print_sys_metrics(self):
        line = ''
        s = self.sys
    
        for i in range(0, len( s['metric'] ) ):
            s['metric'][i]['name'] = s['metric'][i]['name'].replace(' Per ', '/') 
            if i == 0:
                print(color.BOLD + '\nSys Metrics ' + self.delimiter + color.END )
    
            line = line + '{:<35s} {:>15n} {:>15s}'.format( s['metric'][i]['name'], round(s['metric'][i]['value']), ' ' )
    
            if (i+1)% 3  == 0:
                print(line)
                line = ''
    
            if i == len( s['metric'] ) -1 :
                if (i+1)%3 > 0 :
                    print(line)
    

    def print_global_sessions(self):

        print_lines    = self.print_global_sess_lines
        s              = self.sys
        line_format    = '{:<8} {:<17s} {:<20s} {:<15s} {:<43s} {:>8n} {:>12n} {:>12n} {:>12n} {:>12n} {:>12n} {:>10s} {:>8s} {:>8s}'
        head_format    = '{:<8} {:<17s} {:<20s} {:<15s} {:<43s} {:>8s} {:>12s} {:>12s} {:>12s} {:>12s} {:>12s} {:>10s} {:>8s} {:>8s}'
        total_sessions = len(s['glob_sess'])


        if total_sessions > print_lines and print_lines > 0:
            start = total_sessions - print_lines
        else:
            start = 0

        for i in range(start, total_sessions ):

            if i == start:
                print(color.BOLD + '\nTop Global Sessions (' + str(total_sessions) + ')' + self.delimiter + color.END )
                line = color.BOLD + head_format.format( 'Instance', ' SID,Serial', 'Username', 'SQL ID', 'Event', 'ET',
                                      'Blk Gets', 'Cons Gets', 'Phy Rds', 'Blk Chgs',
                                      'Cons Chgs', 'OS PID', 'Blocker', 'QC SID' ) + color.END
                print(line)

            line = line_format.format(
                                   s['glob_sess'][i]['inst_id'],
                                   s['glob_sess'][i]['sid'],
                                   s['glob_sess'][i]['username'][:19],
                                   s['glob_sess'][i]['sql_id'],
                                   s['glob_sess'][i]['event'],
                                   round(s['glob_sess'][i]['last_call_et']),
                                   round(s['glob_sess'][i]['block_gets']),
                                   round(s['glob_sess'][i]['cons_gets']),
                                   round(s['glob_sess'][i]['phy_reads']),
                                   round(s['glob_sess'][i]['blk_changes']),
                                   round(s['glob_sess'][i]['cons_changes']),
                                   str(s['glob_sess'][i]['os_pid']),
                                   str(s['glob_sess'][i]['blocking_sid']),
                                   str(s['glob_sess'][i]['qc_sid']) )
            print(line)


    def print_sessions(self):

        print_lines    = self.print_sess_lines        
        s              = self.sys
        head_format    = '{:<12s} {:<20s} {:<15s} {:<43s} {:>8s} {:>12s} {:>12s} {:>12s} {:>12s} {:>12s} {:>10s} {:>8s} {:>8s}'
        line_format    = '{:<12s} {:<20s} {:<15s} {:<43s} {:>8n} {:>12n} {:>12n} {:>12n} {:>12n} {:>12n} {:>10s} {:>8n} {:>8s}'
        total_sessions = len(s['sess'])


        if total_sessions > print_lines and print_lines > 0:
            start = total_sessions - print_lines 
        else:
            start = 0
   
        for i in range(start, total_sessions ):
    
            if i == start:
                print(color.BOLD + '\nTop Sessions (' + str(total_sessions) + ')' + self.delimiter + color.END )
                line = color.BOLD + head_format.format('SID,Serial', 'Username', 'SQL ID', 'Event', 'ET', 
                                      'Blk Gets', 'Cons Gets', 'Phy Rds', 'Blk Chgs', 
                                      'Cons Chgs', 'OS PID', 'Blocker', 'QC SID' ) + color.END
                print(line)
    
            line = line_format.format( 
                                   s['sess'][i]['sid'],  
                                   s['sess'][i]['username'][:19],  
                                   s['sess'][i]['sql_id'], 
                                   s['sess'][i]['event'],  
                                   round(s['sess'][i]['last_call_et']), 
                                   round(s['sess'][i]['block_gets']), 
                                   round(s['sess'][i]['cons_gets']), 
                                   round(s['sess'][i]['phy_reads']), 
                                   round(s['sess'][i]['blk_changes']), 
                                   round(s['sess'][i]['cons_changes']), 
                                   s['sess'][i]['os_pid'], 
                                   round(s['sess'][i]['blocking_sid']),
                                   s['sess'][i]['qc_sid'] )
            print(line)
    
    

    def print_db_info(self):

        s           = self.sys
        line_format = self.column_format_01
        columns     = 4


        field_names = { 'instance_name':                     'Instance',
                        'host_name':                         'Host',
                        'version':                           'Version',
                        'startup_time':                      'Startup',
                        'status':                            'Inst Status',
                        'database_status':                   'DB Status',
                        'sysdate':                           'Date',
                        'dbid':                              'DBID',
                        'name':                              'DB Name',
                        'checkpoint_change#':                'Chkpt Chg#',
                        'archive_change#':                   'Arch Chg#',
                        'controlfile_sequence#':             'Ctl Seq#',
                        'open_mode':                         'DB Op Mode',
                        'current_scn':                       'Cur SCN',
                      }
   
        s['db_inst']['host_name'] = s['db_inst']['host_name'].replace('.hq.navteq.com', '')


        i            = 0
        line         = ''
        stat_format  = '{:<' + str( len( reduce(max_length, list(field_names.values())) )+2) + 's} {:<10s}'
        print_fields = ['name', 'instance_name', 'host_name', 'version',
                        'sysdate', 'startup_time', 'status', 'database_status',
                        'current_scn', 'controlfile_sequence#', 'checkpoint_change#', 'archive_change#']

        for j in print_fields:
            if i == 0:
                print(color.BOLD + '\nDB, Instance ' + self.delimiter + color.END )

            if j is not None:
                stat = stat_format.format( field_names[j] + ': ' , str(s['db_inst'][j]) )
            else:
                stat = ' '

            line = line + line_format.format( stat, ' ' )


            if (i+1)% columns  == 0:
                print(line)
                line = ''

            if i == len( print_fields ) -1 :
                if (i+1)%columns > 0 :
                    print(line)

            i = i + 1



    def print_temp_usage(self):

        print_lines    = self.print_temp_usage_lines
        s              = self.sys
        head_format    = '{:<15s} {:<20s} {:<15s} {:<15s} {:<15s} {:<10s} {:>10s}'
        line_format    = '{:<15s} {:<20s} {:<15s} {:<15s} {:<15s} {:<10s} {:>10n}'
        total_records = len(s['temp_usage'])


        if total_records > print_lines and print_lines > 0:
            start = total_records - print_lines
        else:
            start = 0

        for i in range(start, total_records ):

            if i == start:
                print(color.BOLD + '\nTemp Usage' + self.delimiter + color.END )
                line = color.BOLD + head_format.format('SID,Serial', 'Username', 'SQL ID', 'Contents', \
                                      'Status', 'Tablespace', 'MB Used' ) + color.END
                print(line)

            line = line_format.format( 
                                   s['temp_usage'][i]['sid_serial'],  
                                   s['temp_usage'][i]['username'],  
                                   s['temp_usage'][i]['sql_id'],  
                                   s['temp_usage'][i]['contents'],  
                                   s['temp_usage'][i]['status'],  
                                   s['temp_usage'][i]['tbsp'],  
                                   round(s['temp_usage'][i]['mb_used']),  
                                 )
            print(line)


    def print_undo_usage(self):

        print_lines    = self.print_undo_usage_lines
        s              = self.sys
        line_format    = '{:<15s} {:<20s} {:<15s} {:>15s} {:>15s}'
        total_records = len(s['undo_usage'])


        if total_records > print_lines and print_lines > 0:
            start = total_records - print_lines
        else:
            start = 0

        for i in range(start, total_records ):

            if i == start:
                print(color.BOLD + '\nUndo Usage' + self.delimiter + color.END )
                line = color.BOLD + line_format.format('SID,Serial', 'Username', 'SQL ID', 'KB Used', 'Blks Used' ) + color.END
                print(line)

            line = line_format.format(
                                   str(s['undo_usage'][i]['sid_serial']),
                                   s['undo_usage'][i]['username'],
                                   str(s['undo_usage'][i]['sql_id']),
                                   str(s['undo_usage'][i]['kb_used']),
                                   str(s['undo_usage'][i]['blk_used']),
                                 )
            print(line)



    def print_sgainfo(self):

        s             = self.sys
        line_format   = '{:<30s} {:>20s} {:>20} {:>20s} {:>20s} {:>10s} {:>15s} {:>10s} {:>25s} {:>20s}'
        start         = 0   
        total_records = len(s['sgainfo'])

        fields = 'component, current_size, min_size, max_size, user_specified_size, oper_count, last_oper_type, last_oper_mode, last_oper_time, granule_size'

        for i in range(start, total_records):
            if i == start:
                print(color.BOLD + '\nSGA ' + self.delimiter + color.END )
                line = line_format.format( 'Component', 'Cur Size', 'Min Size', 'Max Size', 'Spec Size', 'Op Cnt', 'Lst Op Typ', 'Lst Op Md', 'Lst Op Tm', 'Gran Sz' )
                print(line)
            line = line_format.format(  str(s['sgainfo'][i][0]), str(s['sgainfo'][i][1]), str(s['sgainfo'][i][2]), str(s['sgainfo'][i][3]), str(s['sgainfo'][i][4]), str(s['sgainfo'][i][5]), 
                                    str(s['sgainfo'][i][6]), str(s['sgainfo'][i][7]), str(s['sgainfo'][i][8]), str(s['sgainfo'][i][9]) )
            print(line)


    def print_pgastat(self):

        s             = self.sys
        line_format   = '{:<40s} {:>30s}'
        start         = 0
        total_records = len(s['pgastat'])

        for i in range(start, total_records):
            if i == start:
                print(color.BOLD + '\nPGA ' + self.delimiter + color.END )

            if s['pgastat'][i][2] is None: 
                unit =  str( s['pgastat'][i][1] )
            else:
                if s['pgastat'][i][2] == 'bytes':
                    unit = str( int(s['pgastat'][i][1]/1024/1024) ) + ' MB'
                else:
                    unit =  str( s['pgastat'][i][1] ) + ' ' + s['pgastat'][i][2]
                
            line = line_format.format( s['pgastat'][i][0], unit )
            print(line)


    def get_pgastat(self, run):

        if run == 1:
            pass
        elif run == 2:
            cursor = self.db.cursor()
            sql_stmt = "select name, value, unit from v$pgastat order by 2"
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()
            self.sys['pgastat'] = rows


    def get_sgainfo(self, run):
    
        if run == 1:
            pass
        elif run == 2:
            cursor = self.db.cursor()

            fields = 'component, current_size, min_size, max_size, user_specified_size, oper_count, last_oper_type, last_oper_mode, last_oper_time, granule_size'
            sql_stmt = "select " + fields + "  from v$sga_dynamic_components where current_size > 0"

            cursor.execute(sql_stmt)
            rows = cursor.fetchall()

            self.sys['sgainfo'] = rows



    def get_undo_usage(self, run):

        if run == 1:
            pass

        elif run == 2:
            cursor = self.db.cursor()
            sql_stmt = "select                                                                     \
                               '(' || b.sid || ',' || b.serial# || ')' sid_serial,                 \
                               b.username, b.sql_id, (c.value*a.used_ublk)/1024 undo_kbytes,       \
                               a.used_ublk                                                         \
                       from v$transaction a, v$session b, v$parameter c                            \
                       where a.ses_addr = b.saddr and c.name = 'db_block_size'                     \
                       order by 4"

            cursor.execute(sql_stmt)
            rows = cursor.fetchall()

            r = []
            for i in range(0, len(rows)):
                r.append({})
                r[i]['sid_serial'] = rows[i][0]
                r[i]['username']   = rows[i][1]
                r[i]['sql_id']     = rows[i][2]
                r[i]['kb_used']    = rows[i][3]
                r[i]['blk_used']   = rows[i][4]

            self.sys['undo_usage'] = r

    def get_temp_usage(self, run):
        
        if run == 1:
            pass

        elif run == 2: 
            cursor = self.db.cursor() 
            sql_stmt = "select '(' || a.sid || ',' || a.serial# || ')' sid_serial, a.username, round( sum( ((b.blocks*p.value)/1024/1024)),2) size_mb, \
                            a.sql_id, b.contents, a.status, b.tablespace tablespace \
                        from v$session a, v$sort_usage b, v$parameter p \
                        where p.name='db_block_size' and a.saddr = b.session_addr  \
                        group by b.tablespace, a.sid , a.serial#, a.username, a.sql_id, b.contents, a.status \
                        order by 3 "

            cursor.execute(sql_stmt)
            rows = cursor.fetchall()

            r = []
            for i in range(0, len(rows)):
                r.append({})
                r[i]['sid_serial'] = rows[i][0]
                r[i]['username']   = rows[i][1]
                r[i]['mb_used']    = rows[i][2]
                r[i]['sql_id']     = rows[i][3]
                r[i]['contents']   = rows[i][4]
                r[i]['status']     = rows[i][5]
                r[i]['tbsp']       = rows[i][6]
             
            self.sys['temp_usage'] = r
 

    def get_db_info(self):
   
        cursor             = self.db.cursor()
        self.sys['db_inst']     = {}

        fields = 'instance_name, host_name, version, startup_time, status, database_status, sysdate'
        sql_stmt = "select " + fields + "  from v$instance"
        cursor.execute(sql_stmt)
        rows = cursor.fetchall()
       
        field_list = fields.split(',')
        r = {}
        if len(rows) > 0:
            for i in range(0, len(field_list) ):
                r[ field_list[i].strip() ] = rows[0][i]


        fields = 'dbid, name, checkpoint_change#, archive_change#, controlfile_sequence#, open_mode, current_scn '
        sql_stmt = "select " + fields + "  from v$database"
        cursor.execute(sql_stmt)
        rows = cursor.fetchall()

        field_list = fields.split(',')
        if len(rows) > 0:
            for i in range(0, len(field_list) ):
                r[ field_list[i].strip() ] = rows[0][i]

        self.sys['db_inst'] = r


    def get_global_sess_snapshot(self, run):

        if run == 1:
            pass

        elif run == 2:
            cursor               = self.db.cursor()
            self.sys['glob_sess']     = []

            sql_stmt = "select  s.state, \
                        s.sid || ',' || s.serial# sid , \
                        s.username  username, \
                        case when s.state != 'WAITING' \
                             then 'On CPU (Prev: ' || case when length(s.event) > 25 \
                                                      then  rpad(s.event, 25, ' ') || '...)' \
                                                      else s.event || ')' end \
                             else  rpad(s.event || '  (' || lower(s.wait_class) || ')' , 37, ' ') || \
                        case when length(s.event || s.wait_class ) > 35 then '...' else NULL end end as event, \
                        nvl(s.p1text, 'p1') || ': ' || s.p1 p1, \
                        nvl(s.p2text, 'p2') || ': ' || s.p2 p2, \
                        nvl(s.p3text, 'p3') || ': ' || s.p3 p3 , \
                        nvl(s.blocking_session, 0) blocking_session, \
                        s.seconds_in_wait seconds, nvl(s.sql_id, '--') sql_id, \
                        s.last_call_et, \
                        io.block_gets, io.consistent_gets, io.physical_reads, io.block_changes, io.consistent_gets, \
                        p.spid, \
                        (select distinct nvl(to_char(px.qcsid), '--' ) from gv$px_session px where px.inst_id = s.inst_id and px.sid = s.sid),  \
                        s.inst_id  \
                        from gv$session s, gv$sess_io io, gv$process p \
                        where s.inst_id = p.inst_id and s.inst_id = io.inst_id   \
                        and p.inst_id = io.inst_id \
                        and s.sid = io.sid and  s.paddr = p.addr and s.username is not null \
                        and s.event not like '%rdbms ipc message%' and s.event not like '%message from client%' \
                        and s.status = 'ACTIVE' \
                        order by s.last_call_et, s.sid "

            cursor.execute(sql_stmt)
            rows = cursor.fetchall()

            r = []
            for i in range(0, len(rows)):
                r.append([])
                r[i] = {}
                r[i]['state']           = rows[i][0]
                r[i]['sid']             = rows[i][1]
                r[i]['username']        = rows[i][2]
                r[i]['event']           = rows[i][3]
                r[i]['p1']              = rows[i][4]
                r[i]['p2']              = rows[i][5]
                r[i]['p3']              = rows[i][6]
                r[i]['blocking_sid']    = rows[i][7]
                r[i]['sec_wait']        = rows[i][8]
                r[i]['sql_id']          = rows[i][9]
                r[i]['last_call_et']    = rows[i][10]
                r[i]['block_gets']      = rows[i][11]
                r[i]['cons_gets']       = rows[i][12]
                r[i]['phy_reads']       = rows[i][13]
                r[i]['blk_changes']     = rows[i][14]
                r[i]['cons_changes']    = rows[i][15]
                r[i]['os_pid']          = rows[i][16]
                r[i]['qc_sid']          = rows[i][17]
                r[i]['inst_id']         = rows[i][18]

            self.sys['glob_sess'] = r

 
    def get_sess_snapshot(self, run):

        if run == 1:
            pass

        elif run == 2: 
            cursor               = self.db.cursor()
            self.sys['sess']     = []

            sql_stmt = "select  s.state, \
                        '(' || s.sid || ',' || s.serial# || ')' sid , \
                        s.username  username, \
                        case when s.state != 'WAITING' \
                             then 'On CPU (Prev: ' || case when length(s.event) > 25 \
                                                      then  rpad(s.event, 25, ' ') || '...)' \
                                                      else s.event || ')' end \
                             else  rpad(s.event || '  (' || lower(s.wait_class) || ')' , 37, ' ') || \
                        case when length(s.event || s.wait_class ) > 35 then '...' else NULL end end as event, \
                        nvl(s.p1text, 'p1') || ': ' || s.p1 p1, \
                        nvl(s.p2text, 'p2') || ': ' || s.p2 p2, \
                        nvl(s.p3text, 'p3') || ': ' || s.p3 p3 , \
                        nvl(s.blocking_session, 0) blocking_session, \
                        s.seconds_in_wait seconds, nvl(s.sql_id, '--') sql_id, \
                        s.last_call_et, \
                        io.block_gets, io.consistent_gets, io.physical_reads, io.block_changes, io.consistent_gets, \
                        p.spid, nvl(to_char(px.qcsid), ' ')  \
                        from v$session s, v$sess_io io, v$process p, v$px_session px \
                        where s.sid = px.sid(+) and s.sid = io.sid and  s.paddr = p.addr and s.username is not null \
                        and s.event not like '%rdbms ipc message%' and s.event not like '%message from client%' \
                        and s.status = 'ACTIVE' \
                        order by s.last_call_et, s.sid " 

        
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()
         
            r = []
            for i in range(0, len(rows)):
                r.append([])
                r[i] = {}
                r[i]['state']           = rows[i][0]
                r[i]['sid']             = rows[i][1]
                r[i]['username']        = rows[i][2]
                r[i]['event']           = rows[i][3]
                r[i]['p1']              = rows[i][4]
                r[i]['p2']              = rows[i][5]
                r[i]['p3']              = rows[i][6]
                r[i]['blocking_sid']    = rows[i][7]
                r[i]['sec_wait']        = rows[i][8]
                r[i]['sql_id']          = rows[i][9]
                r[i]['last_call_et']    = rows[i][10]
                r[i]['block_gets']      = rows[i][11]
                r[i]['cons_gets']       = rows[i][12]
                r[i]['phy_reads']       = rows[i][13]
                r[i]['blk_changes']     = rows[i][14]
                r[i]['cons_changes']    = rows[i][15]
                r[i]['os_pid']          = rows[i][16]
                r[i]['qc_sid']          = rows[i][17]
        
            self.sys['sess'] = r

    def get_sys_metrics_snapshot(self, run):
    
        if run == 1:
            pass
        elif run == 2:
            cursor                = self.db.cursor()
            self.sys['metric']    = {}

            sql_stmt = "select to_char(end_time, 'mm/dd/yyyy hh24:mi:ss') end_time, metric_id, metric_name, value, metric_unit  \
                        from v$sysmetric where begin_time in (select max(begin_time) from v$sysmetric) "  
        
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()
        
            r = []
            for i in range(0, len(rows)):
                r.append([])
                r[i] = {}
                r[i]['name']            = rows[i][2]
                r[i]['value']           = rows[i][3]
                r[i]['unit']            = rows[i][4]
        
            self.sys['metric'] = r


    def get_stats_snapshot(self, run):
        cursor     = self.db.cursor()
        sql_stmt   = "select statistic#, name, value from v$sysstat order by statistic#  "

        if run == 1:
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()

            self.sys['stat']               = {}
            self.sys['stat']['delta']      = []
            self.sys['stat']['run_data']   = {}

            for i in range(0, len(rows)):
                name = rows[i][1]
                self.sys['stat']['run_data'][name]           = {}
                self.sys['stat']['run_data'][name]['name']   = rows[i][1]
                self.sys['stat']['run_data'][name]['run_01'] = rows[i][2]

        if run == 2:
            d = {}
            l = []
    
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()
            for i in range(0, len(rows)):
                name = rows[i][1]
                self.sys['stat']['run_data'][name]['run_02'] = rows[i][2]
                delta = self.sys['stat']['run_data'][name]['run_02'] - self.sys['stat']['run_data'][name]['run_01']
                self.sys['stat']['run_data'][name]['delta'] = delta
     
                if delta > 0:
                    d[name] = delta
    
            l = sorted(iter(d.items()), key=operator.itemgetter(1))
            self.sys['stat']['delta'] = l
    
   

    def get_segment_stat_snapshot(self, run):
        cursor     = self.db.cursor()
        sql_stmt   = "select owner || '.' ||  object_name || ' -- ' || statistic_name , value, subobject_name, statistic_name, owner, object_name from v$segment_statistics where owner not in ('SYS')  "

        if run == 1:
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()

            self.sys['segment_stat']               = {}
            self.sys['segment_stat']['delta']      = []
            self.sys['segment_stat']['run_data']   = {}

            for i in range(0, len(rows)):
                name = rows[i][0]
                self.sys['segment_stat']['run_data'][name]           = {}
                self.sys['segment_stat']['run_data'][name]['name']   = rows[i][0]
                self.sys['segment_stat']['run_data'][name]['run_01'] = rows[i][1]

        if run == 2:
            d = {}
            l = []

            cursor.execute(sql_stmt)
            rows = cursor.fetchall()
            for i in range(0, len(rows)):
                name = rows[i][0]
                try:
                    self.sys['segment_stat']['run_data'][name]['run_02'] = rows[i][1]
                except KeyError:
                    self.sys['segment_stat']['run_data'][name]           = {}
                    self.sys['segment_stat']['run_data'][name]['name']   = rows[i][0]
                    self.sys['segment_stat']['run_data'][name]['run_01'] = 0
                    self.sys['segment_stat']['run_data'][name]['run_02'] = rows[i][1]

                delta = self.sys['segment_stat']['run_data'][name]['run_02'] - self.sys['segment_stat']['run_data'][name]['run_01']
                self.sys['segment_stat']['run_data'][name]['delta'] = delta

                if delta > 0:
                    d[name] = delta

            l = sorted(iter(d.items()), key=operator.itemgetter(1))
            self.sys['segment_stat']['delta'] = l


 
    
    def get_latch_snapshot(self, run):
        cursor = self.db.cursor()
        sql_stmt_01 = "select addr, name, gets, misses, sleeps, immediate_gets, immediate_misses from v$latch"
        sql_stmt_02 = "select parent_name, location, nwfail_count, sleep_count, wtr_slp_count from v$latch_misses"

        if run == 1:

            cursor.execute(sql_stmt_01)
            rows = cursor.fetchall()

            self.sys['latch']                  = {}
            self.sys['latch']['run_data']      = {}

            for i in range(0, len(rows)):
                name = rows[i][1]
                self.sys['latch']['run_data'][name]                      = {}
                self.sys['latch']['run_data'][name]['run_01']            = {}
                self.sys['latch']['run_data'][name]['name']              = rows[i][1]
                self.sys['latch']['run_data'][name]['run_01']['get']    = rows[i][2]
                self.sys['latch']['run_data'][name]['run_01']['miss']    = rows[i][3]
                self.sys['latch']['run_data'][name]['run_01']['sleep']   = rows[i][4]
                self.sys['latch']['run_data'][name]['run_01']['im_get']  = rows[i][5]
                self.sys['latch']['run_data'][name]['run_01']['im_miss'] = rows[i][6]

            cursor.execute(sql_stmt_02)
            rows = cursor.fetchall()

            self.sys['latch_miss']             = {}
            self.sys['latch_miss']['run_data'] = {}

            for i in range(0, len(rows)):
                id = rows[i][0] + ':' + rows[i][1]
                self.sys['latch_miss']['run_data'][id]                             = {}
                self.sys['latch_miss']['run_data'][id]['run_01']                   = {}
                self.sys['latch_miss']['run_data'][id]['run_01']['parent_name']    = rows[i][0]
                self.sys['latch_miss']['run_data'][id]['run_01']['location']       = rows[i][1]
                self.sys['latch_miss']['run_data'][id]['run_01']['nwfail_count']   = rows[i][2]
                self.sys['latch_miss']['run_data'][id]['run_01']['sleep_count']    = rows[i][3]
                self.sys['latch_miss']['run_data'][id]['run_01']['wtr_slp_count']  = rows[i][4]


        if run == 2:

            cursor.execute(sql_stmt_01)
            rows = cursor.fetchall()
            self.sys['latch']['delta'] = {}

            d = {}
            l = []
            for i in range(0, len(rows)):
                name = rows[i][1]
                self.sys['latch']['run_data'][name]['run_02']            = {}
                self.sys['latch']['run_data'][name]['delta']             = {}

                self.sys['latch']['run_data'][name]['run_02']['get']     = rows[i][2]
                self.sys['latch']['run_data'][name]['run_02']['miss']    = rows[i][3]
                self.sys['latch']['run_data'][name]['run_02']['sleep']   = rows[i][4]
                self.sys['latch']['run_data'][name]['run_02']['im_get']  = rows[i][5]
                self.sys['latch']['run_data'][name]['run_02']['im_miss'] = rows[i][6]

                self.sys['latch']['run_data'][name]['delta']['get']      = rows[i][2] - self.sys['latch']['run_data'][name]['run_01']['get'] 
                self.sys['latch']['run_data'][name]['delta']['miss']     = rows[i][3] - self.sys['latch']['run_data'][name]['run_01']['miss'] 
                self.sys['latch']['run_data'][name]['delta']['sleep']    = rows[i][4] - self.sys['latch']['run_data'][name]['run_01']['sleep'] 
                self.sys['latch']['run_data'][name]['delta']['im_get']   = rows[i][5] - self.sys['latch']['run_data'][name]['run_01']['im_get'] 
                self.sys['latch']['run_data'][name]['delta']['im_miss']  = rows[i][6] - self.sys['latch']['run_data'][name]['run_01']['im_miss'] 


                delta = self.sys['latch']['run_data'][name]['delta']['miss']    + self.sys['latch']['run_data'][name]['delta']['sleep'] + self.sys['latch']['run_data'][name]['delta']['get']     
                if delta > 0:
                    d[name] = delta

            l = sorted(iter(d.items()), key=operator.itemgetter(1))
            self.sys['latch']['sorted_delta'] = l


            cursor.execute(sql_stmt_02)
            rows = cursor.fetchall()
            self.sys['latch_miss']['delta'] = {}

            d = {}
            l = []
            for i in range(0, len(rows)):
                id = rows[i][0] + ':' + rows[i][1]
                self.sys['latch_miss']['run_data'][id]['run_02']                   = {}
                self.sys['latch_miss']['run_data'][id]['delta']                   = {}
                self.sys['latch_miss']['run_data'][id]['run_02']['parent_name']    = rows[i][0]
                self.sys['latch_miss']['run_data'][id]['run_02']['location']       = rows[i][1]
                self.sys['latch_miss']['run_data'][id]['run_02']['nwfail_count']   = rows[i][2]
                self.sys['latch_miss']['run_data'][id]['run_02']['sleep_count']    = rows[i][3]
                self.sys['latch_miss']['run_data'][id]['run_02']['wtr_slp_count']  = rows[i][4]


                self.sys['latch_miss']['run_data'][id]['delta']['nwfail_count']   = rows[i][2] - self.sys['latch_miss']['run_data'][id]['run_01']['nwfail_count'] 
                self.sys['latch_miss']['run_data'][id]['delta']['sleep_count']    = rows[i][3] - self.sys['latch_miss']['run_data'][id]['run_01']['sleep_count'] 
                self.sys['latch_miss']['run_data'][id]['delta']['wtr_slp_count']  = rows[i][4] - self.sys['latch_miss']['run_data'][id]['run_01']['wtr_slp_count'] 

                delta = self.sys['latch_miss']['run_data'][id]['delta']['wtr_slp_count'] + self.sys['latch_miss']['run_data'][id]['delta']['sleep_count']  + \
                        self.sys['latch_miss']['run_data'][id]['delta']['nwfail_count'] 
                if delta > 0:
                    d[id] = delta

                l = sorted(iter(d.items()), key=operator.itemgetter(1))
                self.sys['latch_miss']['sorted_delta'] = l


    def get_events_snapshot(self, run):
        cursor = self.db.cursor()
        sql_stmt = "select event_id, event, time_waited_micro from v$system_event where wait_class not in ('Idle') order by event_id"
    
        if run == 1:
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()

            self.sys['event']              = {}
            self.sys['event']['delta']     = []
            self.sys['event']['run_data']  = {}

            for i in range(0, len(rows)):
                name = rows[i][1] 
                self.sys['event']['run_data'][name] = {}
                self.sys['event']['run_data'][name]['name']   = rows[i][1]
                self.sys['event']['run_data'][name]['run_01'] = rows[i][2]
    
    
        if run == 2:
            d = {}
            l = []
    
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()
            for i in range(0, len(rows)):
                name = rows[i][1]

                try:
                    self.sys['event']['run_data'][name]['run_02'] = rows[i][2]
                except KeyError:
                    self.sys['event']['run_data'][name] = {}
                    self.sys['event']['run_data'][name]['name']   =  rows[i][1]
                    self.sys['event']['run_data'][name]['run_01'] = 0
                    self.sys['event']['run_data'][name]['run_02'] = rows[i][2]


                delta = self.sys['event']['run_data'][name]['run_02'] - self.sys['event']['run_data'][name]['run_01']
                self.sys['event']['run_data'][name]['delta'] = delta
    
                if delta > 0:
                    d[name] = delta
    
            l = sorted(iter(d.items()), key=operator.itemgetter(1))
            self.sys['event']['delta'] = l
    


    def get_file_io_snapshot(self, run):
        cursor = self.db.cursor()
        sql_stmt = " select a.file#,  rpad(regexp_replace(a.name, '/.*/', '.../'), 45), \
                     b.PHYRDS + b.PHYWRTS, b.PHYRDS, b.PHYWRTS, b.PHYBLKRD, b.PHYBLKWRT, b.SINGLEBLKRDS, b.READTIM, b.WRITETIM, b.SINGLEBLKRDTIM \
                     from v$datafile a, v$filestat b where a.file# = b.file#"

        if run == 1:

            cursor.execute(sql_stmt)
            rows = cursor.fetchall()

            self.sys['file_io']                  = {}
            self.sys['file_io']['run_data']      = {}

            for i in range(0, len(rows)):
                name = rows[i][1]
                self.sys['file_io']['run_data'][name]                             = {}
                self.sys['file_io']['run_data'][name]['run_01']                   = {}
                self.sys['file_io']['run_data'][name]['name']                     = rows[i][1]
                self.sys['file_io']['run_data'][name]['run_01']['io_ops']         = rows[i][2]
                self.sys['file_io']['run_data'][name]['run_01']['phyrds']         = rows[i][3]
                self.sys['file_io']['run_data'][name]['run_01']['phywrts']        = rows[i][4]
                self.sys['file_io']['run_data'][name]['run_01']['phyblkrd']       = rows[i][5]
                self.sys['file_io']['run_data'][name]['run_01']['phyblkwrt']      = rows[i][6]
                self.sys['file_io']['run_data'][name]['run_01']['singleblkrds']   = rows[i][7]
                self.sys['file_io']['run_data'][name]['run_01']['readtim']        = rows[i][8]
                self.sys['file_io']['run_data'][name]['run_01']['writetim']       = rows[i][9]
                self.sys['file_io']['run_data'][name]['run_01']['singleblkrdtim'] = rows[i][10]


        if run == 2:

            cursor.execute(sql_stmt)
            rows = cursor.fetchall()
            self.sys['file_io']['delta'] = {}

            d = {}
            l = []
            for i in range(0, len(rows)):
                name = rows[i][1]
                self.sys['file_io']['run_data'][name]['run_02']                   = {}
                self.sys['file_io']['run_data'][name]['delta']                    = {}

                self.sys['file_io']['run_data'][name]['run_02']['io_ops']         = rows[i][2]
                self.sys['file_io']['run_data'][name]['run_02']['phyrds']         = rows[i][3]
                self.sys['file_io']['run_data'][name]['run_02']['phywrts']        = rows[i][4]
                self.sys['file_io']['run_data'][name]['run_02']['phyblkrd']       = rows[i][5]
                self.sys['file_io']['run_data'][name]['run_02']['phyblkwrt']      = rows[i][6]
                self.sys['file_io']['run_data'][name]['run_02']['singleblkrds']   = rows[i][7]
                self.sys['file_io']['run_data'][name]['run_02']['readtim']        = rows[i][8]
                self.sys['file_io']['run_data'][name]['run_02']['writetim']       = rows[i][9]
                self.sys['file_io']['run_data'][name]['run_02']['singleblkrdtim'] = rows[i][10]

                self.sys['file_io']['run_data'][name]['delta']['io_ops']        = rows[i][2] - self.sys['file_io']['run_data'][name]['run_01']['io_ops']
                self.sys['file_io']['run_data'][name]['delta']['phyrds']        = rows[i][3] - self.sys['file_io']['run_data'][name]['run_01']['phyrds']
                self.sys['file_io']['run_data'][name]['delta']['phywrts']       = rows[i][4] - self.sys['file_io']['run_data'][name]['run_01']['phywrts']
                self.sys['file_io']['run_data'][name]['delta']['phyblkrd']      = rows[i][5] - self.sys['file_io']['run_data'][name]['run_01']['phyblkrd']
                self.sys['file_io']['run_data'][name]['delta']['phyblkwrt']     = rows[i][6] - self.sys['file_io']['run_data'][name]['run_01']['phyblkwrt']
                self.sys['file_io']['run_data'][name]['delta']['singleblkrds']  = rows[i][7] - self.sys['file_io']['run_data'][name]['run_01']['singleblkrds']
                self.sys['file_io']['run_data'][name]['delta']['readtim']  = rows[i][8] - self.sys['file_io']['run_data'][name]['run_01']['readtim']
                self.sys['file_io']['run_data'][name]['delta']['writetim']  = rows[i][9] - self.sys['file_io']['run_data'][name]['run_01']['writetim']
                self.sys['file_io']['run_data'][name]['delta']['singleblkrdtim']  = rows[i][10] - self.sys['file_io']['run_data'][name]['run_01']['singleblkrdtim']


                delta = self.sys['file_io']['run_data'][name]['delta']['io_ops'] 
                if delta > 0:
                    d[name] = delta

            l = sorted(iter(d.items()), key=operator.itemgetter(1))
            self.sys['file_io']['sorted_delta'] = l




#########################
##
## main()
##
##############################
if __name__ == '__main__':
    main()


