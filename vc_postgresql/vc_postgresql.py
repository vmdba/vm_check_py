#/usr/bin/python2.4
#
# Small script to show PostgreSQL and Pyscopg together
#
'''
postgres_vc_check.py
Author: Kay Liew
Date: Sept 11, 2016
Description: Extract vCenter or VCSA database schema information for more accurate
             and faster troubleshooting.

To run this tool remotely, pg_hba.conf need to have the correct setup and trust user. postgresql.conf listener need to be uncommented for the right IP

psycopg2 and psutil will need to be installed.

'''

import psycopg2
import sys
import os
import platform
import psutil
import time
import socket
import subprocess

try:
    conn = psycopg2.connect("dbname='vcdb' user='postgres' host='10.21.98.58' port='5432'")
    cursor = conn.cursor()
    print "Connected to database."
except:
    print "Failed to connect to the database. Please check if user and password are correct or pg_hba has any restrictions"

# Init
#########################################################
start = time.time()

def get_cur_user():
    cursor.execute("select current_user")
    return cursor.fetchone()

def get_cur_database():
    cursor.execute("select current_database()")
    return cursor.fetchone()

def get_version():
    cursor.execute("select * from vpx_version")
    return cursor.fetchone()

def get_cur_date():
    cursor.execute("select current_date")
    return cursor.fetchone()

def get_cur_schema():
    cursor.execute("select current_schema()")
    return cursor.fetchone()

def get_psql_version():
    cursor.execute("select version()")
    return cursor.fetchone()

def get_hist_stats():
    cursor.execute("select relname, reltuples from pg_class where relname like 'vpx_hist%' and relname not in ('vpx_hist_stat1','vpx_hist_stat2','vpx_hist_stat3','vpx_hist_stat4') order by 1")
    get_row_hist_stat=cursor.fetchall()
    count = 0
    print "Hist Stats Tables                  Number of rows"
    print "_________________________________________________"
    for row in get_row_hist_stat:
        print row[0].ljust(20, ' '), "---------------> ",str(int(row[1])).ljust(20, ' ')
        count = count +1
    print "\n"
    if count != 541:
        print "Hist is inaccurate. Please report to VMware GSS"
    else:
        print "Total Historical Statistics Table Count :", count
        print "\n"
    return cursor.fetchone()

def get_hist_stats_view():
    cursor.execute("select relname, reltuples from pg_class where relname in ('vpx_hist_stat1','vpx_hist_stat2','vpx_hist_stat3','vpx_hist_stat4')")
    print "\n"
    print "Table Name                       Row Counts"
    print "___________________________________________"
    for row in cursor.fetchall():
        print row[0].ljust(20,' '), "---------------> ", str(int(row[1])).ljust(5, ' ')


def get_bloat_tables():
    cursor.execute("select relname, reltuples from pg_class where relname in ('vpx_event','vpx_event_arg','vpx_task','vpx_stat_counter','vpx_topn_past_day','vpx_topn_past_week','vpx_topn_past_month','vpx_topn_past_year') order by 1")
    for row in cursor.fetchall():
        print row[0].ljust(20, ' '), "---------------> ",str(int(row[1])).ljust(20, ' ')
    return cursor.fetchall()

def get_vm_counts():
    cursor.execute("select * from vpx_vm")
    return cursor.rowcount

def get_host_counts():
    cursor.execute("select * from vpx_host")
    return cursor.rowcount

def get_vacuum_tables():
    "pg_relation_size is in bytes"
    cursor.execute("select table_name, pg_relation_filepath(table_name) as Path ,pg_relation_size(table_name) as Size  FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY size DESC LIMIT 10")
    row_vacuum=cursor.fetchall()
    print "\n"
    print "Table Size"
    print "Table Name                Table pyhsical location                           Size"
    print "________________________________________________________________________________"
    for row in row_vacuum:
        print row[0].ljust(25, ' '), row[1].ljust(50, ' '), str(int(row[2])/1024).ljust(5, ' ')

def get_analyze_big_tab():
    cursor.execute("select relname,last_vacuum, last_autovacuum, last_analyze, last_autoanalyze from pg_stat_user_tables where relname in (select table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY pg_relation_size(table_name) DESC LIMIT 10)")
    row_analyze_tabs=cursor.fetchall()
    print "\n"
    print "Table Name                            Last  Vacuum                         Last Autovacuum                            Last Analyze                            Last AutoAnalyze            "
    print "__________________________________________________________________________________________________________________________________________________________________"
    for row in row_analyze_tabs:
        print row[0].ljust(40, ' '), str(row[1]).ljust(40, ' '), str(row[2]).ljust(40, ' '), str(row[3]).ljust(40, ' '), str(row[4]).ljust(40, ' ')

def get_indexes():
    cursor.execute("select job_id, status, last_run from vpx_job_log")
    print "\n"
    print "Job ID     Status     Last Run        "
    print "______________________________________"
    for row in cursor.fetchall():
        print str(row[0]).ljust(10, ' '), row[1].ljust(10, ' '), str(row[2]).ljust(10, ' ')
'''
def get_event_keeps():
    cursor.execute("select * from vpx_parameter where name ='event.maxAge'")
    row_event_keep = cursor.fetchall()
    for rows_event_keeps in row_event_keep:
        if rows_event_keeps[1] <= 60:
            print "Events have been set for", rows_event_keeps[1] , " days."
            print "If the event table counts and the size are extremely large (over a billion rows or 20gig), it typically impacting overall database performance"
            print "User might consider to reduce the events (purge by dates or truncate) and lower to 30 days"
        else:
            print rows_event_keeps[0].rstrip('.maxAge'), "Events have been set for", rows_event_keeps[1] , " days."
'''
def get_vcenter_procedures():
    cursor.execute("select pp.proname, pl.lanname, pn.nspname from pg_proc pp inner join pg_namespace pn on (pp.pronamespace = pn.oid) inner join pg_language pl on (pp.prolang = pl.oid) where pl.lanname NOT IN ('c','internal')  and pn.nspname NOT LIKE 'pg_%' and pn.nspname <> 'information_schema'")
    print "\n"
    print "Procedure Name                          Proc Type      Owner     "
    print "_________________________________________________________________"
    for row in cursor.fetchall():
        print row[0].ljust(40, ' '), row[1].ljust(15, ' '), row[2].ljust(5, ' ')
#    return cursor.fetchall()


def get_statistic_level():
    cursor.execute("select INTERVAL_DEF_NAME , INTERVAL_VAL/60, INTERVAL_LENGTH/3600, STATS_LEVEL, ROLLUP_ENABLED_FLG from VPX_STAT_INTERVAL_DEF")
    row8 = cursor.fetchall()
    print "\n"
    print "Stat         Interval     Save      Stat   Enable          "
    print "Type         Duration     Days      Level                  "
    print "_______________________________________________________    "
    for row8a in row8:
		if row8a[4] == 1:
			print row8a[0].ljust(20, ' ').lstrip("history."), str(int(round(row8a[1]))).ljust(12, ' '), str(int((round(row8a[2]))/24)).ljust(10, ' '),str(row8a[3]). ljust(5, ' '), "ENABLED"
		else:
			print row8a[0].ljust(20, ' ').lstrip("history."), str(int(round(row8a[1]))).ljust(12, ' '), str(row8a[2]).ljust(10, ' '),str(row8a[3]). ljust(13, ' '), "DISABLED"

def get_vacuum():
    stmt = """ SELECT relname, pg_stat_get_live_tuples(c.oid) AS n_live_tup,
pg_stat_get_tuples_deleted(oid) as Deleted_tup,
pg_stat_get_dead_tuples(c.oid) AS n_dead_tup,
pg_stat_get_autovacuum_count(oid) as vacuumed_by_autovacuum_daemon, pg_stat_get_last_autoanalyze_time(oid) as auto_analyze_manual
FROM   pg_class c where relname like 'vpx_event%' or relname ='vpx_task'"""
    cursor.execute(stmt)
    vacuum_row = cursor.fetchall()
    print "\n"
    print "Table Name      Live Tuples   Deleted Tuples   Dead Tuples  Auto Vacuumed  Last Analyze   "
    print "___________________________________________________________________________________________"
    for row in vacuum_row:
        print str(row[0]).ljust(20, ' '), str(row[1]).ljust(13, ' '), str(row[2]).ljust(13, ' '), str(row[3]).ljust(13, ' '),  str(row[4]).ljust(13, ' '), str(row[5]).ljust(13, ' ')

def transaction_wraparound():
    cursor.execute("select txid_current(), txid_current_snapshot()")
    xid_row = cursor.fetchone()
    print "\n"
    print "Current XID, Current Snapshot XID"
    print "_________________________________"
    print xid_row


#fetchall returns tuples
#ljust is string .. needs to convert 1,2,3,4 since they are int.
def get_event_keeps():
    cursor.execute("select * from vpx_parameter where name in ('event.maxAge','task.maxAge')")
    row_event_keep = cursor.fetchall()
    print "\n"
    print "Events and Tasks           Days Kept     "
    print "_________________________________________"
    for rows_event_keeps in row_event_keep:
        if rows_event_keeps[1] <= 60:
            print "Events and Tasks have been set for", rows_event_keeps[1] , " days."
            print "If the event table counts and the size are extremely large (over a billion rows or 20gig), it typically impacting overall database performance"
            print "User might consider to reduce the events (purge by dates or truncate) and lower to 30 days"
        else:
            print rows_event_keeps[0].rstrip('.maxAge'), "Events have been set for", rows_event_keeps[1] , " days."

print "Report gathered. Please locate the vCenterDB_postgresql.log under ", os.getcwd()
print "\n"
print "Please collect the following files"
print "##################################"
print "\n"
cursor.execute("select name, setting from pg_settings where setting like '%postgresql.conf' or setting like '%pg_ident.conf'")
for row_files in cursor.fetchall():
    print row_files[0], row_files[1]
    print "\n"

cursor.execute("select setting from pg_settings where name = 'data_directory'")
for row_files2 in cursor.fetchall():
    print "pg_logs :", row_files2[0].rstrip() + "/pg_log/{all the log files}"
print "\n"
print "######## End of Report ###########"
print "\n"
print "Disconnecting from database"
print "\n"

sys.stdout=open("vCenterDB_postgresql.log","w")
print "\n"
print "####################################################################################################"
print "#      vCenter Proactive Check Tool - python version                                               #"
print "#      Disclaimer: Use this tool at your own risk and the tool does not associated with companies  #"
print "####################################################################################################"
print "\n"
print "Hostname           : ", socket.gethostname()
print "IP address         : ", socket.gethostbyname(socket.gethostname())
print "Operating System   : ", platform.platform()
print "Processor          : ", platform.processor()
print "Python version     : ", platform.python_version()
print "CPU Cycle          : ", psutil.cpu_times()
print "CPU Usage 2        : ", psutil.cpu_times_percent()
print "Memory             : ", psutil.virtual_memory()
print "Disk Usage         : ", psutil.disk_usage('/')
print "Note               : ", "Please refer to Op Metric for supportability"
print "Op Metric URL      : ", "https://www.vmware.com/resources/compatibility/sim/interop_matrix.php"
print "vCenter Version    : ", get_version()
print "Current Database   : ", get_cur_database()
print "Current User       : ", get_cur_user()
print "Currrent schema    : ", get_cur_schema()
print "Current psql time  : ", get_cur_date()
print "PostgreSQL version : ", get_psql_version()
print "Script ran         : ", time.asctime( time.localtime(time.time()) )
print "Number of Hosts    : ", get_host_counts()
print "Number of VMs      : ", get_vm_counts()
print "\n"
get_hist_stats()
get_hist_stats_view()
get_bloat_tables()
get_statistic_level()
get_event_keeps()
get_vcenter_procedures()
get_indexes()
get_vacuum_tables()
get_vacuum()
get_analyze_big_tab()
transaction_wraparound()
elapsed = (time.time() - start)
print "\n"
process = subprocess.Popen(['df', '-h'] , stdout=subprocess.PIPE)
out, err = process.communicate()
print(out)
print "\n"
print "Time Taken for the script to run :", round(int(elapsed)), " seconds"

#OS level
#Clean up
cursor.close()
conn.close()
