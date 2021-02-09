#! /usr/bin/python

"""
  A Python 2.6, DB2 script, run daily, find, fix out-of-sync, PD player accounts which occurred in the last day.
  https://jira.gtech.com/jira/browse/CASA-20496
  Related to https://jira.gtech.com/jira/browse/CASA-11034
  Author: Pete Jansz

"""
import copy
import csv
import datetime
import glob
import io
import json
import logging
import logging.config
import os.path
import pprint
import re
import shutil
import string
import subprocess
import sys
from datetime import date
from player import Player
from os import chdir
from subprocess import Popen, PIPE
from optparse import OptionParser

SCRIPT_NAME = 'sync-pd-services'
DEFAULT_EXTRACTS_DIR = os.path.join('/files/db2/scripts/', SCRIPT_NAME)
DEFAULT_EXPORT_DEL_SQL_FILENAME = 'export-sync-pd-services.sql'
DEFAULT_EXPORT_DEL_CSV_FILENAME = SCRIPT_NAME + '.csv'
DEFAULT_UPDATE_PLAYER_SQLT_FILENAME = 'sync-pd-player-services.sqlt'
DEFAULT_LOG_DIR = os.path.join('/db2dumps/output_logs/', SCRIPT_NAME)
DEFAULT_DAYS_BACK = -1
DEFAULT_DBNAME = 'PDDB'
DEFAULT_DB2OPTS = '-cmstx +p'
LOGGER_NAME = SCRIPT_NAME

logger = None
parser = OptionParser()

CSV_HEADERS = 'CONTRACT_IDENTITY,ACCOUNT_EMAIL,CONTRACT_ID,EMAIL_VERIFIED,SERVICE_TYPE_IDS,SERVICE_STATUS_IDS'

def parse_cli_args():
    description = 'Find, fix (sync) out-of-sync, PD player accounts\n'
    help_path = 'Working directory path (default=' + DEFAULT_EXTRACTS_DIR + ')'
    help_export_sql = 'Export SQL filename (default=' + DEFAULT_EXPORT_DEL_SQL_FILENAME + ')'
    help_csv = 'Input del csv filename (default=' + DEFAULT_EXPORT_DEL_CSV_FILENAME + ')'
    help_update_sqlt = 'Update player services SQLT filename (default=' + DEFAULT_UPDATE_PLAYER_SQLT_FILENAME + ')'
    help_dbname = 'DB name (default=' + DEFAULT_DBNAME + ')'
    help_db2opts = 'DB2 opts (default=' + DEFAULT_DB2OPTS + ')'
    help_days_back = 'Find player accounts where changed days back (default=' + str(DEFAULT_DAYS_BACK) + ')'

    parser.description = description
    parser.add_option('--path', action='store', type='str',
                      dest='path', help=help_path, default=DEFAULT_EXTRACTS_DIR, metavar='FILE')
    parser.add_option('--export_sql', action='store', type='str',
                      dest='export_sql', help=help_export_sql, default=DEFAULT_EXPORT_DEL_SQL_FILENAME, metavar='FILE')
    parser.add_option('--csvfile', action='store', help=help_csv, default=DEFAULT_EXPORT_DEL_CSV_FILENAME,
                      type='str', dest='csvfile', metavar='FILE')
    parser.add_option('--update_sqlt', action='store', help=help_update_sqlt, default=DEFAULT_UPDATE_PLAYER_SQLT_FILENAME,
                      type='str', dest='update_sqlt', metavar='FILE')
    parser.add_option('--nodb', action='store_true',
                      help='No db2 access. Read csvfile and tells you what it proposes to do', dest='nodb', default=False)
    parser.add_option('--find', action='store_true',
                      help='Only find, export to the csvfile, do not update players', dest='find', default=False)
    parser.add_option('--days_back', action='store', type='int',
                      dest='days_back', help=help_days_back, default=DEFAULT_DAYS_BACK)
    parser.add_option('--dbname', action='store', type='str',
                      dest='dbname', help=help_dbname, default=DEFAULT_DBNAME)
    parser.add_option('--db2opts', action='store', type='str', dest='db2opts',
                      help=help_db2opts, default=DEFAULT_DB2OPTS)

    return parser.parse_args()

def init_work_dir(options):

    logfile_name = os.path.join(DEFAULT_LOG_DIR, 'sync-pd-services-db2-clp.log.' + date.today().isoformat())
    historylog_name = os.path.join(DEFAULT_LOG_DIR, 'sync-pd-services-history.log')

    if options.db2opts:
        os.environ['DB2OPTIONS'] = options.db2opts
    else:
        os.environ['DB2OPTIONS'] = (
            DEFAULT_DB2OPTS + ' -z ' + logfile_name + ' -l ' + historylog_name)

    return (logfile_name, historylog_name)

def convert_popen_strs_to_str(strs):
    msg = ' '.join(strs.split('\\r'))
    return msg.replace('\r', '').replace('\n', '')

def run_export_sync_pd_services(options, logfile_name, historylog_name):
    # Archive an existing CSV file?
    default_export_filename = os.path.join(options.path, DEFAULT_EXPORT_DEL_CSV_FILENAME)
    if os.path.exists(default_export_filename):
        date_filename = DEFAULT_EXPORT_DEL_CSV_FILENAME + '.' + date.today().isoformat()
        archive_export_filename = os.path.join(DEFAULT_LOG_DIR, date_filename)
        shutil.move(default_export_filename, archive_export_filename)

    connect_session = Popen(['db2', ('connect to ' + options.dbname)], stdout=PIPE, stderr=PIPE)
    if connect_session.wait():
        stdoutStrings, stderrStrings = connect_session.communicate()
        msg = convert_popen_strs_to_str(stdoutStrings)
        raise Exception(msg)

    logger.debug('run_export_sync_pd_services connected to ' + options.dbname)

    process_session = Popen(['db2', '-z', logfile_name, '-l', historylog_name], stdin=PIPE, stdout=PIPE, stderr=PIPE)

    sql_stmt = read_file(options.export_sql)
    if options.days_back != DEFAULT_DAYS_BACK:
        sql_stmt = sql_stmt.replace(
            'current date -1 day', 'current date ' + str(options.days_back) + ' day')

    process_session.stdin.write(sql_stmt)

    stdoutStrings, stderrStrings = process_session.communicate()
    omsg = convert_popen_strs_to_str(stdoutStrings)
    emsg = convert_popen_strs_to_str(stderrStrings)
    if omsg.count('error'):
        raise Exception(omsg)
    if emsg.count('error'):
        raise Exception(emsg)

def check_hadr(options):
    """
    Raise exception if HADR and role is Standby.
    """
    hadr_enabled = False
    role_standby = False
    process_session = Popen(['db2', ('get snapshot for database on ' + options.dbname) ], stdout=PIPE, stderr=PIPE)
    stdoutStrings, stderrStrings = process_session.communicate()

    for line in stdoutStrings.split('\\r'):
        if line.count('HADR'): hadr_enabled = True
        if re.search('Role .*= Standby', line): role_standby = True
        if line.count('SQL1013N'):
            msg = convert_popen_strs_to_str(line)
            raise Exception(msg)

    if hadr_enabled and role_standby:
        msg = 'HADR Role: Standby'
        raise Exception(msg)

def create_sql_stmt(options, player):
    sqlt = read_file(options.update_sqlt)
    sql_stmt = sqlt.replace('ContractId', player.contractId).replace('EmailVerifiedStatus', player.emailVerified).replace(
        'PP_ServiceStatusId', player.portalService)
    return sql_stmt

def read_file(filename):
    fin = open(filename, 'r')

    s = ''
    for line in fin.readlines():
        s += line

    fin.close()

    return s

def fix_player(player):
    fixed_player = copy.deepcopy(player)

    NOT_VERIFIED = '0'
    VERIFIED = '1'
    PREACTIVE = '1'
    ACTIVE = '2'
    SUSPENDED = '3'

    # Scenario # 1
    if player.emailVerified == VERIFIED and player.portalService == PREACTIVE and player.secondChanceService == PREACTIVE:
        fixed_player.emailVerified = VERIFIED
        fixed_player.suspend()
    # 2
    elif player.emailVerified == VERIFIED and player.portalService == PREACTIVE and player.secondChanceService == ACTIVE:
        fixed_player.emailVerified = VERIFIED
        fixed_player.activate()
    # 3
    elif player.emailVerified == NOT_VERIFIED and player.portalService == SUSPENDED and player.secondChanceService == PREACTIVE:
        fixed_player.emailVerified = NOT_VERIFIED
        fixed_player.preactivate()
    # 4
    elif player.emailVerified == VERIFIED and player.portalService == ACTIVE and player.secondChanceService == PREACTIVE:
        fixed_player.emailVerified = VERIFIED
        fixed_player.activate()
    # 5
    elif player.emailVerified == VERIFIED and player.portalService == SUSPENDED and player.secondChanceService == PREACTIVE:
        fixed_player.emailVerified = VERIFIED
        fixed_player.suspend()
    # 6
    elif player.emailVerified == VERIFIED and player.portalService == PREACTIVE and player.secondChanceService == SUSPENDED:
        fixed_player.emailVerified = VERIFIED
        fixed_player.suspend()
    # 7
    elif player.emailVerified == VERIFIED and player.portalService == ACTIVE and player.secondChanceService == SUSPENDED:
        fixed_player.emailVerified = VERIFIED
        fixed_player.suspend()
    # 8
    elif player.emailVerified == VERIFIED and player.portalService == SUSPENDED and player.secondChanceService == ACTIVE:
        fixed_player.emailVerified = VERIFIED
        fixed_player.suspend()
    # 9
    elif player.emailVerified == NOT_VERIFIED and player.portalService == ACTIVE and player.secondChanceService == ACTIVE:
        fixed_player.emailVerified = NOT_VERIFIED
        fixed_player.preactivate()
    # 10
    elif player.emailVerified == VERIFIED and player.portalService == SUSPENDED and player.secondChanceService == SUSPENDED:
        pass
    # 11
    elif player.emailVerified == NOT_VERIFIED and player.portalService == SUSPENDED and player.secondChanceService == SUSPENDED:
        fixed_player.emailVerified = VERIFIED
        fixed_player.suspend()

    return fixed_player

def report_player(processed_count, total_count, player, fixed_player):

    stats_str = '%03s/%s' % (str(processed_count), str(total_count))
    msg = stats_str + ' skipped %s' % fixed_player

    if player != fixed_player:
        msg = stats_str + ' synced  %s' % fixed_player

    logger.info(msg)

def no_db(options):
    options.csvfile = options.path + '/' + options.csvfile

    processed_count = 0
    total_count = sum(1 for line in open(options.csvfile))
    csv_file = open(options.csvfile)
    # Pring headings
    format = '                   %s, %s, %s, %s, %s, %s'
    print(format % (
        'contract_identity',
        'contract_id',
        'email_verified',
        'pp_service',
        'sc_service',
        'username'
    ))

    for csv_line in csv_file:
        if csv_line.find('CONTRACT_IDENTITY') >= 0:
            continue  # Skip column-heading row.

        player = Player(csv_line)
        fixed_player = fix_player(player)

        processed_count += 1

        report_player(processed_count, total_count, player, fixed_player)

    exit_value = 0
    exit(exit_value)

def init_logger(options):
    global logger
    logging.config.fileConfig(
        options.path + '/logging.conf', disable_existing_loggers=False)
    logger = logging.getLogger(LOGGER_NAME)

    file_formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    if not os.path.exists(DEFAULT_LOG_DIR):
        os.mkdir(DEFAULT_LOG_DIR)

    logfilename = os.path.join(DEFAULT_LOG_DIR, SCRIPT_NAME + '.log')
    fileHandler = logging.FileHandler(logfilename)
    fileHandler.setFormatter(file_formatter)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(fileHandler)

def main():
    exit_value = 1
    options, args = parse_cli_args()

    if not os.path.exists(options.path):
        sys.stderr.write('\nPath not found: ' + options.path)
        exit(exit_value)

    options.path = os.path.abspath(options.path)
    os.chdir(options.path)
    init_logger(options)
    logger.info('Logger initialized - Starting application')

    if options.nodb: no_db(options)

    try:
        options.export_sql = os.path.join(options.path, options.export_sql)
        if not os.path.exists(options.export_sql):
            msg = 'Export SQL file not found:' + options.export_sql
            raise Exception(msg)

        options.update_sqlt = os.path.join(options.path, options.update_sqlt)
        if not os.path.exists(options.update_sqlt):
            msg = 'SQLT file not found: ' + options.update_sqlt
            raise Exception(msg)

        options.csvfile = os.path.join(options.path, options.csvfile)

        logger.info('Calling check_hadr() ...')
        check_hadr(options)
        (logfile_name, historylog_name) = init_work_dir(options)
        logger.info('Calling run_export_sync_pd_services() ...')
        run_export_sync_pd_services(options, logfile_name, historylog_name)

        if (options.find):
            logger.info('Exiting.')
            exit(0)

        logger.debug('Preparing to process players ...')
        connect_session = Popen(['db2', ('connect to ' + options.dbname)], stdout=PIPE, stderr=PIPE)
        if connect_session.wait():
            stdoutStrings, stderrStrings = connect_session.communicate()
            raise Exception(convert_popen_strs_to_str(stdoutStrings))

        processed_count = 0
        total_count = sum(1 for line in open(options.csvfile))
        logger.info('Exported ' + str(total_count) + ' players')

        csv_file = open(options.csvfile)
        for csv_line in csv_file:
            if csv_line.find('CONTRACT_IDENTITY') >= 0:
                continue  # Skip column-heading row.

            player = Player(csv_line)
            fixed_player = fix_player(player)

            if fixed_player != player:
                sql_stmt = create_sql_stmt(options, fixed_player)

                process_session = Popen(['db2', '-z', logfile_name, '-l', historylog_name], stdin=PIPE, stdout=PIPE, stderr=PIPE)
                process_session.stdin.write(sql_stmt)
                stdoutStrings, stderrStrings = process_session.communicate()
                omsg = convert_popen_strs_to_str(stdoutStrings)
                emsg = convert_popen_strs_to_str(stderrStrings)
                if omsg.count('error'): raise Exception(omsg)
                if emsg.count('error'): raise Exception(emsg)

            processed_count += 1
            report_player(processed_count, total_count, player, fixed_player)

        exit_value = 0
    except Exception as error:
        errtype, value, traceback = sys.exc_info()
        msg = str(value)
        logger.error(msg)

    sys.exit(exit_value)

if __name__ == "__main__":
    main()
