#!/opt/rh/python27/root/usr/bin/python

from ansible.module_utils.basic import *
from ansible.module_utils.facts import *
import subprocess
import sys
import os
import json
import re
import math

try:
    import cx_Oracle
    cx_Oracle_found = True
except ImportError:
    cx_Oracle_found = False

# Reference links
# http://www.oracle.com/technetwork/articles/dsl/prez-python-queries-101587.html

# Notes: IAW this doc : http://docs.ansible.com/ansible/latest/dev_guide/developing_modules_general.html
# This module was setup to return a dictionary called "ansible_facts" which then makes those facts usable
# in the ansible playbook, and roles. The facts in this module are referenced by using the format:
#                    sourcefacts['key'] which returns associated value - the ref name : "sourcefacts" was created in this module
#     example:    {{ sourcefacts['oracle_version'] }} => 11.2.0.4

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'Cru DBA team',
                    'version': '0.1'}

DOCUMENTATION = '''
---
module: dbfacts
short_description: Get Oracle Database facts from a remote database.
(remote database = a database not in the group being operated on)

notes: Returned values are then available to use in Ansible.
requirements: [ python2.* ]
author: "DBA Oracle module Team"
'''

EXAMPLES = '''

    # if cloning a database and source database information is desired
    - local_action:
        module: dbfacts
        systempwd: "{{ database_passwords[source_db_name].system }}"
        db_name: "{{ source_db_name }}"
        host: "{{ source_host }}"
        refname: "{{ refname_str }} (1)"
        ignore: True (2)
        debugging: False
      become_user: "{{ remote_user }}"
      register: src_facts

      (1) refname - name used in Ansible to reference these facts ( i.e. sourcefacts, destfacts )

      (2) ignore - (connection errors) is optional. If you know the source
          database may be down set ignore: True. If connection to the
          source database fails the module will not throw a fatal error
          and continue.

   NOTE: these modules can be run with the when: master_node statement.
         However, their returned values cannot be referenced in
         roles or tasks later. Therefore, when running fact collecting modules,
         run them on both nodes. Do not use the "when: master_node" clause.

'''

# Add anything from v$parameter table to retrieve in here.
vparams=[ "cluster_database",
          "compatible",
          "sga_target",
          "pga_aggregate_target",
          "sga_max_size",
          "db_recovery_file_dest",
          "db_recovery_file_dest_size",
          "diagnostic_dest",
          "remote_listener",
          "db_unique_name",
          "db_block_size",
          "remote_login_passwordfile",
          "spfile",
          "user_dump_dest",
          "core_dump_dest",
          "background_dump_dest",
          "audit_file_dest",
          "db_files" ]

msg = ""
debugme = False
defrefname = "dbfacts"
true_bool = ['True','T','true','t','True','Yes','y']


def add_to_msg(a_msg):
    """Add the arguement to the msg to be passed out"""
    global msg

    if msg:
        msg = msg + " " + a_msg
    else:
        msg = a_msg


def debugg(db_msg):
    """if debugging is on add this to msg"""
    global msg
    global debugme

    if debugme:
        add_to_msg(db_msg)


def convert_size(arg_size_bytes, vunit):
    """Given bytes and units ( K, M, G, T)
       convert input bytes to that unit:
             vtemp = convert_size(float(vtemp),"M")
    """

    size_bytes = arg_size_bytes

    if size_bytes == 0:
       return "0B"

    vunit = vunit.upper()

    size_name = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")

    vidx = int(size_name.index(vunit))

    p = math.pow(1024, vidx)
    s = round(size_bytes / p, 2)
    # i = int(math.floor(math.log(size_bytes, 1024)))
    # p = math.pow(1024, i)
    # s = round(size_bytes / p, 2)
    return "%s%s" % (int(round(s)), size_name[vidx])


# ==============================================================================
# =================================== MAIN =====================================
# ==============================================================================
def main ():
    """ Return Oracle database parameters from a database not in the specified group"""
    global msg
    global defrefname
    global debugme
    ansible_facts={}
    is_rac = None
    ignore_err_flag = False

    # Name to call facts dictionary being passed back to Ansible
    # This will be the name you reference in Ansible. i.e. source_facts['sga_target'] (source_facts)
    refname = ""

    os.system("/usr/bin/scl enable python27 bash")
    # os.system("scl enable python27 bash")

    module = AnsibleModule(
      argument_spec = dict(
        systempwd       =dict(required=True),
        db_name         =dict(required=True),
        host            =dict(required=True),
        refname         =dict(required=False),
        ignore          =dict(required=False),
        debugging       =dict(required=False)
      ),
      supports_check_mode=False,
    )

    # Get arguements passed from Ansible playbook
    vdbpass    = module.params.get('systempwd')
    vdb        = module.params.get('db_name')
    vdbhost    = module.params.get('host')
    vrefname   = module.params.get('refname')
    vignore    = module.params.get('ignore')
    vdebug     = module.params.get('debugging')

    if vdebug in true_bool:
      debugme = True
    else:
      debugme = False

    if vignore is None:
      vignore = False

    if '.org' in vdbhost:
        vdbhost = vdbhost.replace('.ccci.org','')

    if not cx_Oracle_found:
        module.fail_json(msg="Error: cx_Oracle module not found")

    if not vrefname:
        refname = defrefname
    else:
        refname = vrefname

    # check required vars passed in are not NULL.
    if ( vdbpass is not None) and (vdb is not None) and (vdbhost is not None):

        try:

            if '.org' in vdbhost:
                vdbhost = vdbhost.replace(".ccci.org","")

            if '60' not in vdbhost:
                vdb = vdb + vdbhost[-1:]

            if '.org' not in vdbhost:
                vdbhost = vdbhost + ".ccci.org"

            dsn_tns = cx_Oracle.makedsn(vdbhost, '1521', vdb)
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                add_to_msg("Failed to create dns_tns: %s" %s (error.message))
            else:
                module.fail_json(msg='TNS generation error: %s, db name: %s host: %s' % (error.message, vdb, vdbhost), changed=False)
        debugg("DEBUG[01] :: dsn_tns=%s system password=%s" % (dsn_tns,vdbpass))
        try:
          con = cx_Oracle.connect('system', vdbpass, dsn_tns)
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              add_to_msg("DB CONNECTION FAILED : %s" % (error.message))
              debugg(" vignore: %s " % (vignore))
              module.exit_json(msg=msg, ansible_facts=ansible_facts, changed="False")
          else:
              module.fail_json(msg='Database connection error: %s, tnsname: %s host: %s' % (error.message, vdb, vdbhost), changed=False)

        cur = con.cursor()

        ansible_facts = { refname : {}}

        # get parameters listed in the header of this program defined in "vparams"
        for idx in range(len(vparams)):
            try:
              v_sel = "select value from v$parameter where name = '" + vparams[idx] + "'"
              cur.execute(v_sel)
            except cx_Oracle.DatabaseError as exc:
              error, = exc.args
              if not vignore:
                  module.fail_json(msg='Error selecting name from v$asmdiskgroup, Error: %s' % (error.message), changed=False)
              else:
                  add_to_msg("Error while attempting to retrieve parameter %s, %s continuing..." % (vparams[idx],error.message))
                  # if an error just occurred on the select and ignore errors is True skip this and go on
                  continue

            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]

            debugg("param=%s value=%s" % (vparams[idx], vtemp))

            # module.fail_json(msg='processing: vparams[idx]= %s and vtemp = %s' % (vparams[idx], vtemp), changed=False)
            if 'sga_target' == vparams[idx] or 'db_recovery_file_dest_size' == vparams[idx]:
                vtemp = convert_size(float(vtemp),"M")
                ansible_facts[refname].update({ vparams[idx]: vtemp })
            elif 'db_recovery_file_dest' == vparams[idx]:
                ansible_facts[refname].update({ vparams[idx]: vtemp })
            elif 'listener' in vparams[idx]:
                if vtemp is None:
                    ansible_facts[refname].update({ vparams[idx]: 'None' })
                else:
                    head, sep, tail = vtemp.partition('.')
                    ansible_facts[refname].update({vparams[idx]: head})
            elif 'cluster_database' == vparams[idx]:
                debugg(">>>> In PARAM FOR LOOP : %s = %s" % (vparams[idx], vtemp))
                if vtemp.upper() == 'TRUE':
                    is_rac = True
                    ansible_facts[refname].update({'cluster_database': 'True' })
                else:
                    is_rac = False
                    ansible_facts[refname].update({ 'cluster_database': 'False' })
                debugg( ">>>> In PARAM FOR LOOP : is_rac = %s" % (is_rac) )
            else:
                try:
                    ansible_facts[refname][vparams[idx]] = vtemp
                except:
                    debugg("Error while adding %s %s" % (str(vparams[idx]), str(vtemp)))

        debugg("#### PARAMS ADDED ansible_facts[%s]= %s " % (refname, ansible_facts[refname]))

        # select source db version
        try:
            cur.execute('select version from v$instance')
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if not vignore:
                module.fail_json(msg='Error selecting version from v$instance, Error: %s' % (error.message), changed=False)
            else:
                ignore_err_flag = True
                add_to_msg("Error selecting version: %s" % (error.message))

        # if an error just occurred on the select and ignore errors is True skip this and go on
        if not ignore_err_flag:
            dbver =  cur.fetchall()
            retver = dbver[0][0]
            usable_ver = ".".join(retver.split('.')[0:-1])
            ansible_facts[refname].update({'version': usable_ver, 'oracle_version_full': retver, 'major_version': usable_ver.split(".")[0]})
        ignore_err_flag = False

        # select host_name
        try:
            cur.execute('select host_name from v$instance')
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if not vignore:
                module.fail_json(msg='Error selecting host_name from v$instance, Error: %s' % (error.message), changed=False)
            else:
                ignore_err_flag = True
                add_to_msg("Error selecting hostname: %s " % (error.message))
        # if an error just occurred on the select and ignore errors is True skip this and go on
        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname]['host_name'] = vtemp
        ignore_err_flag = False

        # actual db_file count
        try:
          cur.execute('select count(*) from dba_data_files')
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              ignore_err_flag = True
              add_to_msg("Error selecting dba_data_files count: %s " % (error.message))
          else:
              module.fail_json(msg='Error selecting host_name from v$instance, Error: %s' % (error.message), changed=False)
        # if an error just occurred on the select and ignore errors is True skip this and go on
        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update( { 'db_files_actual': vtemp } )
        ignore_err_flag = False

        # Find archivelog mode.
        try:
          cur.execute('select log_mode from v$database')
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              ignore_err_flag = True
              add_to_msg("Error selecting dba_data_files count: %s " % (error.message))
          else:
              module.fail_json(msg='Error selecting log_mode from v$database, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            if vtemp == 'ARCHIVELOG':
              vtemp = 'True'
            else:
              vtemp = 'False'
            ansible_facts[refname].update( { 'archivelog' : vtemp } )
        ignore_err_flag = False

        # Get dbid for active db duplication without target, backup only
        try:
            cur.execute('select dbid from v$database')
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error selecting dbid from v$database, Error: code : %s, message: %s, context: %s' % (error.code, error.message, error.context))
            else:
                module.fail_json(msg='Error selecting dbid from v$database, Error: code : %s, message: %s, context: %s' % (error.code, error.message, error.context), changed=False)

        if ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update( { 'dbid': vtemp } )
        ignore_err_flag = False

        if is_rac:
            # Find ASM diskgroups used by the database
            try:
                cur.execute("select name from v$asm_diskgroup where state='CONNECTED' and name not like '%FRA%'")
            except cx_Oracle.DatabaseError as exc:
                error, = exc.args
                if vignore:
                    ignore_err_flag = True
                    add_to_msg('Error selecting name from v$asmdiskgroup, Error: %s' % (error.message))
                else:
                    module.fail_json(msg='Error selecting name from v$asmdiskgroup, Error: %s' % (error.message), changed=False)

            if not ignore_err_flag:
                vtemp = cur.fetchall()
                vtemp = vtemp[0][0]
                # diskgroups = [row[0] for row in cur.fetchall()]
                ansible_facts[refname].update({ 'diskgroups': vtemp }) #diskgroups
            ignore_err_flag = False
        else:
            ansible_facts[refname].update({ 'diskgroups': 'None' })

        # Open cursors - used in populating dynamic pfiles
        try:
            cur.execute("select value from v$parameter where name = 'open_cursors'")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error selecting value open_cursors, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error selecting value open_cursors, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update( { 'open_cursors': vtemp} )
        ignore_err_flag = False

        # pga_aggregate_target - used in populating dynamic pfiles
        try:
          cur.execute("select value from v$parameter where name = 'pga_aggregate_target'")
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          if vignore:
              add_to_msg('Error selecting value pga_aggregate_target, Error: %s' % (error.message))
              ignore_err_flag = True
          else:
              module.fail_json(msg='Error selecting value pga_aggregate_target, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            vtemp = convert_size(float(vtemp),"M")

            ansible_facts[refname].update({ 'pga_aggregate_target': vtemp })
        ignore_err_flag = False

        # use_large_pages - used in populating dynamic pfiles
        try:
            cur.execute("select value from v$parameter where name = 'use_large_pages'")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                add_to_msg('Error selecting value use_large_pages, Error: %s' % (error.message))
                ignore_err_flag = True
            else:
                module.fail_json(msg='Error selecting value use_large_pages, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update({'use_large_pages': vtemp})
        ignore_err_flag = False

        # Is Block Change Tracking (BCT) enabled or disabled?
        try:
            cur.execute("select status from v$block_change_tracking")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error getting status of BCT, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update({ 'bct_status': vtemp })
        ignore_err_flag = False

        # db_create_online_log_dest_# that aren't null. Needed for utils restore.
        # they will need to be changed in the new database.
        log_dests={}
        try:
            cur.execute("select name,value from v$parameter where replace(value,'+','') in (select name from  v$asm_diskgroup where state = 'CONNECTED' and name not like '%FRA%')")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error selecting version from v$instance, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error selecting version from v$instance, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            try:
              online_logs =  cur.fetchall()
              for create_item,item_value in online_logs:
                log_dests.update({create_item: item_value})
            except cx_Oracle.DatabaseError as exc:
                error, = exc.args
                module.fail_json(msg='Error getting directory info, Error: %s' % (error.message), changed=False)

            ansible_facts[refname].update({ 'log_dest': log_dests })
        ignore_err_flag = False

        # Does the ULNFSA02_DATAPUMP directory exist?
        dirs={}
        try:
            cur.execute("select directory_name, directory_path from dba_directories order by directory_name")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error getting directory info, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error getting directory info, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            try:
                vtemp = cur.fetchall()
                for vdir,vpath in vtemp:
                    dirs.update({vdir: vpath})
            except:
                msg = msg + "%s, %s, %s" % (sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
                msg = msg + ' dir returned meta %s vdir: %s vpath: %s' % (vtemp,vdir,vpath)
                module.fail_json(msg='ERROR: %s' % (msg), changed=False)

            ansible_facts[refname].update( { 'dirs': dirs } )
        ignore_err_flag = False

        # BCT path
        try:
            cur.execute("select filename from v$block_change_tracking")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                add_to_msg('Error getting status of BCT, Error: %s' % (error.message))
                ignore_err_flag = True
            else:
                module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            ansible_facts[refname].update( { 'bct_file': vtemp } )
        ignore_err_flag = True

        meta_msg = ''

        # Get a list of schema owners that aren't Oracle owned schemas
        oracle_owned = "'MDSYS','SQLTXADMIN','PUBLIC','OUTLN','CTXSYS','FLOWS_FILES','SYSTEM','ORACLE_OCM','EXFSYS','APEX_030200','DBSNMP','ORDSYS','ORDPLUGINS','TOAD','SQLTXPLAIN','APPQOSSYS','XDB','ORDDATA','SYS','WMSYS','SI_INFORMTN_SCHEMA','MIGDBA'"
        try:
            cur.execute("select unique(owner) from dba_objects where owner not in (%s)" % (oracle_owned))
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error getting status of BCT, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            owner_list = []
            for own in vtemp:
                owner_list.append(own[0].encode("utf-8"))

            ansible_facts[refname].update( { 'schema_owners': owner_list } )
        ignore_err_flag = False

        # Get default_temp_tablespace and default_permanet_tablespace
        try:
            cur.execute("select property_name,property_value from database_properties where property_name like 'DEFAULT%TABLESPACE'")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error getting status of BCT, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            if cur.rowcount > 0:
                ansible_facts[refname].update({ vtemp[0][0]: vtemp[0][1] } )
                ansible_facts[refname].update({ vtemp[1][0]: vtemp[1][1] } )
        ignore_err_flag = False

        # Get tablespace name like %USER% if one exists:
        try:
            cur.execute("select name from v$tablespace where upper(name) like '%USER%'")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
              ignore_err_flag = True
              add_to_msg('Error getting status of BCT, Error: %s' % (error.message))
            else:
              module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            vtemp = vtemp[0][0]
            if vtemp:
                ansible_facts[refname].update( { 'USER_TABLESPACE': vtemp } )
        ignore_err_flag = False

        if usable_ver[:2] == "12":
            # Get sourcedb home:
            try:
              cur.execute("select SYS_CONTEXT ('USERENV','ORACLE_HOME') from dual")
            except cx_Oracle.DatabaseError as exc:
              error, = exc.args
              if vignore:
                  ignore_err_flag = True
                  add_to_msg('Error getting sourcedb home, Error: %s' % (error.message))
              else:
                  module.fail_json(msg='Error getting sourcedb home, Error: %s' % (error.message), changed=False)

            if not ignore_err_flag:
                vtemp = cur.fetchall()
                vtemp = vtemp[0][0]
                if vtemp:
                    ansible_facts[refname].update( { 'oracle_home': vtemp} )
            ignore_err_flag = False

        # elif usable_ver[:2] == "11":
        #
        #     cmd_str = "DECLARE\n  OH varchar2(100);\nBEGIN\n  dbms_system.get_env('ORACLE_HOME', :OH);\n  dbms_output.put_line(OH);\nEND;"
        #
        #     # Get sourcedb home:
        #     try:
        #       cur.execute(cmd_str)
        #     except cx_Oracle.DatabaseError as exc:
        #       error, = exc.args
        #       module.fail_json(msg='Error getting sourcedb home, Error: %s' % (error.message), changed=False)
        #
        #     vtemp = cur.fetchall()
        #     vtemp = vtemp[0][0]
        #     if vtemp:
        #         ansible_facts[refname]['oracle_home'] = vtemp

        # See if dbainfo user/schema exists
        try:
            cur.execute("select 1 from dba_users where username = 'DBAINFO'")
        except cx_Oracle.DatabaseError as exc:
            error, = exc.args
            if vignore:
                ignore_err_flag = True
                add_to_msg('Error checking existance of dbinfo schema, Error: %s' % (error.message))
            else:
                module.fail_json(msg='Error checking existance of dbinfo schema, Error: %s' % (error.message), changed=False)

        if not ignore_err_flag:
            vtemp = cur.fetchall()
            if cur.rowcount == 0:
                ansible_facts[refname].update({'dbainfo': {'schema_exists': 'False' }} )
                ansible_facts[refname]['dbainfo'].update({'table_exists': 'False' })
            else:
                ansible_facts[refname].update({'dbainfo': {'schema_exists': 'True'}} )

                # if dbainfo schema exists see if dbainfo table exists in the schema
                if cur.rowcount == 1:

                    try:
                        cur.execute("select 1 from dba_objects where owner = 'DBAINFO' and object_name = 'DBAINFO' and object_type ='TABLE' ")
                    except cx_Oracle.DatabaseError as exc:
                        error, = exc.args
                        module.fail_json(msg='Error getting status of BCT, Error: %s' % (error.message), changed=False)

                    vtemp = cur.fetchall()
                    if cur.rowcount == 0:
                        ansible_facts[refname]['dbainfo'].update({'table_exists': 'False' } )
                    else:
                        ansible_facts[refname]['dbainfo'].update({'table_exists': 'True' } )
        ignore_err_flag = False

        try:
            cur.close()
        except cx_Oracle.DatabaseError as exc:
          error, = exc.args
          add_to_msg('Error closing cursor: Error: %s' % (error.message))
          # module.fail_json(msg='Error closing cursor: Error: %s' % (error.message), changed=False)
          pass # No reason to fail at this point

        add_to_msg("Custom module dbfacts, called as refname: %s, succeeded for %s database." % (refname, vdb))

        vchanged="False"

  # if parameters were NULL return informative error.
    else:

        msg="Custom module dbfacts Failed"
        # sourcefacts={}
        if vdbpass is None:
            ansible_facts['systempwd'] = 'missing'
        else:
            ansible_facts['systempwd'] = 'ok'

        if vdb is None:
            ansible_facts['source_db_name'] = 'missing'
        else:
            ansible_facts['source_db_name'] = 'ok'

        if vdbhost is None:
            ansible_facts['source_host'] = 'missing'
        else:
            ansible_facts['source_host'] = 'ok'

        if vrefname is None:
            ansible_facts['refname'] = 'missing'
        else:
            ansible_facts['refname'] = 'ok'

        vchanged="False"

    # print json.dumps( ansible_facts_dict )
    module.exit_json( msg=msg, ansible_facts=ansible_facts , changed=vchanged)

# code to execute if this program is called directly
if __name__ == "__main__":
    main()
