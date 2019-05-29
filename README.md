# cru-ansible-modules
Custom Cru Ansible Modules.


## Oracle Modules
The modules can be run as a pre-task to gather Oracle related information at the beginning of a playbook.


### orafacts
Gather Oracle facts on the host(s) you're running your playbook against.

```
- name: Configure oracle
  hosts: oracle

  pre_tasks:
    - name: Gather Oracle facts on destination servers
      orafacts:

    - debug: msg="{{ orafacts }}"   # only needed to see return results,
                                     # not needed to use them

  Sample returned values:
    "orafacts": {
              "+ASM1": {
                  "home": " /app/12.1.0.2/grid/",
                  "pid": "15696",
                  "status": "running",
                  "version": "12.1.0.2"
              },
              "11g": {
                  "db_version": "11.2.0.4.0",
                  "home": "/app/oracle/11.2.0.4/dbhome_1",
                  "opatch_version": " 11.2.0.3.12",
                  "srvctl_version": "11.2.0.4.0"
              },

  To use returned values:
    "{{ orafacts['11g']['home'] }}"
  returns the value:
    "/app/oracle/11.2.0.4/dbhome_1"

```

### sourcefacts

requirements: `cx_Oracle`

Get Oracle Database facts from a remote (source) database (ie, a database not in the group being operated on).
Used when cloning or refreshing a database and the source database is located on a different host/environment.
i.e. running a clone in the test environment and the source is located in the production environment.

```
- name: Configure oracle
  hosts: oracle

  pre_tasks:
    - local_action: sourcefacts
        systempwd="{{ database_passwords[source_db_name].system }}"
        source_db_name="{{ source_db_name }}"
        source_host="{{ source_host }}"
      become_user: "{{ local_user }}"

    - debug: msg="{{ sourcefacts }}"

  Sample returned values:
    "sourcefacts": {
        "archivelog": "True",
        "bct_path": null,
        "bct_status": "DISABLED",
        "compatible": "11.2.0.4",
        "db_block_size": "8192",
        "db_recovery_file_dest": "FRA",
        "db_recovery_file_dest_size": "153600M",
        "db_unique_name": "jfprod",
        "dbid": 3182794939,
        "diagnostic_dest": "/app/oracle",
        "diskgroups": "DATA2",
        "host_name": "plorad01.ccci.org",
        "open_cursors": "500",
        "oracle_version": "11.2.0.4",
        "oracle_version_full": "11.2.0.4.0",
        "pga_aggregate_target": "8589934592",
        "remote_listener": "prod-scan",
        "sga_target": "10240M",
        "use_large_pages": "ONLY"
    }

  To use returned values:
      "{{ sourcefacts['11g']['compatible'] }}"
  returns the value:
      "11.2.0.4"

```

### sysdbafacts

Requirement: `cx_Oracle`

This module connects as sysdba to a database. This is helpful when the database is in restricted access mode like during duplication, startup mount etc.

```
    # if cloning a database and source database information is desired
    - local_action:
        module: sysdbafacts
        syspwd: "{{ database_passwords[source_db_name].sys }}"
        db_name: "{{ source_db_name }}"
        host: "{{ source_host }}"
        pfile: "{{ /complete/path/and/filename.ora }}" (1)
        oracle_home: "{{ oracle_home }}" (2)
        refname: "{{ refname_str }} (3)"
        ignore: True (4)
      become_user: "{{ utils_local_user }}"
      register: sys_facts

      (1) pfile   - optional. If provided a pfile will be created to the specified directory/filename

      (2) refname - name used in Ansible to reference these facts ( i.e. sourcefacts, destfacts, sysdbafacts )

      (3) ignore - True will do a non-fatal exit of the module. 
                   False will cause the module to stop the play execution when encountering an error.
      
        
```

### rmanfacts 

Gather RMAN spfile backup facts for the source database.
Used during a database restore.

Notes: sourcefacts must run prior to rmanfacts or the user
       must pass values required to run rmanfacts.

       Only retrieval of spfile backup info is currently supported.


```
- name: Configure oracle
  hosts: oracle
  pre_tasks:
    - name: Gather RMAN spfile backup facts for source database
      rmanfacts:
        rman_pwd: "{{ database_passwords['rman'].rco }}"
        dbid: "{{ sourcefacts['dbid'] }}"
        source_db: "{{ source_db_name }}"
        bu_type: spfile
        ora_home: "{{ oracle_home }}"
        staging_path: "{{ oracle_stage }}"

    - debug: msg="{{ rmanfacts }}"

  Sample returned values:
  "rmanfacts": {
      "spfile": {
          "1": {
              "backup_date": "04-JAN-2018 04:34:04",
              "compressed": "YES",
              "copies": "1",
              "device": "DISK",
              "key": "875837",
              "level": "F",
              "pieces": "1",
              "status": "A",
              "tag": "DD",
              "type": "B"
          },

  To use returned values:
    "{{ rmanfacts['spfile']['1']['backup_date'] }}"
  returns the value:
    "04-JAN-2018 04:34:04"

```

### srvctl - srvctl wrapper

Module to interface with srvctl 

Notes: 

  (1) Use when master_node else it may try to execute on all nodes simultaneously.
  
  (2) It's possible to start instance nomount, mount etc. but not to
      alter instance mount, or open. To open the instance using the srvctl module
      you must stop the instance then start instance mount, or start instance open.
      It is possible to "sqlplus> alter database mount" or "alter database open".
      The status change will then be reflected in crsstat.
      
```
    # To start | stop a database or instance from Ansible using srvctl
    - name: start database
      srvctl:
        db: {{ dest_db_name }}
        cmd: stop
        obj: instance
        inst: 2
        stopt: immediate
        param: force
        ttw: 7
      when: master_node                 Note: (1)

    values:
       db: database name
      cmd: [ start | stop ]
      obj: [ database | instance ]
     inst: [ valid instance number ]
    stopt: (stop options): [ normal | immediate | abort ]
           (start options): [ open | mount | nomount | restrict | read only | read write | write ]
    param: [ eval | force | verbose ]
      ttw: time to wait (in min) for status change after executing the command. Default 5.

    Notes:
        (1) Use when master_node else it may try to execute on all nodes simultaneously.

        (2) It's possible to start instance nomount, mount etc. but not to
            alter instance mount, or open. To open the instance using the srvctl module
            you must stop the instance then start instance mount, or start instance open.
            It is possible to "sqlplus> alter database mount" or "alter database open".
            The status change will then be reflected in crsstat.
```

### compver - compare versions

Module to compare Oracle database versions and return the lesser version and whether it was required ( True / False )
for the datapump export/import operation.

This module was needed for automating datapump transfers between dissimilar database versions.

```
    Ansible playbook call:
    - name: Compare database versions
      local_action:
        module: compver
        ver_db1: "{{ sourcefacts['version'] }}"
        ver_db2: "{{ destfacts['version'] }}"
      become_user: "{{ utils_local_user }}"
      register: ver_comp
      when: master_node 
        
    returns:
    
      compver{
        required: true
        version: 11.2.0.4
      }
      
    The results are referred to using the reference name 'compver' in the jinja2 templated par file:
    
    {% if compver is defined and compver.required.lower() == 'true' %}
    version={{ compver['version'] }}
    {% endif %}
    
```      

### dbfacts - Database facts

Requirement: `cx_Oracle`

Module returns internal database settings and parameters, such as 
    v$parameters, 
    database version, 
    host name, 
    archive log mode, 
    database id, 
    ASM diskgroup names and whether the diskgroup is connected to the database
    Open cursors setting
    Block Change Tracking (enabled/disabled)
    pga_aggregate_target, etc.

```
    Call from Ansible playbook: 
    - name: Get source Oracle database information
      local_action:
        module: dbfacts
        systempwd: "{{ database_passwords[source_db_name].system }}"
        db_name: "{{ source_db_name }}"
        host: "{{ source_host }}"
        refname: sourcefacts
        ignore: True
      become_user: "{{ utils_local_user }}"
      register: src_facts
      when: source_host is defined
      tags: always
      
    requires cx_Oracle
    
    permission on the local system is required, so become user would be set to the linux value of 'whoami'.
    in the above example utils_local_user variable = 'whoami'.
    
    refname parameter allows you to change the reference name of the Ansible facts returned. Default reference name is    'dbfacts'
    
    
```

### finrest - Finish Restore

Requirement: `cx_Oracle`

Module Used with Cru's custom Ansible Oracle database automated restore to finish a restore.  

Once the RMAN portion of the restore is complete, and restoring to point in time, this module opens a SQL prompt to execute:
    RECOVER DATABASE UNTIL CANCEL
    CANCEL
    ALTER DATABASE OPEN RESETLOGS
    SHUTDOWN IMMEDIATE
    EXIT
It then returns control to the Ansible playbook to finish RAC'ing the database, reset passwords etc.

```

  Call from Ansible playbook: 
  - name: Finish SQL part of database restore
    finrest:
      db_name: "{{ dest_db_name }}"
    when: master_node
    
```

### lsnr_up - listener up

Module monitors listener control status waiting for a database to register with the local listener.
Used when cloning a database after startup nomount command is issued. This module slows playbook execution down 
so the following tasks don't fail because the database isn't ready.

```
  Call from Ansible playbook: 
  - name: wait for database to register with local listener
    lsnr_up:
      db_name: "{{ db_name }}"
      lsnr_entries: 2
      ttw: 5
    when: master_node
       
    lsnr_entries - the number of entries to expect to find in 'lsnrctl status' for the database. 
                   If listener.ora has an entry 2 should be expected. 
                   
     ttw         - Time to Wait (ttw) is the amount of time to wait ( in minutes ) for the entries to appear before failing.
     
```

### mkalias - make alias (ASM)

Module to create an alias in the ASM diskgroup for the spfile

```
    Call from Ansible playbook: 
    - name: Map new alias to spfile
      mkalias:
        db_name: "{{ db_name }}"
        asm_dg: "{{ asm_dg_name }}"
      when: master_node
      
```

### redologs - Redo Logs (FLUSH or RESIZE)

This module is used to flush redo logs prior to taking a backup, or to resize redo logs. 

FLUSH:
Regarless of what supposed to happen when a database is shutdown for backup, we've experienced times when all archivelogs were not written to disk. By running this before a hot or cold backup, this will ensure all archivelogs are flushed to disk before the backup.

The module looks at the current state and executes 'ALTER SYSTEM ARCHIVE LOG CURRENT' commands until all archivelogs have cycled and flushed their contents to disk.

RESIZE:
Resizes redo logs to whatever size is provided in the parameters.

```
  Call from Ansible playbook: 
  - name: Flush redo logs
    local_action:
        module: redologs
        connect_as: system
        system_password: "{{ database_passwords[dest_db_name].system }}"
        dest_db: "{{ dest_db_name }}"
        dest_host: "{{ dest_host }}"
        function: flush
        size:
        units:
        ignore: true
        refname:
    become_user: "{{ local_user }}"
    register: redo_run

  Call from Ansible playbook: 
  - name: Resize redo logs
    local_action:
        module: redologs
        system_password: "{{ database_passwords[dest_db_name].system }}"
        dest_db: "{{ dest_db_name }}"
        dest_host: "{{ dest_host }}"
        function: resize
        size: 500
        units: m
        ignore: false
        refname:
    become_user: "{{ local_user }}"
    register: redo_run
    
    
    size and units are not required for "flush" but are for resize.
    
    units are single letter: k (kilobytes), m (megabytes), g (gigabytes) etc.
    ignore - tells the module whether to fail on error and raise it or pass on error
             and continue with the play. Default is to fail.
```


### rmandbid - RMAN Database ID

Requirement: `cx_Oracle`

Module queries the RMAN database to retrieve a databases' id (dbid)

```
    Call from Ansible playbook: 
    # Retrieve the dbid of a given database.
    - local_action:
        module: rcatdbid
        systempwd: "{{ database_passwords['cat'].system }}"
        cdb: "cat"
        pdb: "catcdb"
        schema_owner: rco
        host: "{{ source_host }}"
        refname: your_reference_name
      become_user: "{{ local_user }}"

    Notes:
        refname (optional) - any name you want to use to referene the data later in the play
                             defualt refname is 'rmandbid'
                             
```

### sectblcnt - Security Table Count

Requirement: `cx_Oracle`

Security Table Count module - or any table count. This module takes a list of tables and their count to verify they exist in a given schema. Used to export security tables prior to a refresh.

```

Note: This module could ensure the existance of any tables by providing the schema name in the ps_admin parameter and the list of tables for 'security_table_list' along with the count 'num_sec_tables'

Predefined variables:
    ps_admin: bob
    num_sec_tables: 2
    security_table_list: "PSACCESSPROFILE,PSOPRDEFN"

    Call from Ansible playbook: 
    - local_action:
        module: psadmsectblcnt
        ps_admin: "{{ ps_admin }}" (1)
        table_list: "{{ security_table_list }}" (1)
        systempwd: "{{ database_passwords[source_db_name].system }}"
        db_name: "{{ dest_db_name }}"
        host: "{{ dest_host }}"
        refname: "{{ refname_str }}" (2)
        ignore: True (3)
      become: yes
      become_user: "{{ utils_local_user }}"
      register: sec_tbl_count
      when: master_node

      (1) ps_admin, table_list and num_sec_tables - are defined in
          vars/utils/utils_env.yml
          num_sec_tables is used after the count is obtained to fail if
          the count is less than expected.
          Fail when:
          - sectblcount[ps_admin]['security_table_count'] < num_sec_tables

      (2) refname - can be defined to refer to the output later. The default
          is 'sectblcount' ( see above Fail when statement )
          but the user can define anything.

      (3) ignore - (connection errors) is optional. If you know the source
          database may be down set ignore: True. If connection to the
          source database fails the module will not throw a fatal error
          to stop the play and continue. However, not if the result is critical.
          
          
```

### setcntrlfile - Set Control File

Set controlfile module - `Obsolete`

This module would startup nomount a down database and set the controlfile based on what was in ASM diskgroup. 
Obsolete because at times there were more than one controlfile in the ASM diskgroup and it was impossible to tell which was current.

```
    # this will look in ASM for new control files and then
    # startup nomount a down database and set the control_files parameter
    # in the database. i.e control_files = +DATA3/stgdb/controlfile/current.404.989162475
    - name: Set control_files parameter in db with new controlfile name.
      setcntrlfile:
        db_name: "{{ dest_db_name }}"
        db_home: "{{ oracle_home }}"
        asm_dg: "{{ database_parameters[dest_db_name].asm_dg_name }}"
      when: master_node

    Notes:
        The ASM diskgroup ( asm_dg_name ) the database is in can be entered with or without the + ( +DATA3 or DATA3 )
        The database name ( db_name ) can be entered with or without the instance number ( tstdb or tstdb1 )
  
```

