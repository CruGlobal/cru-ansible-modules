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

You also need to edit your ansible.cfg file and make the following entry:
```
local_user=myusername
```
note: local_user found by going to a terminal and typing 'whoami'
this value can also be passed via command line:
```
--extra-vars="local_user=myusername"
```
