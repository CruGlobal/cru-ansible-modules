# cru-ansible-modules
Custom Cru Ansible Modules.


## Oracle Modules
The modules can be run as a pre-task to gather information about the host(s) at the beginning of a playbook.


### orafacts
Gather facts against the hosts you're running against.

```
- name: Configure oracle
  hosts: oracle

  pre_tasks:
    - name: Gather Oracle facts on destination servers
      orafacts:
      register: ora_facts

    - debug: msg="{{ ora_facts }}"
```

### sourcefacts

requirements: `cx_Oracle`

Get Oracle Database facts from a remote database (ie, a database not in the group being operated on).

```
- name: Configure oracle
  hosts: oracle
  
  pre_tasks:

    - local_action: sourcefacts
        systempwd="{{ database_passwords[source_db_name].system }}"
        source_db_name="{{ source_db_name }}"
        source_host="{{ source_host }}"
      become_user: "{{ local_user }}"
      register: src_facts

    - debug: msg="{{ src_facts }}"
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
