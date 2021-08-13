# crupac
Cru custom Python code for modules and Utils a PyQt5 GUI wrapper for our Ansible playbooks
and other funcationality.

### CruPac ( [Cru] Python [Pac]kage )
This is a package of some of the most common Python code used throughout Cru's custom
Python programs.


|Module Name | Purpose                                                         |
|------------|-----------------------------------------------------------------|
|cx.py       | Python class used to make Oracle database connections using cx_Oracle |
|dbug.py     | Custom debugging fx's used throught the other modules and code         |
|utils.py    | Common custom fx's written in Python used thoughout Cru's custom code  |

### To install crupac for use locally:
It is recommended to install using development mode in case a Python module of the same name already exists. Development package directories are searched last and will therefore not overwrite or replace existing modules of the same name.

To install ```crupac``` in development mode: navigate to the cru-ansible-modules root directory and run:

```
  pip install -e ./crupac
```

This will make the contents of the crupac package available site wide.
