# Join Data Check

This python script can be used in conjunction with the [Muscat](https://github.com/IntersectAustralia/muscat) 
application to ensure linking data exists between sources and authorities. It inserts linking data if it doesn’t exist 
in the configured muscat DB join tables.

To use the script simply run python2 on it. For example, `python2 ~/muscat/join_check/join_data_check.py` assuming it 
is located in a directory called `join_check` in the muscat root directory.

The first time the script runs it will create and write to a log file called `join_check.log` which will be appended 
to thereafter.

Before the script can be used it will need to be configured with the credentials of the Muscat application database
and ideally a separate dedicated user. For example,
```
host = "localhost"
user =  "join_check"
pword = "python"
database = "muscat"
``` 
