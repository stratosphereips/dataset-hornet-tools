# zeek_purge_ip_from_data.sh

This utility reads from a folder of Zeek logs (.gz) and processes them to remove a given IP. The script performs various safety checks, including creating backups of each log before modifying them.

```bash
:~$ bash zeek_purge_ip_from_data.sh
Usage: zeek_purge_ip_from_data.sh -i IP_TO_REMOVE -p PATH_TO_LOGS
:~$ # bash zeek_purge_ip_from_data.sh -i 1.1.1.1 -p /opt/zeek/logs/2024-05-31/
```
