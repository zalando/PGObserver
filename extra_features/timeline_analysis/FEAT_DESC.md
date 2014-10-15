Timeline analyzis
===

Feature aims to spot substantial sproc runtimes slowdowns and sending Cron notification emails on findings.

Search patterns:
  Each hour, the total and avg runtime of a every function is compared to the average of past weeks (4 by default), and if substanital jump in run time is dedected, it is reported. To avoid excessive noise, only new extreme findings are reported (e.g. the function's run time is the highest, or 2nd highest, in the past number of days).

NB! Not really a real-time solution (1h chunks are analyzed)

 
Setup
-----

* Run the "sql" files on the PgObserver database
* Set up the Crons



