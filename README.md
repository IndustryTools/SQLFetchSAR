# SQLFetchSAR
SQL Fetch Size And References dynamically identifies size and SQL references of all tables in all database including Linked servers 
**Objective** : To get current properties of all tables in all Databases (from sys.databases) except those that are explicitly excluded and under threshold parameteres @SizeCutoffGB or @RowCountCutOff
which include : 
1. **Classification** (regular/Filetable/temporal etc), RowCount, TotalSpaceGB, Creation and Modification Time, Last Access(Seek/Scan/Lookup/update)
2. **References**: in which SQL objects(Stored Proc/Function/views/triggers) that table is referenced. It suggest soft reference(table name mentioned in 
comments/dynamic sql + direct reference) for objects in same instance and direct reference for objects in different instance/DBserver(SP/Func/view etc
is dependent on table on different instance/DBserver which require further correlation) 
You can use this output and dump in table to analyze  historical data(increase/decrease in size and reference count) which offers granularity at table level instead of DB level
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
SQL User running this script need to have following permissions (check output of below query, SQL login must have public access on master DB with guest account enabled): 
SELECT name,DATABASEPROPERTYEX(name, 'UserAccess'),has_dbaccess(name),* FROM master.sys.databases
WHERE DATABASEPROPERTYEX(name, 'UserAccess') = 'MULTI_USER' AND has_dbaccess(name) = 1
SELECT HAS_PERMS_BY_NAME(null, null, 'VIEW SERVER STATE');  /*Must be 1*/
SELECT HAS_PERMS_BY_NAME('master', 'DATABASE', 'ANY');  /*Must be 1, instead of master DB, use your DB name*/
