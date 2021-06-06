/*
Author: Saurabh SHarma
Created Date : 5-Jun-2021
Objective : To get current properties of all tables in all Databases (from sys.databases) except those that are explicitly excluded and under threshold parameteres @SizeCutoffGB or @RowCountCutOff
			which include : 1. Classification (regular/Filetable/temporal etc), RowCount, TotalSpaceGB, Creation and Modification Time, Last Access(Seek/Scan/Lookup/update)
							2. References: in which SQL objects(Stored Proc/Function/views/triggers) that table is referenced. It suggest soft reference(table name mentioned in 
											comments/dynamic sql + direct reference) for objects in same instance and direct reference for objects in different instance/DBserver(SP/Func/view etc
											is dependent on table on different instance/DBserver which require further correlation) 
You can use this output and dump in table to analyze  historical data(increase/decrease in size and reference count) which offers granularity at table level instead of DB level
*/

BEGIN TRY
SET NOCOUNT ON;
SET DEADLOCK_PRIORITY LOW;
------------------------ Functional Parameter-----------------------------------
	DECLARE @SizeCutoffGB NUMERIC(10,1) = 1 
	DECLARE @RowCountCutOff INT = 1	
	DECLARE @DBNameToBeExcluded VARCHAR(MAX) = 'master,tempdb,model,msdb'
	DECLARE @DBNameToBeIncluded VARCHAR(MAX) --= ''
	DECLARE @IsRemoteCaptureAllowed TINYINT = 1
	DECLARE @Debug TINYINT = 1

------------------------ Internal Parameter-----------------------------------
	DECLARE @RowNum INT = 1
	DECLARE @SQLText VARCHAR(MAX) = ''	
	DECLARE @RefCount VARCHAR(MAX) = ''
	DECLARE @CurrDBName SYSNAME
	DECLARE @ServerLastRebootTime SMALLDATETIME	

	IF OBJECT_ID('tempdb..#TableProperties') IS NOT NULL DROP TABLE #TableProperties
	CREATE TABLE #TableProperties(ID INT IDENTITY(1,1) NOT NULL, CurrentUser VARCHAR(200) NOT NULL, HostName  VARCHAR(200) NOT NULL
			, object_id BIGINT NOT NULL, ServerType VARCHAR(200) NOT NULL, Server VARCHAR(200) NOT NULL , DatabaseName VARCHAR(200) NOT NULL , SchemaName VARCHAR(200) NOT NULL , ObjectName VARCHAR(200) NOT NULL , Classification VARCHAR(200) NOT NULL
			,RowCounts BIGINT NOT NULL, TotalSpaceGB DECIMAL(15,2) NOT NULL, UsedSpaceGB DECIMAL(15,2) NOT NULL, UnusedSpaceGB DECIMAL(15,2) NOT NULL, TableCreationTime DATETIME2 NULL
			, TableModificationTime DATETIME2 NULL, ServerLastRebootTime SMALLDATETIME NOT NULL, Last_User_Action DATETIME2, last_user_seek DATETIME2, last_user_scan DATETIME2, last_user_lookup DATETIME2
			, last_user_update DATETIME2, DBNameToBeExcluded VARCHAR(MAX), DBNameToBeIncluded VARCHAR(MAX), SizeCutoffGB DECIMAL(15,2) , RowCountCutOff BIGINT , JSONRefCount VARCHAR(MAX), IS_JSON TINYINT
			, InsertedDateTime DATETIME2 DEFAULT(GETDATE()) NOT NULL)

----------------------------------------------------------------------Validations----------------------------------------------------------------------------------------
	IF @SizeCutoffGB IS NULL OR @SizeCutoffGB <= 0
	BEGIN
		RAISERROR('@SizeCutoffGB cannot be less than 0',16,1);
		RETURN
	END
	IF @RowCountCutOff IS NULL OR @RowCountCutOff <= 0
	BEGIN
		RAISERROR('@RowCountCutOff cannot be less than 0',16,1);
		RETURN
	END	

/*TODO	*/
--	select name,DATABASEPROPERTYEX(name, 'UserAccess'),has_dbaccess(name),* from master.sys.databases
--WHERE --Name = 'userDBName' AND 
--DATABASEPROPERTYEX(name, 'UserAccess') = 'MULTI_USER' AND has_dbaccess(name) = 1
--SELECT HAS_PERMS_BY_NAME('userDBName', 'DATABASE', 'ANY');  
--SELECT HAS_PERMS_BY_NAME(null, null, 'VIEW SERVER STATE');  


	IF EXISTS(SELECT * FROM (SELECT LTRIM(RTRIM(value)) AS UserDBName  FROM STRING_SPLIT(@DBNameToBeExcluded, ',')  WHERE RTRIM(value) <> '' ) A LEFT JOIN
					(SELECT D.name FROM master.sys.databases D INNER JOIN (SELECT LTRIM(RTRIM(value)) AS UserDBName  FROM STRING_SPLIT(@DBNameToBeExcluded, ',')  WHERE RTRIM(value) <> '') D1 ON D.name = D1.UserDBName
					 ) B
					ON A.UserDBName = B.name WHERE B.name IS NULL) 
	BEGIN
		DECLARE @ErrorText1 VARCHAR(1000) = ''
		SELECT TOP 1 @ErrorText1 = A.UserDBName + ' not a valid Database Name in DBNameToBe[Excluded] list' FROM (SELECT LTRIM(RTRIM(value)) AS UserDBName  FROM STRING_SPLIT(@DBNameToBeExcluded, ',')  WHERE RTRIM(value) <> '' ) A LEFT JOIN
				(SELECT D.name FROM master.sys.databases D INNER JOIN (SELECT LTRIM(RTRIM(value)) AS UserDBName  FROM STRING_SPLIT(@DBNameToBeExcluded, ',')  WHERE RTRIM(value) <> '') D1 ON D.name = D1.UserDBName
				) B
				ON A.UserDBName = B.name WHERE B.name IS NULL
		RAISERROR(@ErrorText1,16,1);
		RETURN
	END
	IF EXISTS(SELECT * FROM (SELECT LTRIM(RTRIM(value)) AS UserDBName  FROM STRING_SPLIT(@DBNameToBeIncluded, ',')  WHERE RTRIM(value) <> '' ) A LEFT JOIN
					(SELECT D.name FROM master.sys.databases D INNER JOIN (SELECT LTRIM(RTRIM(value)) AS UserDBName  FROM STRING_SPLIT(@DBNameToBeIncluded, ',')  WHERE RTRIM(value) <> '') D1 ON D.name = D1.UserDBName
					) B
					ON A.UserDBName = B.name WHERE B.name IS NULL) 
	BEGIN
		DECLARE @ErrorText2 VARCHAR(1000) = ''
		SELECT TOP 1 @ErrorText2 = A.UserDBName + ' not a valid Database Name in DBNameToBe[Included] list' FROM (SELECT LTRIM(RTRIM(value)) AS UserDBName  FROM STRING_SPLIT(@DBNameToBeIncluded, ',')  WHERE RTRIM(value) <> '' ) A LEFT JOIN
				(SELECT D.name FROM master.sys.databases D INNER JOIN (SELECT LTRIM(RTRIM(value)) AS UserDBName  FROM STRING_SPLIT(@DBNameToBeIncluded, ',')  WHERE RTRIM(value) <> '') D1 ON D.name = D1.UserDBName
				) B
				ON A.UserDBName = B.name WHERE B.name IS NULL
		RAISERROR(@ErrorText2,16,1);
		RETURN
	END
------------------------------------------------------------------------------------------------------------------------------------------------------------------
	SELECT @ServerLastRebootTime = sqlserver_start_time FROM sys.dm_os_sys_info /*VIEW SERVER STATE required*/

	IF OBJECT_ID('tempdb..#DB') IS NOT NULL DROP TABLE #DB
	SELECT name AS DBName,0 AS IsPickedUp INTO #DB 
	FROM master.sys.databases 
	WHERE (ISNULL(@DBNameToBeExcluded,'') = ''  OR name NOT IN (SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@DBNameToBeExcluded, ',')  WHERE RTRIM(value) <> ''))
	AND (ISNULL(@DBNameToBeIncluded,'') = ''  OR name IN (SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@DBNameToBeIncluded, ',')  WHERE RTRIM(value) <> '') )
	ORDER BY database_id
	
	IF (ISNULL(@DBNameToBeIncluded,'') = '')
		SELECT @DBNameToBeIncluded = COALESCE(@DBNameToBeIncluded + ',', '') +  CONVERT(VARCHAR(200), DBName) FROM #DB

	IF @Debug = 1
	BEGIN
		DECLARE @JSONtmp VARCHAR(MAX) = (SELECT * FROM #DB FOR JSON AUTO)
		PRINT  '@JSONtmp:' + @JSONtmp
		PRINT '@DBNameToBeExcluded: ' + @DBNameToBeExcluded
		PRINT '@DBNameToBeIncluded: ' + @DBNameToBeIncluded
		PRINT @ServerLastRebootTime 
		PRINT '------------------------------------------------------------------------------------------------------'
	END		

	WHILE EXISTS(SELECT TOP 1 1 FROM #DB WHERE IsPickedUp = 0)
	BEGIN
		SELECT @CurrDBName  = DBName FROM #DB WHERE IsPickedUp = 0
	
		IF @Debug = 1
		BEGIN
			PRINT CONVERT(VARCHAR,@RowNum) + ') ' + @CurrDBName
			SET @RowNum = @RowNum + 1
		END
	--Local Object		
		SELECT @SQLText = '
					;WITH CTE AS(
						SELECT 
							t.OBJECT_ID,
							s.Name AS SchemaName,
							t.NAME AS ObjectName,
							MAX(p.rows) AS RowCounts,	
							CONVERT(DECIMAL(15,2),(SUM(a.total_pages) * 8)/1048576.0) AS TotalSpaceGB,  
							CONVERT(DECIMAL(15,2),(SUM(a.used_pages) * 8)/1048576.0) AS UsedSpaceGB,  
							CONVERT(DECIMAL(15,2),((SUM(a.total_pages) - SUM(a.used_pages)) * 8)/1048576.0) AS UnusedSpaceGB,
							CASE WHEN MAX(CONVERT(INT, is_filetable)) = 1 THEN ''Filetable''
									WHEN MAX(CONVERT(INT, is_external)) = 1 then ''ExternalTable''
									WHEN MAX(CONVERT(INT, is_memory_optimized)) = 1 then ''MemoryOptimized''
									WHEN MAX(CONVERT(INT, temporal_type)) = 2 THEN ''SystemVersionedTable''
									WHEN MAX(CONVERT(INT, temporal_type)) = 1 THEN ''HistoryTable''
									ELSE ''RegularTable'' 
							END AS Classification,
							MAX(t.create_date) AS TableCreationTime,
							MAX(t.modify_date) AS TableModificationTime
						FROM 
							[' + @CurrDBName + '].sys.tables t
						INNER JOIN 
							[' + @CurrDBName + '].sys.schemas s ON s.schema_id = t.schema_id
						INNER JOIN      
							[' + @CurrDBName + '].sys.indexes i ON t.OBJECT_ID = i.object_id
						INNER JOIN 
							[' + @CurrDBName + '].sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
						INNER JOIN 
							[' + @CurrDBName + '].sys.allocation_units a ON p.partition_id = a.container_id
						WHERE 
							t.NAME NOT LIKE ''dt%'' 
							AND t.is_ms_shipped = 0
							AND i.OBJECT_ID > 255 	
						GROUP BY 
							t.OBJECT_ID,t.Name, s.Name		
						) '
		SELECT @RefCount = '(SELECT (SELECT '+ 
									(SELECT STUFF((
										SELECT ', ' + CONVERT(VARCHAR(MAX), List.Col) FROM (SELECT 'A'+CONVERT(VARCHAR,ROW_NUMBER() OVER (ORDER BY DBName)) + '.[JSONRefCount] AS [''' + DBName + ''']' AS Col FROM #DB) List
										FOR XML PATH('')
									), 1, 2, ''))
										+
							'  FOR JSON path) AS ''' + @@SERVERNAME + ''' FOR JSON path) AS JSONRefCount ' 
		SELECT @SQLText = COALESCE(@SQLText, '','') +	
				'SELECT 
					CurrentUser, HostName, object_id, ServerType,Server, DatabaseName, SchemaName, ObjectName, 
					Classification, RowCounts, TotalSpaceGB, UsedSpaceGB, UnusedSpaceGB, 
					TableCreationTime, TableModificationTime, ServerLastRebootTime, Last_User_Action, last_user_seek, 
					last_user_scan, last_user_lookup, last_user_update, DBNameToBeExcluded, DBNameToBeIncluded, SizeCutoffGB, RowCountCutOff, 
					JSONRefCount, ISJSON(JSONRefCount) IS_JSON
				FROM(
					SELECT  F.*, ' + @RefCount +
					'FROM (
							SELECT  ''' + SYSTEM_USER + ''' AS CurrentUser , ''' + HOST_NAME() + ''' AS HostName, t.object_id,''Local'' AS ServerType, ''' + @@SERVERNAME + ''' AS Server,  ''' + @CurrDBName + ''' AS DatabaseName,T.SchemaName, T.ObjectName, T.RowCounts, TotalSpaceGB, T.UsedSpaceGB, T.UnusedSpaceGB, T.Classification, T.TableCreationTime, T.TableModificationTime
							, ''' + CONVERT(VARCHAR,@ServerLastRebootTime) + ''' ServerLastRebootTime, COALESCE(last_user_seek,last_user_scan,last_user_lookup,last_user_update) Last_User_Action,last_user_seek,last_user_scan,last_user_lookup,last_user_update 		
							, ' + CASE WHEN ISNULL(@DBNameToBeExcluded ,'') = '' THEN '''ALL''' ELSE ' ''' + @DBNameToBeExcluded + ''' ' END + ' AS DBNameToBeExcluded
							, ' + CASE WHEN ISNULL(@DBNameToBeIncluded ,'') = '' THEN '''ALL''' ELSE ' ''' + @DBNameToBeIncluded + ''' ' END + ' AS DBNameToBeIncluded		
							, ''' + CONVERT(VARCHAR,ISNULL(@SizeCutoffGB,0)) + ''' AS SizeCutoffGB , ''' + CONVERT(VARCHAR, ISNULL(@RowCountCutOff,0)) + ''' AS RowCountCutOff
							FROM CTE t LEFT JOIN 
							(SELECT object_id,MAX(last_user_seek) last_user_seek, MAX(last_user_scan) last_user_scan, MAX(last_user_lookup) last_user_lookup, MAX(last_user_update) last_user_update  
							FROM ' + @CurrDBName + '.sys.dm_db_index_usage_stats GROUP BY object_id)i ON t.object_id = i.object_id
							WHERE  T.TotalSpaceGB > '+ CONVERT(VARCHAR,ISNULL(@SizeCutoffGB,0))+' OR T.RowCounts > '+ CONVERT(VARCHAR,ISNULL(@RowCountCutOff,0))+'
						) F '

		SELECT @SQLText = COALESCE(@SQLText, '','') + 
						'CROSS APPLY (SELECT ( SELECT ''Local'' AS Reference, O.type_desc AS ReferenceType, COUNT(DISTINCT O.name) AS [ReferenceCount] 
											   FROM [' + DBName + '].sys.objects o WITH(NOLOCK) INNER JOIN [' + DBName + '].sys.syscomments c WITH(NOLOCK) on o.object_id = c.id 
											   WHERE ' + CASE WHEN DBName = @CurrDBName THEN '(c.text COLLATE DATABASE_DEFAULT  LIKE ''%[^a-z]'' + f.ObjectName COLLATE DATABASE_DEFAULT + ''[^a-z]%'') OR (c.text COLLATE DATABASE_DEFAULT  LIKE ''%'' + F.DatabaseName COLLATE DATABASE_DEFAULT  + ''%[^a-z]'' + f.ObjectName COLLATE DATABASE_DEFAULT + ''[^a-z]%'')GROUP BY O.type_desc FOR JSON PATH) AS JSONRefCount)'
															  ELSE '(c.text COLLATE DATABASE_DEFAULT LIKE ''%'' + F.DatabaseName COLLATE DATABASE_DEFAULT  + ''%[^a-z]'' + f.ObjectName COLLATE DATABASE_DEFAULT  + ''[^a-z]%'') GROUP BY O.type_desc FOR JSON PATH) AS JSONRefCount)' 
														  END +
				'A'+CONVERT(VARCHAR,ROW_NUMBER() OVER (ORDER BY DBName)) + ' ' from #DB

		SET @SQLText = @SQLText + ') A' 		
				

		IF @Debug = 1
		BEGIN
			PRINT 'Local : @SQLText: ' + CONVERT(VARCHAR, LEN(@SQLText))
			PRINT '--------------------------------------'
			PRINT @RefCount
			PRINT '--------------------------------------'
			PRINT @SQLText		
			PRINT '------------------------------------------------------------------------------------------------------'
		END

		INSERT INTO #TableProperties(CurrentUser, HostName, object_id, ServerType, Server, DatabaseName, SchemaName, ObjectName,Classification, RowCounts, TotalSpaceGB, UsedSpaceGB, UnusedSpaceGB, 
								    TableCreationTime, TableModificationTime, ServerLastRebootTime, Last_User_Action, last_user_seek, 
									last_user_scan, last_user_lookup, last_user_update, DBNameToBeExcluded, DBNameToBeIncluded, SizeCutoffGB, RowCountCutOff, 
									JSONRefCount, IS_JSON)
		EXEC(@SQLText)

		IF @IsRemoteCaptureAllowed  = 1
		BEGIN
		--Linked Object
			SELECT @SQLText = ';WITH CTELinked AS
							(	SELECT 0 AS object_id , ''Linked'' AS ServerType , UPPER(SR.data_source) AS Server , ISNULL(S.referenced_database_name,'''') AS DatabaseName
								, ISNULL(S.referenced_schema_name,'''') AS SchemaName, ISNULL(S.referenced_entity_name,'''') AS ObjectName, ''Unknown'' AS Classification		
								, O.type_desc,  S.referenced_server_name
								-- select * 
							FROM [' + @CurrDBName + '].sys.sql_expression_dependencies S 
							INNER JOIN [' + @CurrDBName + '].sys.objects O ON S.referencing_id = O.object_id 
							INNER JOIN [' + @CurrDBName + '].sys.servers sr ON S.referenced_server_name COLLATE DATABASE_DEFAULT = Sr.name COLLATE DATABASE_DEFAULT
							WHERE S.referenced_server_name IS NOT NULL 
							)		
							SELECT CurrentUser, HostName, object_id, ServerType, Server, DatabaseName, SchemaName, ObjectName, Classification, RowCounts, TotalSpaceGB, UsedSpaceGB, UnusedSpaceGB, TableCreationTime, TableModificationTime, ServerLastRebootTime, Last_User_Action
								 , last_user_seek, last_user_scan, last_user_lookup, last_user_update, DBNameToBeExcluded, DBNameToBeIncluded, SizeCutoffGB, RowCountCutOff, JSONRefCount , ISJSON(JSONRefCount) AS IS_JSON
							FROM
							(
								SELECT ''' + SYSTEM_USER + ''' AS CurrentUser , ''' + HOST_NAME() + ''' AS HostName, object_id, ServerType, Server, DatabaseName, SchemaName, ObjectName, Classification, 0 AS RowCounts, 0 AS TotalSpaceGB, 0 AS UsedSpaceGB	, 0 AS UnusedSpaceGB
								, NULL AS TableCreationTime, NULL AS TableModificationTime, ''' + CONVERT(VARCHAR,@ServerLastRebootTime) + ''' AS ServerLastRebootTime, NULL AS Last_User_Action, NULL AS last_user_seek
								, NULL AS last_user_scan, NULL AS last_user_lookup, NULL AS last_user_update
								, ' + CASE WHEN ISNULL(@DBNameToBeExcluded ,'') = '' THEN '''ALL''' ELSE ' ''' + @DBNameToBeExcluded + ''' ' END + ' AS DBNameToBeExcluded
								, ' + CASE WHEN ISNULL(@DBNameToBeIncluded ,'') = '' THEN '''ALL''' ELSE ' ''' + @DBNameToBeIncluded + ''' ' END + ' AS DBNameToBeIncluded		
								, ''' + CONVERT(VARCHAR,ISNULL(@SizeCutoffGB,0)) + ''' AS SizeCutoffGB , ''' + CONVERT(VARCHAR, ISNULL(@RowCountCutOff,0)) + ''' AS RowCountCutOff
								, (SELECT (SELECT  AG.[JSONRefCount] AS [''' + @CurrDBName + '''] FOR JSON path) AS ''' + @@SERVERNAME + ''' FOR JSON path) AS JSONRefCount
								FROM 
								(SELECT object_id, ServerType, Server, DatabaseName, SchemaName, ObjectName, Classification ,referenced_server_name
								 FROM CTELinked GROUP BY object_id, ServerType, Server, DatabaseName, SchemaName, ObjectName, Classification,referenced_server_name) A 
								CROSS APPLY (SELECT ( SELECT referenced_server_name AS [Reference], ''' + @@SERVERNAME + ''' AS LocalServer,type_desc AS ReferenceType, COUNT(*) AS ReferenceCount
											FROM CTELinked B WHERE A.object_id = B.object_id AND A.ServerType = B.ServerType AND A.Server = B.Server AND A.DatabaseName = B.DatabaseName AND A.SchemaName = B.SchemaName AND A.ObjectName = B.ObjectName AND A.Classification = B.Classification 				
											GROUP BY referenced_server_name,type_desc  FOR JSON path) AS JSONRefCount
											) AG 
							)Final ' 

			SELECT @SQLText = COALESCE(@SQLText, '','') + '
			/*---------' + @CurrDBName + ' Ends-----------*/  '					
			
			IF @Debug = 1
			BEGIN
				PRINT 'Linked : @SQLText: ' + CONVERT(VARCHAR, LEN(@SQLText))
				PRINT '--------------------------------------'
				PRINT @RefCount
				PRINT '--------------------------------------'
				PRINT @SQLText		
				PRINT '------------------------------------------------------------------------------------------------------'
			END

			INSERT INTO #TableProperties(CurrentUser, HostName, object_id, ServerType, Server, DatabaseName, SchemaName, ObjectName,Classification, RowCounts, TotalSpaceGB, UsedSpaceGB, UnusedSpaceGB, 
										TableCreationTime, TableModificationTime, ServerLastRebootTime, Last_User_Action, last_user_seek, 
										last_user_scan, last_user_lookup, last_user_update, DBNameToBeExcluded, DBNameToBeIncluded, SizeCutoffGB, RowCountCutOff, 
										JSONRefCount, IS_JSON)
			EXEC(@SQLText)

		END

		UPDATE TOP(1) #DB SET IsPickedUp = 1 WHERE IsPickedUp = 0 AND DBName = @CurrDBName

	END	

	SELECT * FROM #TableProperties

END TRY
BEGIN CATCH
	SELECT ERROR_MESSAGE()
	;THROW
END CATCH
