/*

  1) Create audit schema
  2) Initialized empty audit Table in audit schema.
    If the audit table exists fire error/notice
  3) Generate audit triggers for specified table

  REQUIREMENTS:
	a) MSSQL2012 or higher
	b) Databse tables should not contain datatypes:
		TEXT, NTEXT, IMAGE
		Test:
			SELECT c.*
			FROM sys.columns AS c
			JOIN sys.types AS ty
			  ON ty.system_type_id = c.system_type_id
			AND ty.user_type_id = c.user_type_id
			WHERE ty.name IN ('TEXT','NTEXT','IMAGE');
	c) Database tables should not contain after trigger for DELETE or UPADTE fired as last.
		Test:
			SELECT sch.name AS 'Schema'
				  ,tbl.name AS 'Table'
				  ,trg.name AS 'Trigger'
				  ,CASE OBJECTPROPERTY(trg.object_id ,'ExecIsLastUpdateTrigger')  WHEN 0 THEN '' ELSE 'X' END AS 'Update_Last'
				  ,CASE OBJECTPROPERTY(trg.object_id ,'ExecIsLastDeleteTrigger')  WHEN 0 THEN '' ELSE 'X' END AS 'Delete_Last'
			FROM            sys.objects AS trg WITH (NOLOCK)
				 INNER JOIN sys.objects AS tbl WITH (NOLOCK)
					ON  trg.parent_object_id = tbl.object_id
				 INNER JOIN sys.schemas AS sch WITH (NOLOCK)
					ON sch.schema_id = tbl.schema_id
			WHERE trg.TYPE IN (N'TR')
				AND (	OBJECTPROPERTY(trg.object_id ,'ExecIsLastUpdateTrigger') != 0
					 OR OBJECTPROPERTY(trg.object_id ,'ExecIsLastDeleteTrigger') != 0
					)
			ORDER BY sch.name,tbl.name ASC, trg.name ASC

  Example:
  Run on entire db:

-- 1): run the db_audit_tables.sql file on target db.

-- 2) generate audit tables and triggers for all user table (or filtered)

SET NOCOUNT ON;
DECLARE @TableSchema NVARCHAR(128), @TableName NVARCHAR(260);
DECLARE @AuditTableName nvarchar(800);
DECLARE @message VARCHAR(300);

--PRINT '-------- START generate audit tables and triggers --------';

DECLARE table_cur CURSOR FOR
SELECT s.name as TableSchema, o.name as TableName
FROM sys.objects o
JOIN sys.schemas s
  ON o.schema_id = s.schema_id
WHERE o.type = 'U' AND s.name NOT IN ('Audit')
  -- add here more conditions to filter table(s) with audit data retention
  -- not include FileTables
  AND o.object_id NOT IN (select object_id from sys.filetables)
  --AND s.name IN ('drr')
ORDER BY s.name, o.name;

OPEN table_cur

FETCH NEXT FROM table_cur
INTO @TableSchema, @TableName
SET @TableName = @TableSchema + N'.' + @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @message = '-- Create audit table and trigger for source table: ' + @TableName
    PRINT @message

	-- generate or regenerate audit table
    EXECUTE audit.spCreateOrAlterauditTable @TableName, @AuditTableName OUT, 2
	-- generate audit triggers on source table
	EXECUTE audit.spCreateOrAlterauditTrigger @TableName, @AuditTableName, 2

    -- Get the next table.
	FETCH NEXT FROM table_cur
	INTO @TableSchema, @TableName
	SET @TableName = @TableSchema + N'.' + @TableName
END
CLOSE table_cur;
DEALLOCATE table_cur;

--PRINT '-------- END generate audit tables and triggers --------';

-- test if trigger(s) are generated:
SET NOCOUNT ON;
DECLARE @TableSchema NVARCHAR(128), @TableName NVARCHAR(260);
DECLARE @AuditTableName nvarchar(800);
DECLARE @message VARCHAR(300);

--PRINT '-------- START generate audit tables and triggers --------';

DECLARE table_cur CURSOR FOR
SELECT s.name as TableSchema, o.name as TableName
FROM sys.objects o
JOIN sys.schemas s
  ON o.schema_id = s.schema_id
WHERE o.type = 'U' AND s.name NOT IN ('Audit')
  -- add here more conditions to filter table(s) with audit data retention
  -- not include FileTables
  AND o.object_id NOT IN (select object_id from sys.filetables)
  --AND s.name IN ('drr')
ORDER BY s.name, o.name;

OPEN table_cur

FETCH NEXT FROM table_cur
INTO @TableSchema, @TableName
SET @TableName = @TableSchema + N'.' + @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @message = '-- Create audit table and trigger for source table: ' + @TableName
    PRINT @message

	-- generate or regenerate audit table
    EXECUTE audit.spCreateOrAlterauditTable @TableName, @AuditTableName OUT, 2
	-- generate audit triggers on source table
	EXECUTE audit.spCreateOrAlterauditTrigger @TableName, @AuditTableName, 2

    -- Get the next table.
	FETCH NEXT FROM table_cur
	INTO @TableSchema, @TableName
	SET @TableName = @TableSchema + N'.' + @TableName
END
CLOSE table_cur;
DEALLOCATE table_cur;

--PRINT '-------- END generate audit tables and triggers --------';

-- test if trigger(s) are generated:
SELECT sch.name AS 'Schema'
	  ,tbl.name AS 'Table'
	  ,trg.name AS 'Trigger'
	  ,CASE OBJECTPROPERTY(trg.object_id ,'ExecIsLastUpdateTrigger')  WHEN 0 THEN '' ELSE 'X' END AS 'Update_Last'
	  ,CASE OBJECTPROPERTY(trg.object_id ,'ExecIsLastDeleteTrigger')  WHEN 0 THEN '' ELSE 'X' END AS 'Delete_Last'
	  ,tr.is_disabled, tr.is_instead_of_trigger, tr.is_ms_shipped, tr.is_ms_shipped
FROM            sys.objects AS trg WITH (NOLOCK)
	 INNER JOIN sys.objects AS tbl WITH (NOLOCK)
		ON  trg.parent_object_id = tbl.object_id
	 INNER JOIN sys.schemas AS sch WITH (NOLOCK)
		ON sch.schema_id = tbl.schema_id
	 INNER JOIN sys.triggers tr
		ON tr.object_id = trg.object_id
WHERE trg.TYPE IN (N'TR')
	AND (	OBJECTPROPERTY(trg.object_id ,'ExecIsLastUpdateTrigger') != 0
		 OR OBJECTPROPERTY(trg.object_id ,'ExecIsLastDeleteTrigger') != 0
		)
ORDER BY sch.name,tbl.name ASC, trg.name ASC;


--------------------------------------------------------------------
-- Create MSSQL job for purging old audit data from audit table(s)



---------------------------------------------------------------------
-- Example of MIGRATE trigger based audit table(s) to temporal tables in
-- MSSQL 2016+.
--	Recomended is to use MSSQL 2016+ Entreprise Edition or higher
--  as Adding non-nullable columns with defaults to an existing table with
--	data is a size of data operation on all editions other than SQL Server
--	Enterprise Edition (on which it is a metadata operation). With a large
--	existing audit table with data on SQL Server Standard Edition, adding
--	a non-null column can be an expensive operation.

-- Drop trigger on future temporal table
ALTER TABLE BI.Registration
DROP TRIGGER TR_' + 'BI.Registration' + '_UD_audit;
-- Make sure that future period columns are non-nullable
ALTER TABLE BI.Registration ADD
	SysStartTime datetime2(0) GENERATED ALWAYS AS ROW START HIDDEN
         CONSTRAINT DF_SysStart DEFAULT SYSUTCDATETIME(),
    SysEndTime datetime2(0) GENERATED ALWAYS AS ROW END HIDDEN
		CONSTRAINT DF_SysEnd DEFAULT CONVERT(datetime2 (0), '9999-12-31 23:59:59'),
    PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime);
ALTER TABLE audit.BI_Registration ADD SysStartTime datetime2 NOT NULL;
ALTER TABLE audit.BI_Registration ALTER COLUMN SysEndTime datetime2 NOT NULL;
ALTER TABLE BI.Registration
   SET (SYSTEM_VERSIONING = ON (audit_TABLE = audit.BI_Registration, DATA_CONSISTENCY_CHECK = ON));

Important remarks:
Referencing existing columns in PERIOD definition implicitly changes
generated_always_type to AS_ROW_START and AS_ROW_END for those columns.
Adding PERIOD will perform a data consistency check on current table
to make sure that the existing values for period columns are valid.
It is highly recommended to set SYSTEM_VERSIONING with DATA_CONSISTENCY_CHECK = ON
to enforce data consistency checks on existing data.
System Consistency Checks:
Before SYSTEM_VERSIONING is set to ON, a set of checks are performed on the
audit table and the current table. These checks are grouped into schema checks
and data checks (if audit table is not empty). In addition, the system also
performs a runtime consistency check.
Schema Check:
When creating or alter a table to become a temporal table, the system verifies
that requirements are met:
	The names and number of columns is the same in both the current table and
		the audit table.
	The datatypes match for each column between the current table and the audit
		table.
	The period columns are set to NOT NULL.
	The current table has a primary key constraint and the audit table does
		not have a primary key constraint.
	No IDENTITY columns are defined in the audit table.
	No triggers are defined in the audit table.
	No foreign keys are defined in the audit table.
	No table or column constraints are defined on the audit table. However,
		default column values on the audit table are permitted.
	The audit table is not placed in a read-only filegroup.
	The audit table is not configured for change tracking or change data capture.
Data Consistency Check:
Before SYSTEM_VERSIONING is set to ON and as part of any DML operation, the
	system performs the following check: SysEndTime ≥SysStartTime
When creating a link to an existing audit table, you can choose to perform a
data consistency check. This data consistency check ensures that existing
records do not overlap and that temporal requirements are fulfilled for every
individual record. Performing the data consistency check is the default.
Generally, performing the data consistency is recommended whenever the data
between the current and audit tables may be out of sync, such as when
incorporating an existing audit table that is populated with audit data.

*/

-- Create audit schema as dba/dbo
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Audit')
BEGIN
  EXEC('CREATE SCHEMA Audit');
END;


-- initialize empty table audit schema from tablename and schema name
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P'
	AND schema_id = (SELECT schema_id FROM sys.schemas WHERE name = 'Audit')
	AND name = 'spCreateOrAlterauditTable')
DROP PROCEDURE audit.spCreateOrAlterauditTable
GO
CREATE PROCEDURE audit.spCreateOrAlterauditTable
  @TableName nvarchar(800),
  @AuditTableName nvarchar(800) OUTPUT,
  @GenerateSQL int = 1
AS
/*
  @TableName - has to be fully qualified table name in db including schema name
  @AuditTableName - the table name is generated using @TableName as source
                    with schemaname: audit if the source schemaname and
					tablename is longer than 127 char then generate the audit
					table using md5 hash of source table name
  @GenerateSQL - 0: does not generate SQL, 1: generate SQL and objects, 
				 2: generate ALL SQL but not objects

  example:
    DECLARE @TableName nvarchar(800) = 'BI.Registration';
    DECLARE @AuditTableName nvarchar(800);
	-- generate or alter audit table from source table
    EXECUTE audit.spCreateOrAlterauditTable @TableName, @AuditTableName OUT
*/

SET NOCOUNT ON;

DECLARE @SQLCMD NVARCHAR(MAX);
DECLARE @SQLSOURCE NVARCHAR(4000);
DECLARE @SQLAUDIT NVARCHAR(4000);
DECLARE @DefaultSchemaName sysname = 'audit';
DECLARE @TableID int;
DECLARE @Message NVARCHAR(2000);

SET @TableID = (SELECT OBJECT_ID(@TableName, 'U'));

IF (@TableID IS NOT NULL)
BEGIN
	SET @AuditTableName = OBJECT_SCHEMA_NAME(@TableID)
		+ N'_' + OBJECT_NAME(@TableID);
	IF ( len(@AuditTableName) < (128 - 17) )
		SET @AuditTableName = @DefaultSchemaName + N'.' + @AuditTableName
	ELSE
		SET @AuditTableName = @DefaultSchemaName + N'.'
			+ SUBSTRING(@AuditTableName, 1, 128 - 65) + N'_'
			+ CONVERT(NVARCHAR(32),HashBytes('MD5', @AuditTableName),2)
END;


-- If audit table does not exists initialize it as empty table.
-- If audit table exists, add new columns from source table or expand current columns.
-- Else throw error about incompatible datatypes in source and audit tables
IF ( OBJECT_ID(@AuditTableName, 'U') IS NULL OR @GenerateSQL = 2)
BEGIN
	BEGIN TRY
		-- create empty audit table with null columns
		SET @SQLSOURCE = N'SELECT * FROM ' + @TableName
		SET @SQLCMD = N'CREATE TABLE ' + @AuditTableName + N' (' + char(10)
		SELECT @SQLCMD = @SQLCMD + AUDIT.name + N' ' + AUDIT.system_type_name + N' NULL, ' + char(10)
		FROM sys.dm_exec_describe_first_result_set (@SQLSOURCE, NULL, 0) AUDIT;

		-- add audit data metadata informations
		SET @SQLCMD = @SQLCMD
		+ N'SysEndTime datetime2 NOT NULL CONSTRAINT DF_' + REPLACE(@AuditTableName,'.','_')
		+ N'_SysEndTime DEFAULT SYSDATETIME(), ' + char(10)
		+ N'DMLAction char(1), ' + char(10)
		+ N'SuserName nvarchar(128) CONSTRAINT DF_' + REPLACE(@AuditTableName,'.','_')
		+ N'_SuserName DEFAULT suser_name(), ' + char(10)
		+ N'AppName nvarchar(128) CONSTRAINT DF_' + REPLACE(@AuditTableName,'.','_')
		+ N'_AppName DEFAULT app_name() ' + char(10)
		+ N');' + char(10)
		IF (@GenerateSQL IN (1,2))	PRINT @SQLCMD + 'GO' + char(10);
		IF (@GenerateSQL IN (0,1))	EXECUTE(@SQLCMD);

		SET @SQLCMD =
		N'CREATE CLUSTERED INDEX CX_' + OBJECT_NAME(OBJECT_ID(@AuditTableName)) + N'_SysEndTime '
		+ N'ON ' + @AuditTableName + '(SysEndTime ASC); ' + char(10);
		IF (@GenerateSQL IN (1,2))	PRINT @SQLCMD + 'GO' + char(10);
		IF (@GenerateSQL IN (0,1))	EXECUTE(@SQLCMD);
		
	END TRY
	BEGIN CATCH
		SELECT
			 ERROR_NUMBER()  	AS ErrorNumber
			,ERROR_SEVERITY() 	AS ErrorSeverity
			,ERROR_STATE() 		AS ErrorState
			,ERROR_PROCEDURE() 	AS ErrorProcedure
			,ERROR_LINE() 		AS ErrorLine
			,ERROR_MESSAGE() 	AS ErrorMessage
			,@SQLCMD		    AS SQLCMD;
		PRINT 'ERRSQL:' + char(10) + @SQLCMD;
	END CATCH;

	SET @Message = 'For source table ' + @TableName + ' new empty audit table '
		+ @AuditTableName + ' was created'
	IF (@GenerateSQL IN (0)) PRINT @Message
END
ELSE
BEGIN

	-- set audit and source tables metadata query
	SET @SQLSOURCE = N'SELECT * FROM ' + @TableName
	SET @SQLAUDIT = N'SELECT * FROM ' + @AuditTableName

	-- test for incompatible datatypes. The system datatype has to exactly the
	-- same on SOURCE and on audit tables. It is recommended if
	-- datatype length on SOURCE is the same as on audit table, but audit
	-- table could have longer datatype as SOURCE table to accomodate longer
	-- audit data.
	IF EXISTS( SELECT *
		FROM sys.dm_exec_describe_first_result_set (@SQLSOURCE, NULL, 0) SOURCE
		INNER JOIN  sys.dm_exec_describe_first_result_set (@SQLAUDIT, NULL, 0) AUDIT
			ON SOURCE.name = AUDIT.name
		WHERE AUDIT.system_type_id != SOURCE.system_type_id
		)
	BEGIN
		-- To resolve use this issue(s) use manual approach. 
		-- Check source control system like git, talk to DEV, PM, Architect, BA, manager(s)
		SELECT 'Possible incompatible column datatype(s) conversion' as ERROR_message,
			@TableName			 	as SOURCE_Table,
			@AuditTableName		 	as AUDIT_Table,
			SOURCE.name 		 	as SOURCE_ColumnName,
			AUDIT.name 			 	as AUDIT_ColumnName,
			SOURCE.system_type_name as SOURCE_Datatype,
			AUDIT.system_type_name 	as AUDIT_Datatype
		FROM sys.dm_exec_describe_first_result_set (@SQLSOURCE, NULL, 0) SOURCE
		INNER JOIN  sys.dm_exec_describe_first_result_set (@SQLAUDIT, NULL, 0) AUDIT
			ON SOURCE.name = AUDIT.name
		WHERE AUDIT.system_type_id != SOURCE.system_type_id;
		
		PRINT 'ERRMSG: Possible incompatible column datatype conversion(s) from source table ' + @TableName + ' to audit table '+ @AuditTableName;
	END
	ELSE
	BEGIN TRY

		-- generate audit alter table with adding new columns to audit table
		IF EXISTS ( SELECT *
			FROM sys.dm_exec_describe_first_result_set (@SQLSOURCE, NULL, 0) SOURCE
			LEFT OUTER JOIN sys.dm_exec_describe_first_result_set (@SQLAUDIT, NULL, 0) AUDIT
			ON SOURCE.name = AUDIT.name
			WHERE AUDIT.name IS NULL )
		BEGIN
			SET @SQLCMD = N'ALTER TABLE ' + @AuditTableName + N' ADD ';
			SELECT @SQLCMD = @SQLCMD + SOURCE.name + N' ' + SOURCE.system_type_name + N', '
			FROM sys.dm_exec_describe_first_result_set (@SQLSOURCE, NULL, 0) SOURCE
			LEFT OUTER JOIN sys.dm_exec_describe_first_result_set (@SQLAUDIT, NULL, 0) AUDIT
			ON SOURCE.name = AUDIT.name
			WHERE AUDIT.name IS NULL;

			SET @SQLCMD = SUBSTRING(@SQLCMD, 1, LEN(@SQLCMD) - 2) + char(10);
			IF (@GenerateSQL IN (1,2))	PRINT @SQLCMD + 'GO' + char(10);
			IF (@GenerateSQL IN (0,1))	EXECUTE(@SQLCMD);

			SET @Message = 'Based on source table ' + @TableName + ' new '
				+ 'column(s) were added to AUDIT table ' + @AuditTableName;
			IF (@GenerateSQL IN (0)) PRINT @Message
		END;

		-- modify audit table with expanded datatypes in source table
		IF EXISTS( SELECT *
			FROM sys.dm_exec_describe_first_result_set (@SQLSOURCE, NULL, 0) SOURCE
			INNER JOIN  sys.dm_exec_describe_first_result_set (@SQLAUDIT, NULL, 0) AUDIT
				ON SOURCE.name = AUDIT.name
					AND SOURCE.system_type_id = AUDIT.system_type_id
			WHERE AUDIT.max_length < SOURCE.max_length )
		BEGIN
			SET @SQLCMD = N'ALTER TABLE ' + @AuditTableName + N' ALTER COLUMN ';
			SELECT @SQLCMD = @SQLCMD + SOURCE.name + N' ' + SOURCE.system_type_name + N', '
			FROM sys.dm_exec_describe_first_result_set (@SQLSOURCE, NULL, 0) SOURCE
			INNER JOIN  sys.dm_exec_describe_first_result_set (@SQLAUDIT, NULL, 0) AUDIT
				ON SOURCE.name = AUDIT.name
					AND SOURCE.system_type_id = AUDIT.system_type_id
			WHERE AUDIT.max_length < SOURCE.max_length;

			SET @SQLCMD = SUBSTRING(@SQLCMD, 1, LEN(@SQLCMD) - 2) + char(10);
			IF (@GenerateSQL IN (1,2))	PRINT @SQLCMD + 'GO' + char(10);
			IF (@GenerateSQL IN (0,1))	EXECUTE(@SQLCMD);

			SET @Message = 'Based on source table ' + @TableName + ' column(s) '
			    + ' were altered in audit table ' + @AuditTableName;
			IF (@GenerateSQL IN (0)) PRINT @Message
		END;

	END TRY
	BEGIN CATCH
		SELECT
			 ERROR_NUMBER()  	AS ErrorNumber
			,ERROR_SEVERITY() 	AS ErrorSeverity
			,ERROR_STATE() 		AS ErrorState
			,ERROR_PROCEDURE() 	AS ErrorProcedure
			,ERROR_LINE() 		AS ErrorLine
			,ERROR_MESSAGE() 	AS ErrorMessage
			,@SQLCMD			AS SQLCMD;
		PRINT 'ERRSQL:' + char(10) + @SQLCMD;
	END CATCH;

END;
GO


-- generate audit trigger for update, delete on source table
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P'
	AND schema_id = (SELECT schema_id FROM sys.schemas WHERE name = 'audit')
	AND name = 'spCreateOrAlterauditTrigger')
DROP PROCEDURE audit.spCreateOrAlterauditTrigger
GO
CREATE PROCEDURE audit.spCreateOrAlterauditTrigger
  @TableName nvarchar(800),
  @AuditTableName nvarchar(800),
  @GenerateSQL int = 1
AS
/*
  @TableName - has to be fully qualified table name in db including schema name
  @AuditTableName - the table name is generated using @TableName as source with schemaname audit
                    if the source schemaname and tablename is longer than 127 char
                    then generate the audit table using md5 hash of source table name
  @GenerateSQL - 0: does not generate SQL, 1: generate SQL and objects, 
				 2: generate ALL SQL but not objects

  example:
    DECLARE @TableName nvarchar(800) = 'BI.Rejection';
    DECLARE @AuditTableName nvarchar(800);
	DECLARE @Message nvarchar(4000);
	-- generate or alter audit table from source table
    EXECUTE audit.spCreateOrAlterauditTable @TableName, @AuditTableName OUT
	-- generate audit triggers on source table
	EXECUTE audit.spCreateOrAlterauditTrigger @TableName, @AuditTableName

*/

DECLARE @SQLSOURCE NVARCHAR(4000);
DECLARE @GENERATED_COLUMNS NVARCHAR(MAX) = '';
DECLARE @GENERATED_COLUMNS_TAG NVARCHAR(20) = '<generated_columns>';
DECLARE @CREATE_TRIGGER_TEMPLATE NVARCHAR(MAX);
DECLARE @TRIGGER_NAME NVARCHAR(800);
DECLARE @TRIGGER_SQL NVARCHAR(MAX);
DECLARE @TableID int;
DECLARE @Message NVARCHAR(2000);

SET @TableID = (SELECT OBJECT_ID(@TableName, 'U'));
IF (@TableID IS NOT NULL)
BEGIN
	BEGIN TRY
		SET @TRIGGER_NAME=OBJECT_SCHEMA_NAME(@TableID) + '.' + N'TR_' + OBJECT_NAME(@TableID) + '_UD_audit';

		IF ( OBJECT_ID(@TRIGGER_NAME, 'TR') IS NOT NULL AND @GenerateSQL != 2 )
		BEGIN
			SET @TRIGGER_SQL=N'DROP TRIGGER ' + @TRIGGER_NAME;
			EXECUTE(@TRIGGER_SQL);
		END;

		SET @CREATE_TRIGGER_TEMPLATE=N''+ char(10)
		+ N'--******************************************************************' + char(10)
		+ N'--Author:		' + suser_name() + char(10)
		+ N'--Date Created:	' + convert(NVARCHAR(40), SYSDATETIME(), 121) + char(10)
		+ N'--Description:	Insert record into audit table ' + @AuditTableName + char(10)
		+ N'--				from table ' + @TableName + N' on UPDATE or DELETE action' + char(10)
		+ N'--      		The trigger has to be executed last.' + char(10)
		+ N'--Modifications: 	DATE		PERSON		DESCRIPTION' + char(10)
		+ N'--' + char(10)
		+ N'--******************************************************************' + char(10)
		+ N'CREATE TRIGGER ' + @TRIGGER_NAME + ' ' + char(10)
		+ N'ON ' + @TableName + ' ' + char(10)
		+ N'AFTER UPDATE, DELETE ' + char(10)
		+ N'AS ' + char(10)
		+ N'BEGIN ' + char(10)
		+ N'	SET NOCOUNT ON; ' + char(10)
		+ N'	IF EXISTS(SELECT 1 FROM INSERTED) ' + char(10)
		+ N'		INSERT INTO ' + @AuditTableName + '(' + @GENERATED_COLUMNS_TAG + 'DMLAction) ' + char(10)
		+ N'		SELECT ' + @GENERATED_COLUMNS_TAG + '''U'' ' + char(10)
		+ N'		FROM DELETED ' + char(10)
		+ N'	ELSE ' + char(10)
		+ N'		INSERT INTO ' + @AuditTableName + '(' + @GENERATED_COLUMNS_TAG + 'DMLAction) ' + char(10)
		+ N'		SELECT ' + @GENERATED_COLUMNS_TAG + '''D'' ' + char(10)
		+ N'		FROM DELETED ' + char(10)
		+ N'END ' + char(10)

		SET @GENERATED_COLUMNS = '';
		SET @SQLSOURCE = N'SELECT * FROM ' + @TableName;
		SELECT @GENERATED_COLUMNS = @GENERATED_COLUMNS + SOURCE.name + N', '
		FROM sys.dm_exec_describe_first_result_set (@SQLSOURCE, NULL, 0) SOURCE;

		SET @TRIGGER_SQL = REPLACE(@CREATE_TRIGGER_TEMPLATE, @GENERATED_COLUMNS_TAG, @GENERATED_COLUMNS);
		--SET @Message = 'TRIGGER SQL: ' + @TRIGGER_SQL; PRINT @Message;
		IF (@GenerateSQL IN (1,2))	PRINT @TRIGGER_SQL + 'GO' + char(10);
		IF (@GenerateSQL IN (0,1))	EXECUTE(@TRIGGER_SQL);

		-- set trigger firing order as last for UPDATE and DELETE:
		SET @TRIGGER_SQL = N'EXECUTE sp_settriggerorder @triggername=''' + @TRIGGER_NAME + N''', @order=''Last'', @stmttype = ''UPDATE'';' + char(10)
		SET @TRIGGER_SQL = @TRIGGER_SQL + 'GO' + char(10)
		SET @TRIGGER_SQL = @TRIGGER_SQL + 'EXECUTE sp_settriggerorder @triggername=''' + @TRIGGER_NAME + N''', @order=''Last'', @stmttype = ''DELETE'';' + char(10)
		IF (@GenerateSQL IN (1,2))	PRINT @TRIGGER_SQL + 'GO' + char(10);
		IF (@GenerateSQL IN (0,1))
		BEGIN
			EXECUTE sp_settriggerorder @triggername= @TRIGGER_NAME, @order='Last', @stmttype = 'UPDATE';
			EXECUTE sp_settriggerorder @triggername= @TRIGGER_NAME, @order='Last', @stmttype = 'DELETE';
		END;

		SET @Message = 'Trigger: ' + @TRIGGER_NAME + ' on table: ' + @TableName + ' was created or altered'
		IF (@GenerateSQL IN (0)) PRINT @Message
	END TRY
	BEGIN CATCH
		SELECT
			 ERROR_NUMBER()  	AS ErrorNumber
			,ERROR_SEVERITY() 	AS ErrorSeverity
			,ERROR_STATE() 		AS ErrorState
			,ERROR_PROCEDURE() 	AS ErrorProcedure
			,ERROR_LINE() 		AS ErrorLine
			,ERROR_MESSAGE() 	AS ErrorMessage
			,@TRIGGER_SQL		AS TRIGGER_SQL;
		PRINT 'ERRSQL:' + char(10) + @TRIGGER_SQL;
	END CATCH;

END;
GO


-- purge audit table old data based on retention period
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P'
	AND schema_id = (SELECT schema_id FROM sys.schemas WHERE name = 'audit')
	AND name = 'spPurgeAuditTable')
DROP PROCEDURE audit.spPurgeAuditTable
GO
CREATE PROCEDURE audit.spPurgeAuditTable
  @AuditTableName nvarchar(800),
  @DataRetention int = 4000,
  @RemoveRowBulk int = 10000
AS
/*
  Purge audit table old data based on retention period. Purge data only in audit schema.

  @AuditTableName - the table name is generated using @TableName as source
                    with schemaname: audit if the source schemaname and
					tablename is longer than 127 char then generate the audit
					table using md5 hash of source table name
  @DataRetention - data retention in days. Default is 10 years = (10 * 365 ~ 4000)
  @RemoveRowBulk - max number of row removed from table. Default is 10000
  					
  example:
    EXECUTE audit.spPurgeAuditTable 'audit.BI_Registration', 100;
*/

SET NOCOUNT ON;

DECLARE @SQLCMD NVARCHAR(MAX);
DECLARE @DefaultSchemaName sysname = 'audit';
DECLARE @Message NVARCHAR(2000);

IF ( OBJECT_ID(@AuditTableName, 'U') IS NOT NULL )
BEGIN
	IF ( OBJECT_SCHEMA_NAME(OBJECT_ID(@AuditTableName, 'U')) = @DefaultSchemaName )
	BEGIN TRY
		SET @SQLCMD = 
			N'DELETE TOP (@RemoveRowBulk) FROM ' + @AuditTableName + N' '
			+ N'WHERE SysEndTime < dateadd(DAY, -@DataRetention, SYSDATETIME()) ';
			--SET @Message = 'SQL to be executed: ' + char(10) + @SQLCMD; PRINT @Message;

		EXEC sp_executesql @SQLCMD,  
			 N'@RemoveRowBulk INT, @DataRetention INT', 
			 @RemoveRowBulk, @DataRetention

	END TRY
	BEGIN CATCH
		SELECT
			 ERROR_NUMBER()  	AS ErrorNumber
			,ERROR_SEVERITY() 	AS ErrorSeverity
			,ERROR_STATE() 		AS ErrorState
			,ERROR_PROCEDURE() 	AS ErrorProcedure
			,ERROR_LINE() 		AS ErrorLine
			,ERROR_MESSAGE() 	AS ErrorMessage
			,@SQLCMD		    AS SQLCMD;
		PRINT 'ERRSQL:' + char(10) + @SQLCMD;
	END CATCH
	ELSE
	BEGIN
		SELECT 'Table ' + @AuditTableName + N' is not in schema ' + @DefaultSchemaName AS ERRMSG;
		PRINT 'ERRMSG: Table ' + @AuditTableName + N' is not in schema ' + @DefaultSchemaName;
	END
END
ELSE
BEGIN
	PRINT 'ERRMSG: Table ' + @AuditTableName + N' does not exists';
END
GO


-- Purge audit table(s) based on filter condition. Provide retention period and remove row bulk count
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P'
	AND schema_id = (SELECT schema_id FROM sys.schemas WHERE name = 'audit')
	AND name = 'spPurgeAuditTableByFilter')
DROP PROCEDURE audit.spPurgeAuditTableByFilter
GO
CREATE PROCEDURE audit.spPurgeAuditTableByFilter
  @AuditTableFilter nvarchar(4000) = null,
  @DataRetention int = 4000,
  @RemoveRowBulk int = 10000
AS
/*
  Purge audit table old data based on retention period. Purge data only in audit schema.

  @AuditTableFilter - filter the table based on condition. Default is null ~ all tables in audit schema
  @DataRetention - data retention in days. Default is 10 years = (10 * 365 ~ 4000)
  @RemoveRowBulk - max number of row removed from table. Default is 10000
  					
  example:
    EXECUTE audit.spPurgeAuditTableByFilter;
*/

SET NOCOUNT ON;

DECLARE @DefaultSchemaName sysname = 'audit';
DECLARE @TableID int, @AuditTableName sysname;
DECLARE @ListAuditTables TABLE (TableID int identity(1,1) not null, AuditTableName NVARCHAR(800) NOT NULL)

BEGIN TRY
	INSERT INTO @ListAuditTables(AuditTableName)
	SELECT s.name + '.' + o.name as AuditTableName
	FROM sys.objects o
	JOIN sys.schemas s
	  ON o.schema_id = s.schema_id
	WHERE o.type = 'U'
	  AND s.name = @DefaultSchemaName
	  AND o.name LIKE CASE WHEN (@AuditTableFilter IS NULL OR RTRIM(LTRIM(@AuditTableFilter)) = '') THEN '%' ELSE @AuditTableFilter END
	ORDER BY s.name, o.name;

	SELECT * FROM @ListAuditTables;
	WHILE ( (SELECT count(*) from @ListAuditTables) > 0 ) 
	BEGIN
		SELECT TOP 1 @TableID = TableID, @AuditTableName = AuditTableName
		FROM @ListAuditTables ORDER BY TableID;
		
		EXECUTE audit.spPurgeAuditTable @AuditTableName, @DataRetention, @RemoveRowBulk;

		DELETE FROM @ListAuditTables WHERE TableID = @TableID;
	END
END TRY
BEGIN CATCH
	SELECT
		 ERROR_NUMBER()  	AS ErrorNumber
		,ERROR_SEVERITY() 	AS ErrorSeverity
		,ERROR_STATE() 		AS ErrorState
		,ERROR_PROCEDURE() 	AS ErrorProcedure
		,ERROR_LINE() 		AS ErrorLine
		,ERROR_MESSAGE() 	AS ErrorMessage;
	PRINT 'ERROR_MESSAGE:' + char(10) + ERROR_MESSAGE();
END CATCH;
GO

-- Purge all audit tables
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P'
	AND schema_id = (SELECT schema_id FROM sys.schemas WHERE name = 'audit')
	AND name = 'spPurgeAuditTableAll')
DROP PROCEDURE audit.spPurgeAuditTableAll
GO
CREATE PROCEDURE audit.spPurgeAuditTableAll
  @DataRetention int = 4000,
  @RemoveRowBulk int = 10000
AS
/*
  Purge audit table old data based on retention period. Purge data only in audit schema.

  @DataRetention - data retention in days. Default is 10 years = (10 * 365 ~ 4000)
  @RemoveRowBulk - max number of row removed from table. Default is 10000
  					
  example:
    EXECUTE audit.spPurgeAuditTableAll;
*/

SET NOCOUNT ON;
EXECUTE audit.spPurgeAuditTableByFilter '', @DataRetention, @RemoveRowBulk;
GO


-- View to see triggers, their property and dependendencies
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V'
	AND schema_id = (SELECT schema_id FROM sys.schemas WHERE name = 'audit')
	AND name = 'vwTriggerInfo')
DROP VIEW audit.vwTriggerInfo
GO
CREATE VIEW audit.vwTriggerInfo
AS 
SELECT TOP 90000
	   OBJECT_SCHEMA_NAME(tbl.object_id) + '.' + tbl.name AS 'TableOwingTrigger'
	  ,OBJECT_SCHEMA_NAME(trg.object_id) + '.' + trg.name AS 'TriggerName'
	  ,STUFF( (
				 SELECT DISTINCT ', ' + (s6.name+ '.' + o1.name)
				 FROM	sys.objects  o1  WITH (NOLOCK)
					   ,sys.sysdepends  d3  WITH (NOLOCK)
					   ,sys.schemas  s6  WITH (NOLOCK)
				WHERE  o1.object_id = d3.depid 
				  AND  d3.id = trg.object_id   
				  AND  o1.schema_id = s6.schema_id  
				  AND deptype < 2 
				ORDER BY ', ' + (s6.name+ '.' + o1.name)
				FOR XML PATH('')), 1, 1, '') AS ListObjectsOnWhichTriggerDepends
      ,CASE OBJECTPROPERTY(trg.object_id ,'ExecIsFirstInsertTrigger') WHEN 0 THEN '' ELSE 'X' END AS 'Insert_First'
      ,CASE OBJECTPROPERTY(trg.object_id ,'ExecIsLastInsertTrigger')  WHEN 0 THEN '' ELSE 'X' END AS 'Insert_Last'
      ,CASE OBJECTPROPERTY(trg.object_id ,'ExecIsFirstUpdateTrigger') WHEN 0 THEN '' ELSE 'X' END AS 'Update_First'
      ,CASE OBJECTPROPERTY(trg.object_id ,'ExecIsLastUpdateTrigger')  WHEN 0 THEN '' ELSE 'X' END AS 'Update_Last'
      ,CASE OBJECTPROPERTY(trg.object_id ,'ExecIsFirstDeleteTrigger') WHEN 0 THEN '' ELSE 'X' END AS 'Delete_First'
      ,CASE OBJECTPROPERTY(trg.object_id ,'ExecIsLastDeleteTrigger')  WHEN 0 THEN '' ELSE 'X' END AS 'Delete_Last'
	  ,tr.is_disabled, tr.is_instead_of_trigger, tr.is_ms_shipped, tr.is_not_for_replication
FROM            sys.objects AS trg WITH (NOLOCK)
	 INNER JOIN sys.objects AS tbl WITH (NOLOCK)
		ON  trg.parent_object_id = tbl.object_id
	 INNER JOIN sys.schemas AS sch WITH (NOLOCK)
		ON sch.schema_id = tbl.schema_id
	 INNER JOIN sys.triggers AS tr WITH (NOLOCK)
		ON tr.object_id = trg.object_id
WHERE trg.TYPE IN (N'TR')
ORDER BY sch.name,tbl.name ASC, trg.name ASC;
GO

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V'
	AND schema_id = (SELECT schema_id FROM sys.schemas WHERE name = 'audit')
	AND name = 'vwAuditTriggerInfo')
DROP VIEW audit.vwAuditTriggerInfo
GO
CREATE VIEW audit.vwAuditTriggerInfo
AS 
SELECT TableOwingTrigger
      ,TriggerName
      ,ListObjectsOnWhichTriggerDepends
      ,Insert_First
      ,Insert_Last
      ,Update_First
      ,Update_Last
      ,Delete_First
      ,Delete_Last
      ,is_disabled
      ,is_instead_of_trigger
      ,is_ms_shipped
      ,is_not_for_replication
FROM audit.vwTriggerInfo
WHERE TriggerName LIKE '%!_audit' ESCAPE '!'
  OR ListObjectsOnWhichTriggerDepends LIKE 'Audit._%'
GO

