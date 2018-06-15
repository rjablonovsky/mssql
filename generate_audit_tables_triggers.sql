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
SELECT * FROM audit.vwAuditTriggerInfo;
--SELECT * FROM audit.vwTriggerInfo;
