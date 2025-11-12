/*
Goal :  Check for the connections to the database
---------------------------------------------------------------------------------------------------------------------
1. Get current session - the session that we are currently connected
2. Get the other sessions connected to the database other than the one we are currently connected 
*/

USE BaraaDataWarehouse ; 
GO 
-- Creating the procedure to check for connections to database
CREATE OR ALTER PROCEDURE warehouse_procedures.check_sessions 
(
    @kill_other SMALLINT  = 1
)
AS
BEGIN TRY 
    DECLARE @session_id INT, @status VARCHAR(100), @login_hour INT, @iteration_value INT
    -- Getting the current user session id
    SELECT
        @session_id=session_id, 
        @status=status, 
        @login_hour=DATEPART(HOUR, login_time)
    FROM sys.dm_exec_sessions 
    WHERE session_id = @@SPID ; 
    RAISERROR('>>> Current User: session id = %d | login hour = %d | status = %s', 10, 1, @session_id, @login_hour, @status) WITH NOWAIT;  
    -- Getting all the other users connected to the database
    RAISERROR('>>> Other Users connected to database', 10, 2) WITH NOWAIT; 
    SELECT 
        basic_session.session_id,
        basic_session.status,
        DATEPART(HOUR, login_time) AS login_hour,
        query_executed.text AS executed_query
    FROM sys.dm_exec_sessions AS basic_session
    INNER JOIN sys.dm_exec_requests AS more_informatic 
        ON basic_session.session_id = more_informatic.session_id
    CROSS APPLY sys.dm_exec_sql_text(more_informatic.sql_handle) AS query_executed 
    WHERE basic_session.session_id != @@SPID  AND is_user_process=1; 
    -- Killing all the other sessions user specified
    IF @kill_other = 1
    BEGIN 
         -- Setting the cursor - to store the ids and iterate them  
         DECLARE id_list CURSOR FOR
         SELECT 
            session_id 
         FROM sys.dm_exec_sessions 
         WHERE session_id != @@SPID AND is_user_process=1 ; 
         -- Getting the first value to kill from the id list
         FETCH NEXT FROM id_list INTO @iteration_value ; 
         -- Iteration : fetch status = 1(value extracted sucessfully) -1 (No more values to fetch) -2(Failed to extract value)
         WHILE @@FETCH_STATUS = 0
         BEGIN    
             BEGIN TRY
                 -- Killing the current value
                 DECLARE @executable VARCHAR(100) = 'KILL ' + CAST(@iteration_value AS VARCHAR(20)) ; 
                 EXEC(@executable) ;  
                 RAISERROR('Killed session id  %d', 10, 3, @iteration_value) ;
             END TRY
             BEGIN CATCH
                 RAISERROR('Failed to kill session %d', 10, 4, @iteration_value) ; 
             END CATCH
             -- Getting the next value
             FETCH NEXT FROM id_list INTO @iteration_value ; 
         END    
    END 
END TRY 
BEGIN CATCH 
    PRINT '>>> 1. Error Number ' + CAST(ERROR_NUMBER() AS VARCHAR(20)) ; 
    PRINT '>>> 2. Error Line ' + CAST(ERROR_LINE() AS VARCHAR(50)) ; 
    PRINT '>>> 3. Error Message ' + ERROR_MESSAGE() ; 
    PRINT '>>> 4. Error Procedure ' + CAST(COALESCE(ERROR_PROCEDURE(), 'N/A') AS VARCHAR(100)) ; 
END CATCH 
GO 
-- Executing the procedure 
EXEC warehouse_procedures.check_sessions @kill_other = 0; 
GO