CREATE   PROCEDURE [procfwk].[GetStages]
	(
	@ExecutionId UNIQUEIDENTIFIER,
  @BatchId INT
	)
AS
BEGIN
	SET NOCOUNT ON;

	SELECT DISTINCT 
		[StageId],
    [BatchId]
	FROM 
		[procfwk].[CurrentExecution]
	WHERE
		[LocalExecutionId] = @ExecutionId
    AND [BatchId] = @BatchId
		AND ISNULL([PipelineStatus],'') <> 'Success'
	ORDER BY 
		[StageId] ASC
END;