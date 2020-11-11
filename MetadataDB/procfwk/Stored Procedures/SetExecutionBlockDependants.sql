CREATE   PROCEDURE [procfwk].[SetExecutionBlockDependants]
	(
	@ExecutionId UNIQUEIDENTIFIER = NULL,
  @BatchId INT,
	@PipelineId INT
	)
AS
BEGIN
	--assume current execution if value not provided
	IF @ExecutionId IS NULL SELECT TOP 1 @ExecutionId = [LocalExecutionId] FROM [procfwk].[CurrentExecution] WHERE [BatchId] = @BatchId;
	
	--update dependents status
	UPDATE
		ce
	SET
		ce.[PipelineStatus] = 'Blocked',
		ce.[IsBlocked] = 1
	FROM
		[procfwk].[PipelineDependencies] pe
		INNER JOIN [procfwk].[CurrentExecution] ce
			ON pe.[DependantPipelineId] = ce.[PipelineId]
	WHERE
		ce.[LocalExecutionId] = @ExecutionId
    AND ce.[BatchId] = @BatchId
		AND pe.[PipelineId] = @PipelineId
END;