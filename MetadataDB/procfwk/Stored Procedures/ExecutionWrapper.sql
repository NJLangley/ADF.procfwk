CREATE [procfwk].[ExecutionWrapper]
	(
  @CallingDataFactory NVARCHAR(200),
  @AdfParentPipelineRunId UNIQUEIDENTIFIER,
  @BatchName NVARCHAR(200)

	)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @RestartStatus BIT,
          @BatchId INT;

  SET @BatchId = ( SELECT TOP 1 BatchId FROM procfwk.Batches WHERE BatchName = @BatchName )
  IF @BatchId IS NULL
		BEGIN
			RAISERROR('The batch name does not exist. Check the batch name and re-run the batch.',16,1);
			RETURN 0;
		END;

	IF @CallingDataFactory IS NULL
		SET @CallingDataFactory = 'Unknown';

	--get restart overide property	
	SELECT @RestartStatus = [procfwk].[GetPropertyValueInternal]('OverideRestart')

	--check for running execution
	IF EXISTS
		(
		SELECT * FROM [procfwk].[CurrentExecution] WHERE [BatchId] = @BatchId AND ISNULL([PipelineStatus],'') = 'Running'
		)
		BEGIN
			RAISERROR('There is already an execution run of this batch in progress. Stop this via Data Factory before restarting.',16,1);
			RETURN 0;
		END;

    /***********************************************************************************************************************************
    TODO: Add check for running executions with same pipelines / params passed in, and stop those duplicate runs from occuring
    ***********************************************************************************************************************************/

	--reset and restart execution
	IF EXISTS
		(
		SELECT * FROM [procfwk].[CurrentExecution] WHERE [BatchId] = @BatchId AND ISNULL([PipelineStatus],'') <> 'Success'
		) 
		AND @RestartStatus = 0
		BEGIN
			EXEC [procfwk].[ResetExecution]
        @BatchId = @BatchId,
        @AdfParentPipelineRunId = @AdfParentPipelineRunId;
		END
	--capture failed execution and run new anyway
	ELSE IF EXISTS
		(
		SELECT * FROM [procfwk].[CurrentExecution] WHERE [BatchId] = @BatchId
		)
		AND @RestartStatus = 1
		BEGIN
			EXEC [procfwk].[UpdateExecutionLog]
				@PerformErrorCheck = 0, --Special case when OverideRestart = 1;
        @BatchId = @BatchId;

			EXEC [procfwk].[CreateNewExecution] 
				@CallingDataFactoryName = @CallingDataFactory,
        @AdfParentPipelineRunId = @AdfParentPipelineRunId,
        @BatchId = @BatchId;
		END
	--no restart considerations, just create new execution
	ELSE
		BEGIN
			IF EXISTS --edge case, if all current workers succeeded, or some other not understood situation, archive records
				(
				SELECT * FROM [procfwk].[CurrentExecution]
				)
				BEGIN
					EXEC [procfwk].[UpdateExecutionLog]
						@PerformErrorCheck = 0;
				END

			EXEC [procfwk].[CreateNewExecution] 
				@CallingDataFactoryName = @CallingDataFactory,
        @AdfParentPipelineRunId = @AdfParentPipelineRunId,
        @BatchId = @BatchId;
		END
END;