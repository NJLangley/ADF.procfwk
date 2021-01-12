CREATE PROCEDURE [procfwk].[GetStages]
	(
	@ExecutionId UNIQUEIDENTIFIER
	)
AS
BEGIN
	SET NOCOUNT ON;

	--defensive check
	IF NOT EXISTS 
		( 
		SELECT
			1
		FROM 
			[procfwk].[CurrentExecution]
		WHERE
			[LocalExecutionId] = @ExecutionId
			AND ISNULL([PipelineStatus],'') <> 'Success'
		)
		BEGIN
			RAISERROR('Requested execution run does not contain any enabled stages/pipelines.',16,1);
			RETURN 0;
		END;

	SELECT
		[StageId] 
	FROM 
		[procfwk].[CurrentExecution]
	WHERE
		[LocalExecutionId] = @ExecutionId
		AND ISNULL([PipelineStatus],'') <> 'Success'
	GROUP BY
		[StageId]
	ORDER BY 
		Min(IsNull([StageRunOrder], StageId)) ASC
END;