CREATE PROCEDURE [procfwkHelpers].[SetDefaultPipelines]
AS
BEGIN
	DECLARE @Pipelines TABLE
		(
		[OrchestratorId] [INT] NOT NULL,
		[StageId] [INT] NOT NULL,
		[PipelineName] [NVARCHAR](200) NOT NULL,
		[LogicalPredecessorId] [INT] NULL,
		[Enabled] [BIT] NOT NULL,
		[LogicalUsageValue] VARCHAR (255) NOT NULL
		)

	INSERT @Pipelines
		(
		[OrchestratorId],
		[StageId],
		[PipelineName], 
		[LogicalUsageValue],
		[LogicalPredecessorId],
		[Enabled]
		) 
	VALUES 
		(1,1	,'Wait 1'				,'Daily Wait 1'				,NULL		,1),
		(1,1	,'Wait 2'				,'Daily Wait 2'				,NULL		,1),
		(1,1	,'Intentional Error'	,'Daily Intentional Error'	,NULL		,1),
		(1,1	,'Wait 3'				,'Daily Wait 3'				,NULL		,1),
		(1,2	,'Wait 4'				,'Daily Wait 4'				,NULL		,1),
		(1,2	,'Wait 5'				,'Daily Wait 5'				,1			,1),
		(1,2	,'Wait 6'				,'Daily Wait 6'				,1			,1),
		(1,2	,'Wait 7'				,'Daily Wait 7'				,NULL		,1),
		(1,3	,'Wait 8'				,'Daily Wait 8'				,1			,1),
		(1,3	,'Wait 9'				,'Daily Wait 9'				,6			,1),
		(1,4	,'Wait 10'				,'Daily Wait 10'				,9			,1),
		--speed
		(1,5	,'Wait 1'				,'Speed Wait 1'				,NULL		,0),
		(1,5	,'Wait 2'				,'Speed Wait 2'				,NULL		,0),
		(1,5	,'Wait 3'				,'Speed Wait 3'				,NULL		,0),
		(1,5	,'Wait 4'				,'Speed Wait 4'				,NULL		,0);

	MERGE INTO [procfwk].[Pipelines] AS tgt
	USING 
		@Pipelines AS src
			ON tgt.[PipelineName] = src.[PipelineName]
				AND tgt.[StageId] = src.[StageId]
				AND tgt.[LogicalUsageValue] = src.[LogicalUsageValue]
	WHEN MATCHED THEN
		UPDATE
		SET
			tgt.[OrchestratorId] = src.[OrchestratorId],
			tgt.[LogicalPredecessorId] = src.[LogicalPredecessorId],
			tgt.[Enabled] = src.[Enabled]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT
			(
			[OrchestratorId],
			[StageId],
			[PipelineName],
			[LogicalUsageValue],
			[LogicalPredecessorId],
			[Enabled]
			)
		VALUES
			(
			src.[OrchestratorId],
			src.[StageId],
			src.[PipelineName],
			src.[LogicalUsageValue],
			src.[LogicalPredecessorId],
			src.[Enabled]
			)
	WHEN NOT MATCHED BY SOURCE THEN
		DELETE;	
END;