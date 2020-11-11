CREATE TABLE [procfwk].[BatchPipelineLink] (
    [BatchId]              INT NOT NULL,
    [StageId]              INT NOT NULL,
    [PipelineId]           INT NOT NULL,
    [LogicalPredecessorId] INT NULL,
    [Enabled]              BIT CONSTRAINT [DF_BatchPipelineLink_Enabled] DEFAULT ((1)) NOT NULL,
    CONSTRAINT [PK_BatchPipelineLink] PRIMARY KEY CLUSTERED ([BatchId] ASC, [StageId] ASC, [PipelineId] ASC),
    CONSTRAINT [CK_BatchPipelineLink_PipelineId_DifferentTo_LogicalPredecessorId] CHECK ([PipelineId]<>[LogicalPredecessorId]),
    CONSTRAINT [FK_BatchPipelineLink_Batches] FOREIGN KEY ([BatchId]) REFERENCES [procfwk].[Batches] ([BatchId]),
    CONSTRAINT [FK_BatchPipelineLink_Pipelines_LogicalPredecessorId] FOREIGN KEY ([LogicalPredecessorId]) REFERENCES [procfwk].[Pipelines] ([PipelineId]),
    CONSTRAINT [FK_BatchPipelineLink_Pipelines_PipelineId] FOREIGN KEY ([PipelineId]) REFERENCES [procfwk].[Pipelines] ([PipelineId]),
    CONSTRAINT [FK_BatchPipelineLink_Stages] FOREIGN KEY ([StageId]) REFERENCES [procfwk].[Stages] ([StageId])
);

