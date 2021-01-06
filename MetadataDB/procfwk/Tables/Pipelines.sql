CREATE TABLE [procfwk].[Pipelines] (
    [PipelineId]           INT            IDENTITY (1, 1) NOT NULL,
    [DataFactoryId]        INT            NOT NULL,
    [StageId]              INT            NOT NULL,
    [PipelineName]         NVARCHAR (200) NOT NULL,
    [LogicalPredecessorId] INT            NULL,
    [Enabled]              BIT            CONSTRAINT [DF_Pipelines_Enabled] DEFAULT ((1)) NOT NULL,
    [LogicalUsageValue]    VARCHAR (255)  NOT NULL,
    CONSTRAINT [PK_Pipelines] PRIMARY KEY CLUSTERED ([PipelineId] ASC),
    CONSTRAINT [FK_Pipelines_DataFactorys] FOREIGN KEY ([DataFactoryId]) REFERENCES [procfwk].[DataFactorys] ([DataFactoryId]),
    CONSTRAINT [FK_Pipelines_Pipelines] FOREIGN KEY ([LogicalPredecessorId]) REFERENCES [procfwk].[Pipelines] ([PipelineId]),
    CONSTRAINT [FK_Pipelines_Stages] FOREIGN KEY ([StageId]) REFERENCES [procfwk].[Stages] ([StageId]),
    CONSTRAINT [UQ_Pipelines_LogicalUsageValue] UNIQUE NONCLUSTERED ([LogicalUsageValue] ASC)
);



