CREATE TABLE [procfwk].[Batches] (
    [BatchId]          INT            IDENTITY (1, 1) NOT NULL,
    [BatchName]        NVARCHAR (200) NOT NULL,
    [BatchDescription] VARCHAR (4000) NULL,
    CONSTRAINT [PK_Batches] PRIMARY KEY CLUSTERED ([BatchId] ASC)
);


GO
CREATE UNIQUE NONCLUSTERED INDEX [UQ_Batches_BatchName]
    ON [procfwk].[Batches]([BatchName] ASC);

