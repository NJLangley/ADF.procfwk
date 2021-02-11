CREATE TABLE [procfwkHelpers].[MetadataSnapshot] (
    [Id]               INT             IDENTITY (1, 1) NOT NULL,
    [SnapshotDateTime] DATETIME2 (0)   CONSTRAINT [DF_procfwkHelpers_MetadataSnapshot_#SnapshotDateTime] DEFAULT (getdate()) NOT NULL,
    [SnapshotJson]     NVARCHAR (MAX)  NOT NULL,
    [Comments]         NVARCHAR (1000) NULL,
    CONSTRAINT [CK_procfwkHelpers_MetadataSnapshot_Is_#SnapshotDateTime_Json] CHECK (isjson([SnapshotJson])=(1))
);

