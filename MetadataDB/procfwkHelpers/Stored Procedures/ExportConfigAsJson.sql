CREATE PROCEDURE procfwkHelpers.ExportConfigAsJson
(
  @prettyPrintJson BIT = 1
 ,@jsonNestingSpaces INT = 4
 ,@json NVARCHAR(MAX) OUTPUT
)
AS
BEGIN
  IF @jsonNestingSpaces IS NULL
    SET @jsonNestingSpaces = 4

  SET @json = (
                SELECT (SELECT p.PropertyName AS [name]
                              ,p.PropertyValue AS [value]
                              ,p.Description AS [description]
                        FROM procfwk.Properties AS p
                        WHERE p.ValidTo IS NULL
                        FOR JSON PATH
                       )AS properties
                      ,(SELECT t.TenantId AS tenantId
                              ,t.Name AS [name]
                              ,t.Description AS [description]
                        FROM procfwk.Tenants AS t
                        FOR JSON PATH
                       )AS tenants
                      ,(SELECT s.SubscriptionId AS subscriptionId
                              ,s.Name AS [name]
                              ,s.Description AS [description]
                              ,s.TenantId AS tenantId
                        FROM procfwk.Subscriptions AS s
                        FOR JSON PATH
                       )AS subscriptions
                      ,(SELECT o.OrchestratorName AS [name]
                              ,o.OrchestratorName AS [slug]
                              ,o.OrchestratorType AS [type]
                              ,o.IsFrameworkOrchestrator AS isFrameworkOrchestrator
                              ,o.ResourceGroupName AS resourceGroupName
                              ,o.SubscriptionId AS subscriptionId
                              ,o.Description AS [description]
                        FROM procfwk.Orchestrators AS o
                        FOR JSON PATH
                       )AS orchestrators
                      ,(SELECT sp.PrincipalName AS [name]
                              ,sp.PrincipalId AS principalId
                              ,CASE WHEN sp.PrincipalId IS NOT NULL THEN '{{principal_secret_' + Cast(PrincipalId AS CHAR(36)) + '}}' ELSE NULL END AS [secret]
                              ,sp.PrincipalIdUrl AS principalIdKeyVaultUrl
                              ,sp.PrincipalSecretUrl AS secretKeyVaultUrl
                        FROM dbo.ServicePrincipals AS sp
                        FOR JSON PATH
                       )AS servicePrincipals
                      ,(SELECT r.Name AS [name]
                              ,r.EmailAddress AS emailAddress
                              ,r.MessagePreference AS messagePreference
                              ,r.Enabled AS [enabled]
                        FROM procfwk.Recipients AS r
                        FOR JSON PATH
                       )AS alertRecipients
                      ,(SELECT b.BatchId AS id
                              ,b.BatchName + '' AS [name]
                              ,b.BatchDescription AS [description]
                              ,b.Enabled AS [enabled]
                              ,Json_Query(Replace(Replace((SELECT s.StageName
                                                           FROM procfwk.BatchStageLink AS bsl
                                                           INNER JOIN procfwk.Stages AS s
                                                             ON s.StageId = bsl.StageId
                                                           WHERE bsl.BatchId = b.BatchId
                                                           FOR JSON AUTO
                                                          )
                                        ,'{"stageName":', ''), '}', '')
                                        )AS stages
                        FROM procfwk.Batches AS b
                        FOR JSON PATH
                       )AS batches
                      ,(SELECT s.StageName AS [name]
                              ,s.StageDescription AS [description]
                              ,s.Enabled AS [enabled]
                        FROM procfwk.Stages AS s
                        FOR JSON PATH
                       )AS stages
                      ,(SELECT --p.PipelineId AS Id
                               p.PipelineName AS [name]
                              ,p.Enabled AS [enabled]
                              ,p.LogicalUsageValue AS logicalUsageValue
                              ,(SELECT plp.LogicalUsageValue
                                FROM procfwk.Pipelines AS plp 
                                WHERE plp.PipelineId = p.LogicalPredecessorId
                               )AS logicalPredecessorLogicalUsageValue
                              ,s.StageName AS stageName
                              ,o.OrchestratorName AS orchestratorSlug
                              ,sp.PrincipalName AS servicePrincipalName
                              ,(SELECT pp.ParameterName AS parameterName
                                      ,pp.ParameterValue AS parameterValue
                                FROM procfwk.PipelineParameters AS pp
                                WHERE pp.PipelineId = p.PipelineId
                                FOR JSON PATH
                               )AS [parameters]
                              /*
                                This exports all PipelineAuthLink records, not just the SP used for the orchestrator defined on the pipeline

                              ,(SELECT df2.OrchestratorName AS orchestratorName
                                      ,sp2.PrincipalName AS principalName
                                FROM procfwk.PipelineAuthLink AS pal2
                                INNER JOIN procfwk.Orchestrators AS df2
                                  ON df2.OrchestratorId = pal2.OrchestratorId
                                INNER JOIN dbo.ServicePrincipals AS sp2
                                  ON sp2.CredentialId = pal2.CredentialId
                                WHERE pal2.PipelineId = p.PipelineId
                                FOR JSON PATH
                               )AS [authLink]
                              */
                              ,Json_Query(Replace(Replace((SELECT pdn.LogicalUsageValue
                                                           FROM procfwk.PipelineDependencies AS pd
                                                           INNER JOIN procfwk.Pipelines AS pdn
                                                             ON pdn.PipelineId = pd.PipelineId
                                                           WHERE pd.DependantPipelineId = p.PipelineId
                                                           FOR JSON PATH
                                                          )
                                         ,'{"LogicalUsageValue":', ''), '}', '')
                                         )AS dependsOnPipelines
                              ,(SELECT r.Name AS [name]
                                      ,pal.Enabled AS [enabled]
                                      ,Json_Query(Replace(Replace((SELECT ao.PipelineOutcomeStatus
                                                                   FROM procfwk.AlertOutcomes AS ao
                                                                   WHERE ao.BitValue = ao.BitValue & pal.OutcomesBitValue
                                                                   FOR JSON PATH
                                                                  )
                                                 ,'{"PipelineOutcomeStatus":', ''), '}', '')
                                                 )AS alertFor
                                FROM procfwk.PipelineAlertLink AS pal
                                INNER JOIN procfwk.Recipients AS r
                                  ON r.RecipientId = pal.RecipientId
                                WHERE pal.PipelineId = p.PipelineId
                                FOR JSON PATH
                               ) AS alertRecipients
                        FROM procfwk.Pipelines AS p
                        INNER JOIN procfwk.Stages AS s
                          ON s.StageId = p.StageId
                        INNER JOIN procfwk.Orchestrators AS o
                          ON o.OrchestratorId = p.OrchestratorId
                        LEFT JOIN procfwk.PipelineAuthLink AS pal
                          ON pal.PipelineId = p.PipelineId
                             AND pal.OrchestratorId = p.OrchestratorId
                        LEFT JOIN dbo.ServicePrincipals AS sp
                          ON sp.CredentialId = pal.CredentialId
                        FOR JSON PATH
                       )AS pipelines
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
              );

  IF IsNull(@prettyPrintJson, 1) = 1
    SET @json = dbo.PrettyPrintJson(@json, 4);

  RETURN 1;
END