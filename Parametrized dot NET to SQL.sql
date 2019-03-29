/* In create pipeline(PipelineDbManager=>UpdatePipeline) */


INSERT INTO dbo.[JobPhaseStatusJob] 
                        ([Ukey]
                        ,[JobId]
                        ,[JobUkey]
                        ,[JobPhaseStatusId]
                        ,[JobPhaseStatusUkey]
                        ,[JobPhaseStatusStateId]
                        ,[JobPhaseStatusStateUkey])
                            VALUES(NEWID() 
, @JobId 
, @jobUkey 
, ( SELECT TOP 1 [Id]
                                                            FROM JobPhaseStatus
                                                            WHERE [Ukey] = 'fb32aac0-0c5c-48dd-aba1-fd2e9dc1cb6a') 
, @JobPhaseStatusUkey00 
, ( SELECT TOP 1 [Id]
                                                            FROM JobPhaseStatusState
                                                            WHERE [Ukey] = '8e2e58f9-2fd6-45b9-b0d7-6dfe18ae0e78') 
, @JobPhaseStatusStateUkey00 
 ),(NEWID() 
, @JobId 
, @jobUkey 
, ( SELECT TOP 1 [Id]
                                                            FROM JobPhaseStatus
                                                            WHERE [Ukey] = '5a45e16c-b5c3-446b-88f0-c74d2393d5e4') 
, @JobPhaseStatusUkey01 
, ( SELECT TOP 1 [Id]
                                                            FROM JobPhaseStatusState
                                                            WHERE [Ukey] = '8e2e58f9-2fd6-45b9-b0d7-6dfe18ae0e78') 
, @JobPhaseStatusStateUkey01 
 ),(NEWID() 
, @JobId 
, @jobUkey 
, ( SELECT TOP 1 [Id]
                                                            FROM JobPhaseStatus
                                                            WHERE [Ukey] = 'da4e0e7e-fc11-4257-bf25-3f933389561c') 
, @JobPhaseStatusUkey02 
, ( SELECT TOP 1 [Id]
                                                            FROM JobPhaseStatusState
                                                            WHERE [Ukey] = '8e2e58f9-2fd6-45b9-b0d7-6dfe18ae0e78') 
, @JobPhaseStatusStateUkey02 
 )


 /* CODE */
         public void UpdatePipeline(Pipeline pipeline)
        {
            string deletePhaseStatusesQuery = String.Format(@"
                DELETE FROM {0}.[JobPhaseJob]
                WHERE [JobId] = @JobId;

                DELETE FROM {0}.[JobPhaseStatusJob]
                WHERE [JobId] = @JobId", SchemaName);

            if(pipeline.Phases != null)
            {
                List<JobPhase> JobPhases = pipeline.Phases.Where(x => x.SelectedStatuses.Count != 0).ToList();

                if(JobPhases.Count() != 0)
                {
                    IDbCommand command = MyConnection.CreateCommand();
                    command.AddParameter("JobId", pipeline.JobId, DbType.Int32);

                    StringBuilder phaseQuery = new StringBuilder(string.Format(@"  
                            DECLARE @jobUkey uniqueidentifier;
                            SELECT @jobUkey = [Ukey]
                            FROM [Profile]
                            WHERE [Id] = @JobId;

                            INSERT INTO {0}.[JobPhaseJob]
                                                            ([Ukey]
                                                            ,[JobId]
                                                            ,[JobUkey]
                                                            ,[JobPhaseId]
                                                            ,[JobPhaseUkey])
                            VALUES ", SchemaName));

                    StringBuilder statusQuery = new StringBuilder(string.Format(@"
                            INSERT INTO {0}.[JobPhaseStatusJob] 
                                                                ([Ukey]
                                                                ,[JobId]
                                                                ,[JobUkey]
                                                                ,[JobPhaseStatusId]
                                                                ,[JobPhaseStatusUkey]
                                                                ,[JobPhaseStatusStateId]
                                                                ,[JobPhaseStatusStateUkey])
                            VALUES", SchemaName));



                    for(int phaseIndex = 0; phaseIndex < JobPhases.Count(); phaseIndex++)
                    {
                        JobPhase phase = JobPhases[phaseIndex];

                        if(phase.SelectedStatuses.Count(x => x.JobPhaseStatusStateUkey != JobSetupConstants.InactiveStateUkey) > 0 && phaseIndex > 0)
                            statusQuery.Append(",");

                        string jobPhaseUkeyName = "@JobPhaseUkey" + phaseIndex;

                        phaseQuery.AppendFormat("({0},{1},{2},{3},{4}){5}"
                                    , "NEWID()"
                                    , "@JobId"
                                    , "@jobUkey"
                                    , string.Format(@"(SELECT TOP 1 [Id]
                                                       FROM JobPhase
                                                       WHERE [Id] = '{0}')", phase.Id)
                                    , jobPhaseUkeyName
                                    , phaseIndex < JobPhases.Count - 1 ? "," : string.Empty);

                        command.AddParameter(jobPhaseUkeyName, phase.Ukey, DbType.Guid);

                        for(int i = 0; i < phase.SelectedStatuses.Count; i++)
                        {
                            JobPhaseStatus status = phase.SelectedStatuses[i];

                            if(status.JobPhaseStatusStateUkey != JobSetupConstants.InactiveStateUkey)
                            {
                                string jobPhaseStatusUkeyName = string.Format("@{0}{1}{2}", "JobPhaseStatusUkey", phaseIndex, i);
                                string JobPhaseStatusStateUkey = string.Format("@{0}{1}{2}", "JobPhaseStatusStateUkey", phaseIndex, i);

                                statusQuery.AppendFormat("({0} \n, {1} \n, {2} \n, {3} \n, {4} \n, {5} \n, {6} \n ){7}"
                                        , "NEWID()"
                                        , "@JobId"
                                        , "@jobUkey"
                                        , string.Format(@"( SELECT TOP 1 [Id]
                                                            FROM JobPhaseStatus
                                                            WHERE [Ukey] = '{0}')", status.Ukey)
                                        , jobPhaseStatusUkeyName
                                        , string.Format(@"( SELECT TOP 1 [Id]
                                                            FROM JobPhaseStatusState
                                                            WHERE [Ukey] = '{0}')", status.JobPhaseStatusStateUkey)
                                        , JobPhaseStatusStateUkey
                                        , i < phase.SelectedStatuses.Count - 1 ? "," : string.Empty);

                                command.AddParameter(jobPhaseStatusUkeyName, status.Ukey, DbType.Guid);
                                command.AddParameter(JobPhaseStatusStateUkey, status.JobPhaseStatusStateUkey, DbType.Guid);
                            }

                        }
                    }

                    bool existStatuses = JobPhases.Select(x => x.SelectedStatuses.Count(y => y.JobPhaseStatusStateUkey != JobSetupConstants.InactiveStateUkey)).Sum() != 0;
                    var validQuery = existStatuses ? statusQuery.ToString().Replace(",,", ",") : string.Empty;
                    command.CommandText = string.Format(@"{0}; {1}; {2};", deletePhaseStatusesQuery, phaseQuery.ToString(), validQuery);
                    int rc = command.ExecuteNonQuery();
                    CreateInitalPipelinePhaseForJob(pipeline.JobId, pipeline.JobUkey);
                }
            }
        }