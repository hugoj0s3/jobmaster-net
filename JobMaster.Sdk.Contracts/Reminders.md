# Reminders and Improvements.

## Code clean up.
# Consolidate the GetJobDefinitionId and GetJobHandlerTypeFromId. In some places we are get definition id without using the method and filtering the attribute directly.
# Improve the names and ids.
    # Validates dupes names in cluster by removing the -, . and _, : and make case insensitive. e.g joe-does is the same as joeDoes and joe_does and joe:does.
    # Don't use . or : for worknames and buckets use _ or -. : . is separator of segment.
    # Limit the cluster name to X characters the same for agent connection id

## Performance
## Do upsert specfic for each db. avoid 2 roundtrips - get and update/insert. the version can also be very specific.
## Reduce the roundtrips instead of using partition lock do something like lockandfecth 

## Scheduler
### Ignoring the recurring schedule if the handle does not exists anymore and log error. It will avoid to explode later when the job is scheduled.
### Review lockers we might not need ~~saving~~ anymore and ideally it should be per resource not action, but keep this design for now if it is difficulty.

## Clean ups
### Review documentation all class

## Runners
### The stop gracefully should be reviewed. it is not quite good.
### Also the immediate stop should be reviewed. it is not quite good as well.

## Ideas
### Create a JobExecution entity align JobExecution log subject. If job run many times we have all execution start-end and result.
    -- JobExecution
        -- Id        
        -- JobId
        -- StartedAt
        -- CompletedAt
        -- Status

### JobMasterSchedulerClusterAware creates a timer or something that ensure the job/recurring schedule is saved. 
    - Hold in memory and then ensure it is saved after X minutes
    = Insert it in a bulk into the generic repository.
    = Consider saved saved in partition of X jobs. e.g 100 jobs represent single record. 
    = Flush like a log after X minutes and X number of items.
    = Create a runner that get data from the group id that the records were stored and move to master db.
    = If it works consider using this on SaveOperation class also, but it is tricky since can conflict with the JobMasterSchedulerClusterAware
    
### Explorer the posibility to schedule based JobDefinition.
    - it will be class that define timeout all job configration. 
    - it for advance scenario when the user want truly separation of consumer and publisher. 
    - The handler code will leave on the consumer JobHandlerA : IJobHandler<DefinitionJobA>, but keep the handler direct as well for simple scnerario. (maybe it can be on version 2)


