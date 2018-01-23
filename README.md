**LogMine**

A Stackdriver Log mining tool

Jan 22, 2018

Tom Lichtenberg


*  **OVERVIEW**

The Theory of Artificial Ignorance

The process of establishing invariants and subsequently detecting new events is not new,
    but has been around for decades, pioneered by Unix sysadmins and known as Artificial Ignorance.

In essence, they used grep to detect patterns in loglines, then stored those already seen in a
    "stoplist", then later used that stoplist in a subsequent call such as 'grep -v -f stoplist'
    to ignore those previously seen patterns or loglines and report only on new ones.

This is done progressively. Each time you review a batch of log files, call out any previously unseen entries
    and add them to the stoplist. The stoplist builds up over time while producing a new "newlist" each time.
    Inspect or report on the newlist and save the stoplist each iteration.

Along the way some log entries may vary in details but not critically. The logmine tool allows these entries to be grouped
    together through a "filterfile". For example, a call to a method FindEvents() may return a different user name or
    number of messages found, but we are mainly interested in the number of FindEvents calls. The filter file can grow over time,
    like the stoplist. It is not updated automatically like the stoplist, but must be modified manually.

* **USAGE**


Use a virtual environment and pip to install the required modules:

  virtualenv env
  source env/bin/activate
  pip install -r requirements.txt

sample run args:

python -m logmine.LogMine
--query
--parse
--process
--log_filter 'resource.type=dataflow_step resource.labels.job_id=DATAFLOW_JOB_NAME severity>=WARNING'
--output_dir ./logmine
--source my-dataflow-job
--limit 100
--range_in_minutes 10
--output_file my-dataflow-job.log
--project my-gcp-project

*  **IDENTIFY PROJECT SOURCES**

You'll want to distinguish between project sources so that you can have stoplists per source. Log entries
  vary considerably between projects

e.g. source = a GKE container or a Dataflow job would produce output files such as
                stoplist_my_container
                stoplist_my_dataflow_job

and newlist_* of same

* **FILTER COMMON LOG ENTRIES**

A filter file is a text file containing a list of strings from commonly found log entries, used to condense them
  into a common set where individual differences won't matter. for example, from a container log file there
  could be many log entries like this:

  'Completed call to FindEvents(elapsedMs=98 receivedMessageCount=1 sentMessageCount=1 statusCode=OK)'

where the message count and elapsed time fields will differ, but we don't care about that. We want to count them all
  as if they were exactly the same, so the filter file would include a line like this:

   Completed call to FindEvents

and any log line finding that substring will be treated as the same entry

filterfiles must be named in the pattern:

   '{}/filterfile_{}.txt'.format(self.args.output_dir, self.args.source)

for example:

   /tmp/filterfile_my_container.txt


* **DATAFLOW JOBS - A SPECIAL CASE**

The Stackdriver client's log filter requires the job_id but we only know the job_name. The same job name acquires
  a new job_id on every launch and Stackdriver logs don't map these intrinsically based on the name. We can use
  the Dataflow API client to find the job_id through the job name.

--log_filter 'resource.type=dataflow_step resource.labels.job_id=DATAFLOW_JOB_NAME severity>=WARNING'

when the tool sees the string "DATAFLOW_JOB_NAME" in the --log_filter argument, it will look it up and replace that string with the job_id, if it can find it. The actual dataflow job name should be passed as the --source parameter

   --source my_dataflow_job

   --log_filter 'resource.type=dataflow_step resource.labels.job_id=DATAFLOW_JOB_NAME severity>=WARNING'