from argparse import ArgumentParser
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud.gapic.logging.v2 import logging_service_v2_client
from google.cloud import logging
from googleapiclient import discovery
from importlib import import_module
import json
import logging as logger
from oauth2client.client import GoogleCredentials
import pandas as pd
import re
import smtplib
import sys
import traceback

DEBUG = sys.flags.debug

SEVERITY = {
               0:  "DEFAULT",    # (0) The log entry has no assigned severity level",
              100: "DEBUG",      # (100) Debug or trace information",
              200: "INFO",       # (200) Routine information, such as ongoing status or performance",
              300: "NOTICE",     # (300) Normal but significant events, such asa configuration change",
              400: "WARNING",    # (400) Warning events might cause problems",
              500: "ERROR",      # (500) Error events are likely to cause problems",
              600: "CRITICAL",   # (600) Critical events cause more severe problems or outages",
              700: "ALERT",      # (700) A person must take an action immediately",
              800: "EMERGENCY",  # (800) One or more systems are unusable"
}


class LogMine():
    def __init__(self):
        self.logger = None
        self.init_logger('LogMine')
        self.args = self.arguments()
        self.results = list()
        self.new_data = dict()
        self.stopfile_data = {}
        self.filterfile_data = []
        self.df = None  # pandas dataframe
        self.logger_client = logging_service_v2_client.LoggingServiceV2Client()
        self.credentials = GoogleCredentials.get_application_default()
        self.dataflow_service = discovery.build('dataflow', 'v1b3', credentials=self.credentials)

        if self.args.query:
            """
              get the logs from stackdriver and write to stdout
            """
            self.do_query()  #
            for result in self.results:
                self.logger.debug(result)
            self.write_results('{}/{}'.format(self.args.output_dir, self.args.output_file))

        if self.args.parse:
            """
            parse the fetched logs and output to formatted file
            """
            self.do_parse('{}/{}'.format(self.args.output_dir, self.args.output_file)) # output from query is input to parse

        if self.args.process:
            """
            look for new input (based on a stopfile) and report it, then
            update the stopfile with the new input
            
            use optional output_file arg in case of pre-existing file (no query and no parse)
            """
            self.do_process('{}/{}'.format(self.args.output_dir, self.args.output_file))

        """
        send notification emails if sender, recipients and new_data are not None
        """
        if self.args.sender and self.args.recipients and self.new_data:
            for recipient in self.args.recipients:
                self.do_sendmail(self.args.sender, recipient, self.new_data)

    def arguments(self):
        parser = ArgumentParser()
        parser.add_argument('--query', action='store_true')
        parser.add_argument('--process', action='store_true')
        parser.add_argument('--parse', action='store_true')
        parser.add_argument('--project', required=True, help='your GCP project name')
        parser.add_argument('--output_dir', default='/tmp')
        parser.add_argument('--limit', default=100, type=int, help='number of log entries to retrieve')
        parser.add_argument('--max_entry_count', default=20, type=int, help='max per log entry to save to file')
        parser.add_argument('--range_in_minutes', type=int, default=0)
        parser.add_argument('--sender', default=None, help='your email sender')
        parser.add_argument('--recipients', nargs='+', default=[], help='list of email recipients')
        known_args = parser.parse_known_args()

        # dependent args
        parser.add_argument('--log_filter', required=known_args[0].query == True, help='log query filter')
        parser.add_argument('--output_file',  required=known_args[0].parse == True)
        parser.add_argument('--source', required=known_args[0].process == True,
                            help="for stopfile_<source>.json, newfile_<source>.json and filterfile_<source>.txt")

        return parser.parse_args()

    def do_query(self):
        """
        performs the stackdriver log query, putting annotated results into the self.results list
        :return:
        """
        # initialize raw_entries and filterfile_data
        self.read_filterfile()

        # do optional filter revisions for timestamp, limit and dataflow job name as applicable
        self.do_filter_revisions()
        self.logger.info(self.args.log_filter)

        try:
            raw_entries = self.get_log_entries(log_filter=self.args.log_filter, limit=self.args.limit)
            for entry in raw_entries:
                output = []
                self.logger.debug('* * * RAW ENTRY: {}'.format(entry))
                if len(entry.text_payload) > 0:
                    try:
                        # lose any commas and newlines in the entry string
                        output.append(str(entry.text_payload).replace(',','').replace('\n', ''))
                    except:
                        self.logger.error(traceback.format_exc())

                elif entry.json_payload:
                    try:
                        # lose any commas and newlines in the entry string
                        output.append(str(entry.json_payload['message']).replace(',', '').replace('\n', ''))
                    except:
                        self.logger.error(traceback.format_exc())

                elif entry.proto_payload:
                    class_ = getattr(import_module('google.protobuf.any_pb2'), 'Any')  # class reference
                    try:
                        if entry.proto_payload.type_url.find('AuditLog') < 0:
                            rv = class_()  # instantiate class instance
                            # decode Any.value
                            rv.ParseFromString(entry.proto_payload.value)
                            # lose any commas and newlines in the entry string
                            output.append(str(rv).replace(',', '').replace('\n', ''))
                        else:
                            # lose any commas and newlines in the entry string
                            output.append(str(entry.proto_payload.value).replace(',', '').replace('\n', ''))
                    except:
                        self.logger.error(traceback.format_exc())

                if output:
                    # use the filterfile to consolidate common entries
                    output[0] = self.do_filter(output[0])
                    self.logger.info('payload: {}'.format(output))
                    # prepend the severity and the timestamp into the results list for later pandas dataframe use
                    output.insert(0, datetime.fromtimestamp(entry.timestamp.seconds).strftime('%Y-%m-%dT%H:%M:%SZ'))
                    output.insert(0, SEVERITY[entry.severity])
                    self.results.append(str(output))
        except:
            self.logger.error(traceback.format_exc())

    def do_parse(self, input_file):
        """ parse the raw log into a csv file, then into a pandas dataframe and write that out to file  """
        self.results = []

        # read in the raw log file
        infile = input_file
        r = open(infile, 'r').readlines()

        # format and output as csv file
        csv_file = infile + '.csv'
        r2 = open(csv_file, 'w')
        for rr in r:
            rr = rr.replace('[', '').replace(']','').replace("'","")
            self.results.append(rr)
            r2.write(rr + '\n')
        r2.flush()
        r2.close()

        # convert the revised csv file into a pandas dataframe for later use in parsing
        self.df = pd.read_csv(csv_file, sep=",", names=['Level','Time','Text'], engine='python', dtype=str)
        self.df.Time = pd.to_datetime(self.df.Time, infer_datetime_format=True) # format="%Y-%m-%dT%H:%M:%SZ")
        self.df['Text'] = self.df.Text.astype(str)
        self.logger.info(self.df.head(5))
        parsed_file = infile + '.df'
        self.write_results(parsed_file)

    def do_filter(self, log_entry):
        """
        filterfile_data contains substrings of common log entries. if the given entry finds one of those
        substrings, replace the entry with that substring. This is especially useful for entries that may
        differ in details (such as user names or ids or timestamps) that don't matter to the collection

        :param log_entry: a log entry
        :return:
        """
        for line in self.filterfile_data:
            line = line.rstrip('\n')
            self.logger.debug('checking line {}'.format(line))
            if log_entry.find(line) >= 0:
                self.logger.debug('found line {}'.format(line))
                log_entry = line
                break
        return log_entry

    def do_filter_revisions(self):
        """
         optional filter revisions for timestamp, limit and dataflow job name as applicable

         for dataflow jobs we need the job_id not the job name
         the caller should provide a dataflow job name as the --source arg
         for example:
         --source your-data-flow-job-name
         --log_filter 'resource.type=dataflow_step resource.labels.job_id=DATAFLOW_JOB_NAME severity>=WARNING'
        :return:
        """
        if self.args.log_filter.find('DATAFLOW_JOB_NAME') >= 0:
            job = self.get_dataflow_job_by_name(self.args.source)
            if job:
                self.args.log_filter = self.args.log_filter.replace('DATAFLOW_JOB_NAME', job['id'])
            else:
                raise Exception('did not find active dataflow job for --source: {}'.format(self.args.source))

        # add a time range if range_in_minutes is specified in the args, as timestamp > (NOW minus N minutes specified)
        if self.args.range_in_minutes > 0:
            start_time = (datetime.now() - timedelta(minutes=self.args.range_in_minutes)).strftime('%Y-%m-%dT%H:%M:%SZ').upper()
            self.args.log_filter += " timestamp > \"{}\"".format(start_time)

        self.logger.info('self.args.log_filter: {}'.format(self.args.log_filter))

    def write_results(self, outfile=None):
        """
        write to disk
        :param outfile: optional output file path. defaults to args.output_file
        :return:
        """
        if not outfile:
            outfile = self.args.output_file

        if self.results:
            outfile_list = outfile
            with open(outfile_list, 'w') as f:
                for r in self.results:
                    # get rid of hex code stuff if there is any or substitute your own regex filtering here
                    r = str(re.sub(r"\\x[0-9a-fA-F]{2}", "", str(r)))
                    # write to the output file
                    f.write('{}\n'.format(r))

    def do_process(self, path=None):
        """
        :param path: if previous query and parse, then use the *.df file from parse
                     else load the file provided by path (expected to be a dataframe in the parsed format)

        process the parsed file against the input stopfile_data
        discover new data and report it
        overwrite stopfile and write newfile to disk
        :return:
        """

        if self.df.empty:
            if path:
                try:
                    self.df = pd.read_csv(path, index_col=0, parse_dates=True)
                except:
                    self.logger.error('no data to process')
                    return
            else:
                self.logger.error('no data to process')
                return

        self.logger.debug(self.df.head(5))

        # read in the stopfile. this is a json file with previously seen entries and their counts
        self.read_stopfile()
        data_counts = {}

        # iterate through the log entry pandas dataframe dataset
        self.df['Text'] = self.df.Text.astype(str)
        for row in self.df['Text'].iteritems():
            item = row[1]
            self.logger.debug('row: {}'.format(item))
            # add to counts of unique keys in this dataset
            if data_counts.has_key(item):
                data_counts[item] += 1
            else:
                data_counts[item] = 1

        # add new counts to existing stopfile entries
        # or create new stopfile entry if new data
        for k,v in data_counts.iteritems():
            self.logger.debug('k = {}'.format(k))
            self.logger.debug('v = {}'.format(v))
            # if the entry has been seen before, add its data count
            if k in self.stopfile_data.keys():
                self.stopfile_data[k].append(v)
                # truncate the entry's data count in the stopfile according to the max_entry_count length
                if len(self.stopfile_data[k]) > self.args.max_entry_count:
                    del self.stopfile_data[k][:self.args.max_entry_count]  # keep the last max_entry_count elements only
            else:
                # if the entry hasn't been seen before, add it to the stopfile
                self.stopfile_data[k] = []
                self.stopfile_data[k].append(v)
                # if the entry hasn't been seen before, add it to the new_data list for optional email notifications
                self.new_data[k] = [v]

        # write out the new stopfile
        self.write_stopfile(self.new_data)

        # report on new data and write out the newfile
        self.write_newfile(self.new_data)

    def read_stopfile(self):
        """
        reads in an existing stopfile or initializes an empty dict for that field
        """
        infile = '{}/stopfile_{}.json'.format(self.args.output_dir, self.args.source)
        try:
            with open(infile) as f:
                self.stopfile_data = json.load(f)
        except:
            self.stopfile_data = {}

    def read_filterfile(self):
        """
        reads in an existing filterfile or initializes an empty list for that field
        """
        infile = '{}/filterfile_{}.txt'.format(self.args.output_dir, self.args.source)
        try:
            with open(infile) as f:
                self.filterfile_data = f.readlines()
        except:
            self.filterfile_data = []

    def write_stopfile(self, new_data):
        """
        writes the new stopfile, including new entries
        :param new_data: dictionary of entries not previously seen in stopfile
=        """
        outfile = '{}/stopfile_{}.json'.format(self.args.output_dir, self.args.source)
        self.stopfile_data.update(new_data)

        with open(outfile, 'w') as f:
            json.dump(self.stopfile_data, f)

    def write_newfile(self, new_data):
        """
        writes the new newfile, consisting of entries not previously seen
        :param new_data: dictionary of entries not previously seen in stopfile
        :return:
        """
        outfile = '{}/newfile_{}.json'.format(self.args.output_dir, self.args.source)

        with open(outfile, 'w') as f:
            json.dump(new_data, f)

    def get_dataflow_job_by_name(self, job_name, page_size=5000):
        """
        returns the active dataflow job that matches the given name. returns None if the job is not found
        """
        page_token = None
        while True:
            jobs = self.dataflow_service.projects().jobs().list(projectId=self.args.project, pageSize=page_size, pageToken=page_token, filter='ACTIVE').execute()
            for job in jobs['jobs']:
                if job['name'].find(job_name) >= 0:
                    return job
            try:
                page_token = jobs['nextPageToken']
            except:
                break
        self.logger.error('job not found {}'.format(job_name))
        return None

    def get_log_entries(self, log_filter, limit=10, order_by=logging.DESCENDING):
        """
        use the logger_clitn interface to google.cloud.logging client.list_entries to retrieve log entries
        :param resource_names: a list, containing the project_id
        :param log_filter: required
        example: filter_='resource.type=\"container\" resource.labels.cluster_name=\"qa-trinity\" \"SecurityAudit\" timestamp >= "2016-08-17T11:00:00-07:00"')
        you can get this kind of filter from the Monitoring Logging UI, 'convert to advanced filter' dropdown option
        :param limit: max number of desired results
        :param order_by: descending (most-recent first) or ascending (oldest first)
        :return: log entries found
        """
        
        raw_entries = []
        if not log_filter:
            raise Exception('log_filter was not provided and is required')
        else:
            count = 0
            entries = self.logger_client.list_log_entries(resource_names=["projects/{}".format(self.args.project)], filter_=log_filter, order_by=order_by, page_size=limit)
            try:
                for entry in entries:
                    raw_entries.append(entry)
                    self.logger.info(entry)
                    count += 1
                    self.logger.debug('entry count {} of {}'.format(count, limit))
                    if count >= limit:
                        break
            except:
                self.logger.error(traceback.format_exc())

        self.logger.info('len(raw_entries): {}'.format(len(raw_entries)))
        return raw_entries

    def init_logger(self, logger_name):
        """
        initialize system logger to a friendly format
        :param logger_name:
        :return:
        """
        self.logger = logger.getLogger(logger_name)
        FORMAT = '%(asctime)s %(levelname)-6s %(filename)s: %(lineno)s - %(funcName)20s() %(message)s'
        logger.basicConfig(format=FORMAT)
        if DEBUG:
            self.logger.setLevel(logger.DEBUG)
        else:
            self.logger.setLevel(logger.INFO)

    def do_sendmail(self, sender, recipient, new_data):
        """
            general email routine to notify recipients of new entries
        """
        body = "<body><table border=2><tr><td> New Entries </td><td> Entry Count </td></tr>"
        for key in sorted(new_data.keys()):
            body_part = "<tr><td>%s</td><td>%s</td></tr>" % \
                        (key, str(new_data[key]))
            body += body_part
        body += "</table></body>"
        subject = "(%s) Log Mine: %s - %s" % (self.args.project, self.args.source, datetime.now().strftime("%Y-%m-%d"))

        self.logger.info(subject)
        self.logger.info(body)

        message = body
        server = smtplib.SMTP('localhost')
        server.set_debuglevel(0)

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient
        content = MIMEText(message, 'html')
        msg.attach(content)
        self.logger.debug("send mail from {} to {}".format(sender, recipient))
        server.sendmail(sender, [recipient], msg.as_string())
        server.quit()

if __name__ == "__main__":
    """
# query, parse and analyze logs using "artificial ignorance"
python -m logmine.LogMine \
--query \
--parse \
--process \
--log_filter "resource.type="container" resource.labels.cluster_name="my-container" severity>=INFO" LIMIT 10\
--output_dir ./logmine \
--output_file my_container_output.log \
--max_entry_count 20 \
--limit 100 \
--source my-container \
--range_in_minutes 30 \
--project my_gcp_project   
    """

    lm = LogMine()
