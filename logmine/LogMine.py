from argparse import ArgumentParser
from boom.common.utils import init_logger
from boom.common.utils import dynamite_parser
from boom.dynamite.DynamitePod import DynamitePod
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from importlib import import_module
import re
import smtplib
import traceback

FILTER2 = True

if FILTER2:
    from FilterFinder import FilterFinder

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
        self.logger = init_logger('LogMine')
        self.args = self.arguments()
        self.results = list()
        self.new_data = dict()
        self.stopfile_data = {}
        self.filterfile_data = []
        self.min_len = 16
        self.max_len = 64
        self.max_substring_length = 128  # truncate log entry payloads to 1st N characters if FILTER2
        self.df = None  # pandas dataframe

        self.dynamite = DynamitePod(pod=self.args.pod,
                                    realm=self.args.realm,
                                    zone=self.args.zone,
                                    project=self.args.project,
                                    cloud=self.args.cloud,
                                    user=self.args.user)

        if self.args.send_file:
            self.send_requested_file()
        else:
            self.logger.info('do query')
            entries = self.do_query()
            self.logger.debug('list entries')
            for entry in entries:
                self.logger.debug(entry)
            self.logger.info('output raw results to file')
            self.write_raw_results('{}/{}'.format(self.args.output_dir, self.args.output_file), entries)

            # look for new input (based on what is not in the stopfile) and report it, then
            # update the stopfile with the new input
            # use optional output_file arg in case of pre-existing file (no query and no parse)
            self.logger.info('do process')
            self.do_process(entries)

            # send notification emails if sender, recipients and new_data        if self.args.sender and self.args.recipients and self.new_data:
            if self.new_data:
                self.logger.info('send notification(s)')
                for recipient in self.args.recipients:
                    self.do_sendmail(self.args.sender, recipient, self.new_data)

            # write out the new filter_file, including existing and new entries found by FilterFinder
            self.logger.info('write filter file')
            self.write_filter_file()

    def arguments(self):
        """
        argument parser
        :return: parsed args
        """
        parser = ArgumentParser(parents=[dynamite_parser()])
        parser.add_argument('--limit', default=100, type=int, help='number of log entries to retrieve')
        parser.add_argument('--max_entry_count', default=20, type=int, help='max per log entry to save to file')
        parser.add_argument('--output_dir', default='/tmp')
        parser.add_argument('--range_in_minutes', type=int, default=0)
        parser.add_argument('--sender', default='no-reply@nestlabs.com')
        parser.add_argument('--recipients', nargs='+', default=['tlichtenberg@google.com'])
        parser.add_argument('--output_file',  required=True)
        parser.add_argument('--send_file', default=False, type=bool, help="request an email containing the file specified in output_dir and output_file")
        known_args = parser.parse_known_args()

        # dependent args
        parser.add_argument('--log_filter', required=known_args[0].send_file == False)
        parser.add_argument('--source', required=known_args[0].send_file == False,
                            help="for stopfile_<source>.json and newfile_<source>.json")

        return parser.parse_args()

    def do_query(self):
        """
        performs the stackdriver log query, putting annotated results into the self.results list
        :return:
        """
        # initialize raw_entries, filterfile_data and optional filter revisions
        raw_entries = []
        entries = []
        self.read_filter_file()
        self.do_filter_revisions()
        self.logger.info(self.args.log_filter)

        # get log entries
        try:
            raw_entries = self.get_log_entries()
        except:
            self.logger.error(traceback.format_exc())

        # generate live append to filterfile_data from the raw_entries by analyzing for common substrings
        payloads = []
        for entry in raw_entries:
            self.logger.debug('* * * RAW ENTRY: {}'.format(entry))
            payload = self.get_payload(entry)
            final_entry = self.prep_entry(payload)
            self.logger.debug('* * * FINAL ENTRY: len() {}, {}'.format(len(final_entry), final_entry))
            payloads.append(final_entry)

        if FILTER2:
            p2 = []
            for p in payloads:
                p = p[:self.max_substring_length]  # truncate for substring matching
                p2.append(p)
            self.logger.debug('len payloads list: {}'.format(len(p2)))
            if len(p2) > 0:
                finder = FilterFinder(p2)
                finder_list = finder.run()
                for item in finder_list:
                    self.logger.info("adding {} to filterfile_data".format(item))
                    self.filterfile_data.append(item)

        for payload in payloads:
            output = [payload]
            if output:
                output[0] = self.do_filter(output[0])
                self.logger.debug('payload: {}'.format(output))
                output.insert(0, datetime.fromtimestamp(entry.timestamp.seconds).strftime('%Y-%m-%dT%H:%M:%SZ'))
                output.insert(0, SEVERITY[entry.severity])
                entries.append(str(output))

        return entries

    def prep_entry(self, entry):
        """
        same as in FilterFinder, strips unwanted chars and removes multiple white spaces within line
        :param entry: the log entry payload
        :return: sanitizing string
        """
        entry = entry.strip().replace('[', '').replace(']','').replace("'","").replace('"',"")
        entry = re.sub( '\s+', ' ', entry ).strip()
        self.logger.debug('len(item): {}, item: {}'.format(len(entry),entry))
        return entry

    def get_payload(self, entry):
        """
        in the python logging api, the payload can be of several types. in go it's always just the entry.payload
        :param entry: the raw log entry
        :return: the actual payload
        """
        payload = ''
        if len(entry.text_payload) > 0:
            try:
                payload = str(entry.text_payload).replace(',','').replace('\n', '') # lose any commas in the entry string
            except:
                self.logger.error(traceback.format_exc())

        elif entry.json_payload:
            try:
                payload = str(entry.json_payload['message']).replace(',', '').replace('\n', '')
            except:
                self.logger.error(traceback.format_exc())

        elif entry.proto_payload:
            class_ = getattr(import_module('google.protobuf.any_pb2'), 'Any')  # class reference
            try:
                if entry.proto_payload.type_url.find('AuditLog') < 0:
                    rv = class_()  # instantiate class instance
                    rv.ParseFromString(entry.proto_payload.value)  # decode Any.value
                    payload = str(rv)
                else:
                    payload = str(entry.proto_payload.value).replace('\n', '')
            except:
                self.logger.error(traceback.format_exc())

        return payload

    def do_filter(self, log_entry):
        """
        filterfile_data contains substrings of common log entries. if the given entry finds one of those
        substrings, replace the entry with that substring.
        for example:

        :param log_entry: a log entry
        :return:
        """
        for line in self.filterfile_data:
            line = line.rstrip('\n').strip()
            self.logger.debug('checking line {}'.format(line))
            if log_entry.find(line) >= 0:
                self.logger.debug('found line {}'.format(line))
                log_entry = line
                break
        return log_entry

    def do_filter_revisions(self):
        """
         optional filter revisions for timestamp and dataflow job name as applicable

         for dataflow jobs we need the job_id not the job name
         the caller should provide a dataflow job name as the --source arg
         for example:
        --source your-data-flow-job-name
         --log_filter 'resource.type=dataflow_step resource.labels.job_id=DATAFLOW_JOB_NAME'
        :return:
        """
        if self.args.log_filter.find('DATAFLOW_JOB_NAME') >= 0:
            job = self.dynamite.dataflow_client.get_job_by_name(self.args.source)
            if job:
                self.args.log_filter = self.args.log_filter.replace('DATAFLOW_JOB_NAME', job['id'])
            else:
                raise Exception('did not find active dataflow job for --source: {}'.format(self.args.source))

        if self.args.range_in_minutes > 0:
            start_time = (datetime.now() - timedelta(minutes=self.args.range_in_minutes)).strftime('%Y-%m-%dT%H:%M:%SZ').upper()
            self.args.log_filter += " timestamp > \"{}\"".format(start_time)

    def get_log_entries(self):
        """
        interface to google.cloud.logging client.list_entries
        """
        _, _, raw_entries = self.dynamite.logging.get_log_entries(log_filter=self.args.log_filter, limit=self.args.limit)
        self.logger.debug('len(raw_entries): {}'.format(len(raw_entries)))
        return raw_entries

    def write_raw_results(self, outfile=None, entries=None):
        """
        write to disk
        :param outfile: optional output file path. defaults to args.output_file
        :param entries: list of [SEVERITY, TIMESTAMP, TEXT] log entries
        :return:
        """
        if entries:
            if not outfile:
                outfile = self.args.output_file

            # get rid of hex code stuff if there is any and write to file
            outfile_list = outfile
            with open(outfile_list, 'w') as f:
                for r in entries:
                    r = str(re.sub(r"\\x[0-9a-fA-F]{2}", "", str(r)))
                    f.write('{}\n'.format(r))

    def do_process(self, entries):
        """
        :param entries: list of [SEVERITY, TIMESTAMP, TEXT] log entries

        process the log entries against the input stopfile_data
        discover new data and report it
        overwrite the stopfile and write the newfile to disk
        :return:
        """

        # read in stopfile
        self.read_stop_file()

        data_counts = {}

        # iterate through the log entry dataset
        # for this we are only interested in the TEXT portion of the log entries
        # and in that, we want the first part (the log entry, not its count list)
        for entry in entries:
            row = entry.split(',')
            txt = row[2].strip()
            items = txt.rsplit(":")
            item = self.prep_entry(items[0])
            if len(item) < self.min_len or len(item) > self.max_len:
                continue
            # add to counts of unique keys in this dataset
            if data_counts.has_key(item):
                data_counts[item] += 1
            else:
                data_counts[item] = 1

        # add new counts to existing stopfile entries
        # or create new stopfile entry if new data
        for k,v in data_counts.iteritems():
            if len(k) == 0:
                continue
            self.logger.debug('k = {}, v = [{}]'.format(k, v))
            if k in self.stopfile_data.keys():
                self.stopfile_data[k].append(v)
                if len(self.stopfile_data[k]) > self.args.max_entry_count:
                    del self.stopfile_data[k][:self.args.max_entry_count]  # keep the last max_entry_count elements only
            else:
                self.stopfile_data[k] = []
                self.stopfile_data[k].append(v)
                self.new_data[k] = [v]

        # write out new stopfile
        new_stopfile = []
        for k, v in self.stopfile_data.iteritems():
            new_stopfile.append("{}:{}".format(k,str(v)))
        self.write_stop_file(new_stopfile)

        # report new data and write out newfile
        new_newfile = []
        for k, v in self.new_data.iteritems():
            new_newfile.append("{}:{}".format(k,str(v)))
        self.write_new_file(new_newfile)

    def write_text_file(self, outfile, lines):
        """
        utility to write a list of lines to a text file
        :param outfile: path to the file
        :param lines: list of text lines
        :return:
        """
        f = open(outfile, 'w')
        for line in lines:
            f.write("{}\n".format(line))
        f.flush()
        f.close()

    def read_text_file(self, infile):
        """
        utility to read text file into a list
        :param infile: the file path
        :return: list of text lines
        """
        lines = []
        raw_lines = open(infile, 'r').readlines()
        for r in raw_lines:
            lines.append(r.replace('\n',''))
        return lines

    def write_stop_file(self, stopfile):
        """
        writes the stopfile entries to a text file
        :param stopfile: the stopfile entries
        :return:
        """
        outfile = '{}/stopfile_{}.txt'.format(self.args.output_dir, self.args.source)
        self.write_text_file(outfile, stopfile)

    def read_stop_file(self, infile=None):
        """
        read the stopfiles from disk and parse into the self.stopfiles_data dictionary
        :return:
        """
        try:
            if not infile:
                infile = '{}/stopfile_{}.txt'.format(self.args.output_dir, self.args.source)
            lines = self.read_text_file(infile)
            for l in lines:
                counts = []
                line = l.split(":")
                # line 0 is the log entry, line 1 is a str list of counts that needs to be turned back into an int list
                value = self.prep_entry(line[1])
                values = value.split(',')
                for v in values:
                    counts.append(int(v))
                self.stopfile_data[line[0]] = counts
        except:
            self.stopfile_data = {}

    def write_new_file(self, newfile):
        """
        writes the newfile entries to a text file
        :param newfile: the newfile entries
        :return:
        """
        outfile = '{}/newfile{}.txt'.format(self.args.output_dir, self.args.source)
        self.write_text_file(outfile, newfile)

    def read_filter_file(self, infile=None):
        """
        reads in an existing filterfile or initializes an empty list for that field
        """
        if not infile:
            infile = '{}/filterfile_{}.txt'.format(self.args.output_dir, self.args.source)
        try:
            self.filterfile_data = []
            with open(infile) as f:
                data = f.readlines()
                for d in data:
                    self.filterfile_data.append(d.rstrip('\n'))
        except:
            self.filterfile_data = []

    def write_filter_file(self):
        """
        writes out a new filterfile with existing and additional filters found
        """
        outfile = '{}/filterfile_{}.txt'.format(self.args.output_dir, self.args.source)
        self.write_text_file(outfile, self.filterfile_data)

    def send_requested_file(self):
        """
        read and format requested stop or filter files and send as email to recipients
        :return:
        """
        infile = '{}/{}'.format(self.args.output_dir, self.args.output_file)
        if infile.find('filterfile') >= 0:
            self.read_filter_file(infile)
            new_data = {}
            for item in self.filterfile_data:  # format into a dictionary for sendmail processing
                new_data[item] = 1
        elif infile.find('stopfile') >= 0:
            self.read_stop_file(infile)
            new_data = self.stopfile_data
        else:
            self.logger.error('only stopfiles and filterfiles are supported by send_file')
            return

        subject = "(%s) Log Mine: %s - %s" % (self.args.project, infile, datetime.now().strftime("%Y-%m-%d"))

        self.logger.info('send notification(s)')
        for recipient in self.args.recipients:
            self.do_sendmail(self.args.sender, recipient, new_data, subject)

    def do_sendmail(self, sender, recipient, new_data, subject=None):
        """
            general email routine to notify recipients of new entries
        """
        body = "<body><table border=2><tr><td> Entries </td><td> Count </td></tr>"
        for key in sorted(new_data.keys()):
            body_part = "<tr><td>%s</td><td>%s</td></tr>" % \
                        (key, str(new_data[key]))
            body += body_part
        body += "</table></body>"
        if not subject:
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
python -m boom.tools.logs.LogMine \
--log_filter "resource.type="container" resource.labels.cluster_name="qa-trinity" severity>=WARNING" \
--output_dir ./logmine \
--source qa_trinity \
--limit 1000 \
--range_in_minutes 30 \
--max_entry_count 20 \
--output_file qa_trinity_output.log    


python -m boom.tools.logs.LogMine \
--send_file true \
--output_dir ./logmine \
--output_file stopfile_airflow_dyna.txt
    """

    lm = LogMine()