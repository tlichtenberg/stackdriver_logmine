from argparse import ArgumentParser
from boom.common.utils import init_logger
from difflib import SequenceMatcher
import os
import re
import sys


class FilterFinder():
    def __init__(self, input_list=None):
        self.logger = init_logger('FilterFinder')
        self.content = input_list
        self.matches = []

        if not input_list:
            self.args = self.arguments()
            self.min_len = self.args.min_len
            self.max_len = self.args.max_len
            if self.args.input_file:
                self.content = open(self.args.input_file, 'r').readlines()
            else:
                self.logger.error('--input_file required if no input_list')
                sys.exit(-1)
        else:
            self.min_len = 16
            self.max_len = 64

    def arguments(self):
        parser = ArgumentParser()
        parser.add_argument('--input_file', default=None)
        parser.add_argument('--min_len', default=16)
        parser.add_argument('--max_len', default=64)
        return parser.parse_args()

    def run(self):
        self.content = list(self.find_matching_substrings(self.content))
        self.logger.info('=================')
        self.logger.info('final content length: {}'.format(len(self.content)))
        for f in self.content:
            self.logger.info(f)
        return self.content

    def find_matching_substrings(self, the_content):
        """
        find matching substrings between each pair of strings in the given list of strings (self.content)
        :return:
        """
        results = []
        # reduce the set by removing exact duplicates
        as_set = set(the_content)
        the_content = list(as_set)
        for i in range(len(the_content)):
            the_content[i] = self.prep_entry(the_content[i])

        for i in range(len(the_content)-1):
            if i % 10 == 0:
                self.logger.debug('line {} of {}'.format(i,len(the_content)-1))
            for j in range(1, len(the_content)-2):
                self.logger.debug('\n.... now comparing: "{} vs {}"'.format(the_content[i],the_content[j]))
                # use SequenceMatcher from the difflib standard library
                seq_result = SequenceMatcher(None, the_content[i], the_content[j]).get_matching_blocks()
                best_size = 0
                a_start = 0
                # try to find the longest match within our size limitations
                for t in seq_result:
                    if t.size > self.min_len:
                        if t.size > best_size:
                            best_size = t.size
                            a_start = t.a
                # if we have a best match, extract it
                if best_size > 0:
                    self.logger.debug(seq_result)
                    start_index = a_start
                    if best_size > self.max_len:  # truncate if needed
                        end_index = a_start + self.max_len
                    else:
                        end_index = start_index + best_size
                    result = the_content[i][start_index:end_index]
                    result = self.prep_entry(result)  # strip unwanted characters and extra white space
                    self.logger.debug('comparing: "{} vs {}"'.format(the_content[i],the_content[j]))
                    self.logger.debug('result is {}, strlen = {}'.format(result, len(result)))

                    # see if it shares a common prefix with an existing result. if so, use that common prefix instead
                    for r in range(len(results)):
                        common_prefix = os.path.commonprefix([result, results[r]])
                        if common_prefix:
                            common_prefix = self.prep_entry(common_prefix)
                            if self.is_in_range(common_prefix):
                                self.logger.debug('found a common prefix, using {}'.format(results[r]))
                                results[r] = common_prefix
                                result = None
                                break

                    if result and self.is_in_range(result):
                        self.logger.debug('appending: {}'.format(result))
                        self.logger.debug(":".join("{:02x}".format(ord(c)) for c in result))
                        results.append(result)

        result_set = list(set(results))
        return result_set

    def is_in_range(self, s1):
        """
        checks to see if the strlen falls within the min/max range
        :param s1: the string to check
        :return: true or false
        """
        if len(s1) > self.min_len:
            if len(s1) < self.max_len:
                return True
        return False

    def initialize_twodlist(self, len1, len2):
        """
        utility to create an uneven two dimensional array and initialize with zeros
        :param len1: table width value
        :param len2: table height value
        :return: two dimentional array initialized with zeros
        """
        the_list=[]
        for row in range(len1):
            the_list.append([0]*len2)
        return the_list

    def prep_entry(self, entry):
        """
        same as in LogMine, strips unwanted chars and removes multiple white spaces within line
        :param entry: the log entry payload
        :return: sanitizing string
        """
        entry = entry.strip().replace('[', '').replace(']','').replace("'","").replace("\"","")
        entry = re.sub( '\s+', ' ', entry ).strip()
        self.logger.debug('len(item): {}, item: {}'.format(len(entry),entry))
        return entry


if __name__ == "__main__":
    """
    python -m boom.tools.logs.FilterFinder --input_file test_set.txt 
    """
    ff = FilterFinder()
    ff.run()