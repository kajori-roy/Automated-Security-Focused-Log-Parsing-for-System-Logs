import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from typing import List
import pandas as pd
import random
import string


class LogParser:
    
    def __init__(self, path: str, log_format: str, ct: float = 0.35, lower_bound: int = 0, output_dir: str = "result"):
        self.path = path
        self.log_format = log_format
        self.ct = ct
        self.lower_bound = lower_bound
        self.output_dir = output_dir
        self.log_clusters = []
        
    def random_event_id(self, length=8):
        characters = string.ascii_letters + string.digits
        return ''.join(random.choices(characters, k=length))

    def load_logs(self):
        loglist = []
        with open(self.path, 'r') as file:
            for line in file.readlines():
                log = self._parse_log(line.strip())
                if log:
                    loglist.append(log)
        # print(loglist)
        return loglist

    def _parse_log(self, log_line: str):
        try:
            log = re.search(self.log_format, log_line).groupdict()
            # log_line = " ".join(log.values())
            # print(log)
            return {k: v if v is not None else '' for k, v in log.items()}
        except Exception as e:
            log = re.search('^(?P<Month>.*?)\s+(?P<Date>.*?)\s+(?P<Time>.*?).*:\s+(?P<Content>.*?)$', log_line).groupdict()
            # print(log)
            # log_line = " ".join(log.values())
            if log:
                return {k: v if v is not None else '' for k, v in log.items()}
            else:
                print(f"Error: {e}\nInvalid log_format or log line: {log_line}")
                return None
            
    def _get_token_count(self, log_tokens: List[str]):
        token_count = defaultdict(int)
        for token in log_tokens:
            token_count[token] += 1
        return token_count
    
    def _create_clusters(self, log_list):
        for log_dict in log_list:
            log_tokens = " ".join(log_dict.values())
            log_tokens = log_tokens.split()
            log_length = len(log_tokens)
            has_new_cluster = True
            token_count = self._get_token_count(log_tokens)

            for cluster in self.log_clusters:
                if log_length != cluster["length"]:
                    continue

                shared_tokens = 0
                for token, count in token_count.items():
                    if token in cluster["tokens"]:
                        shared_tokens += min(count, cluster["tokens"][token])

                similarity = shared_tokens / log_length
                if similarity >= self.ct:
                    cluster["logs"].append(log_dict)
                    cluster["tokens"].update(token_count)
                    has_new_cluster = False
                    break

            if has_new_cluster:
                new_cluster = {"length": log_length, "tokens": token_count, "logs": [log_dict]}
                self.log_clusters.append(new_cluster)



    def parse_logs(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        header, regex = self.generate_logformat_regex()
        self.log_format = regex

        log_list = self.load_logs()
        self._create_clusters(log_list)

        all_logs = []
        for idx, cluster in enumerate(self.log_clusters, 1):
            unique_event_id = self.random_event_id()
            if len(cluster["logs"]) >= self.lower_bound:
                for line_id, log_dict in enumerate(cluster["logs"]):
                    log_dict["LineId"] = line_id + 1
                    log_dict["EventId"] = unique_event_id.lower()
                    all_logs.append(log_dict)

        if all_logs:
            result_df = pd.DataFrame(all_logs)
            # result_df['EventId'] = result_df.groupby('EventId')['EventId'].apply(lambda x: self.random_value())
            print(result_df)
            result_df.to_csv(os.path.join(self.output_dir, "clusters.csv"), index=False)
        else:
            print("No clusters found to save.")

    def generate_logformat_regex(self):
        # Function to generate regular expression to split log messages
        
        headers = []
        splitters = re.split(r'(<[^<>]+>)', '<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>')
        regex = ''
        for k in range(len(splitters)):
            if k % 2 == 0:
                splitter = re.sub(' +', "\\\\s+", splitters[k])
                regex += splitter
            else:
                header = splitters[k].strip('<').strip('>')
                regex += '(?P<%s>.*?)' % header
                headers.append(header)
        regex = re.compile('^' + regex + '$')
        print(regex.pattern)
        return headers, regex.pattern

if __name__ == "__main__":
    path = sys.argv[1]
    log_format = sys.argv[2]
    ct = float(sys.argv[3])
    lower_bound = int(sys.argv[4])
    output_dir = sys.argv[5]

    parser = LogParser(path, log_format, ct, lower_bound, output_dir)
    parser.parse_logs()

