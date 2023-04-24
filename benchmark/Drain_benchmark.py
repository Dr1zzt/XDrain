#!/usr/bin/env python
import re
import sys
sys.path.append('../')
from logparser import XDrain, evaluator
import os
import pandas as pd
from collections import Counter
import en_core_web_md
from utils.pos_tag import _pos_score_tag, PerceptronScoreTagger
import datetime

nlp = en_core_web_md.load()
input_dir = '../logs/' # The input directory of log file
output_dir = 'XDrain_result/' # The output directory of parsing results

benchmark_settings = {
    # 'HDFS': {
    #     'log_file': 'HDFS/HDFS_2k.log',
    #     'log_format': '<Date> <Time> <Pid> <Level> <Component>: <Content>',
    #     'regex': [r'blk_-?\d+', r'[/]?(\d+\.){3}\d+(:\d+)?'],
    #     'filter': [],
    #     'index_list': [0, ],
    #     'st': 0.1,
    #     'depth': 6
    #     },
    #
    #
    # 'Hadoop': {
    #     'log_file': 'Hadoop/Hadoop_2k.log',
    #     'log_format': '<Date> <Time> <Level> \[<Process>\] <Component>: <Content>',
    #     'regex': [r'\[.*?(_.*?)+\]', ],
    #     'filter': [],
    #     'index_list': [0, 1, 2, 3, 4],
    #     'st': 0.6,
    #     'depth': 5
    #     },
    #
    #
    # 'Spark': {
    #     'log_file': 'Spark/Spark_2k.log',
    #     'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    #     'regex': [],
    #     'filter': [],
    #     'index_list': [0, 1, 2, 3, 4],
    #     'st': 0.1,
    #     'depth': 7
    #     },
    #
    #
    # 'Zookeeper': {
    #     'log_file': 'Zookeeper/Zookeeper_2k.log',
    #     'log_format': '<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>',
    #     'regex': [],
    #     'filter': [],
    #     'index_list': [0, 1, 2, 3, 4],
    #     'st': 0.1,
    #     'depth': 7
    #     },
    #
    #
    # 'BGL': {
    #     'log_file': 'BGL/BGL_2k.log',
    #     'log_format': '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>',
    #     'regex': [],
    #     'filter': [],
    #     'index_list': [0, ],
    #     'st': 0.4,
    #     'depth': 4
    #     },
    #
    #
    # 'HPC': {
    #     'log_file': 'HPC/HPC_2k.log',
    #     'log_format': '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>',
    #     'regex': [r'=\d+'],
    #     'filter': [],
    #     'index_list': [0, 1, 2, 3, 4],
    #     'st': 0.1,
    #     'depth': 7
    #     },
    #
    #
    # 'Thunderbird': {
    #     'log_file': 'Thunderbird/Thunderbird_2k.log',
    #     'log_format': '<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>',
    #     'regex': [r'(\d+\.){3}\d+'],
    #     'filter': [],
    #     'index_list': [0,],
    #     'st': 0.3,
    #     'depth': 4
    #     },
    #
    #
    # 'Windows': {
    #     'log_file': 'Windows/Windows_2k.log',
    #     'log_format': '<Date> <Time>, <Level>                  <Component>    <Content>',
    #     'regex': [r'0x.*?\s'],
    #     'filter': [],
    #     'index_list': [0, ],
    #     'st': 0.7,
    #     'depth': 4
    #     },
    #
    #
    # 'Linux': {
    #     'log_file': 'Linux_corrected/Linux_2k.log',
    #     'log_format': '<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>',
    #     'regex': [r'(\d+\.){3}\d+', r'\d{2}:\d{2}:\d{2}'],
    #     'filter': [],
    #     'index_list': [0, ],
    #     'st': 0.1,
    #     'depth': 5,
    #     },
    #
    #
    # 'Andriod': {
    #     'log_file': 'Andriod/Andriod_2k.log',
    #     'log_format': '<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>',
    #     'regex': [r'(/[\w-]+)+', r'([\w-]+\.){2,}[\w-]+', r'\b(\-?\+?\d+)\b|\b0[Xx][a-fA-F\d]+\b|\b[a-fA-F\d]{4,}\b',
    #               r'-\<\*\>'],
    #     'filter': [],
    #     'index_list': [0, ],
    #     'st': 0.9,
    #     'depth': 7
    #     },
    #
    #
    # 'HealthApp': {
    #     'log_file': 'HealthApp/HealthApp_2k.log',
    #     'log_format': '<Time>\|<Component>\|<Pid>\|<Content>',
    #     'regex': [],
    #     'filter': [],
    #     'index_list': [0, 1, 2, 3, 4],
    #     'st': 0.4,
    #     'depth': 8
    #     },
    #
    #
    # 'Apache': {
    #     'log_file': 'Apache/Apache_2k.log',
    #     'log_format': '\[<Time>\] \[<Level>\] <Content>',
    #     'regex': [r'\/(?:\w+\/){2,}\w+\.\w+$'],
    #     'filter': [],
    #     'index_list': [0, ],
    #     'st': 0.5,
    #     'depth': 4
    #     },
    #
    #
    # 'Proxifier': {
    #     'log_file': 'Proxifier/Proxifier_2k.log',
    #     'log_format': '\[<Time>\] <Program> - <Content>',
    #     'regex': [r'<\d+\ssec', r'([\w-]+\.)+[\w-]+(:\d+)?', r'\d{2}:\d{2}(:\d{2})*', r'[KGTM]B'],
    #     "filter": [r' \(\d+(\.\d+)?\s(?:K|M)B\)', ],
    #     'index_list': [0, ],
    #     'st': 0.6,
    #     'depth': 2
    #     },
    #
    #
    # 'OpenSSH': {
    #     'log_file': 'OpenSSH/OpenSSH_2k.log',
    #     'log_format': '<Date> <Day> <Time> <Component> sshd\[<Pid>\]: <Content>',
    #     'regex': [r"(\d+):"],
    #     'filter': [],
    #     'index_list': [0, ],
    #     'st': 0.6,
    #     'depth': 5
    #     },
    #
    #
    # 'OpenStack': {
    #     'log_file': 'OpenStack/OpenStack_2k.log',
    #     'log_format': '<Logrecord> <Date> <Time> <Pid> <Level> <Component> \[<ADDR>\] <Content>',
    #     'regex': ["(\w+-\w+-\w+-\w+-\w+)", r'HTTP\/\d+\.\d+'],
    #     'filter': [r'HTTP\/\d+\.\d+', ],
    #     'index_list': [0, 1, 2],
    #     'st': 0.1,
    #     'depth': 9
    #     },

    # 'Mac': {
    #     'log_file': 'Mac/Mac_2k.log',
    #     'log_format': '<Month>  <Date> <Time> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>',
    #     'regex': [r'([\w-]+\.){2,}[\w-]+'],
    #     'index_list': [0, 1, 2, 3, 4],
    #     'filter': [],
    #     'st': 0.6,
    #     'depth': 7
    #     },

    # 'Thunderbird_expand': {
    #     'log_file': 'Thunderbird_expand/Thunderbird_2k.log',
    #     'log_format': '<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>',
    #     'index_list': [0, 1, 2],
    #     'regex': [],
    #     'filter': [],
    #     'st': 0.6,
    #     'depth': 2
    # },
}


def oov_count(items):
    count = 0
    for str in items:
        if nlp(str)[0].is_oov:
            count += 1
    return count


def maxCounter(data):
    counter = Counter(data).most_common(2)
    if len(counter)==1 or counter[0][1] > counter[1][1]:
        return counter[0][0]
    elif counter[0][1] == counter[1][1]:
        count_1 = counter[0][0].count("<*>")
        count_2 = counter[1][0].count("<*>")
        if count_1 > count_2:
            return counter[0][0]
        elif count_1 < count_2:
            return counter[1][0]
        else:
            index_1 = [i for i, x in enumerate(counter[0][0].split()) if x == "<*>"]
            index_2 = [i for i, x in enumerate(counter[1][0].split()) if x == "<*>"]
            diff_2 = [counter[0][0].split()[i] for i in (set(index_2) - set(index_1))]
            diff_1 = [counter[1][0].split()[i] for i in (set(index_1) - set(index_2))]
            if oov_count(diff_1) > oov_count(diff_2):
                return counter[0][0]
            elif oov_count(diff_2) > oov_count(diff_1):
                return counter[1][0]
            else:
                score_1 = _pos_score_tag(diff_1, tagger=PerceptronScoreTagger(), lang='eng')[0]
                score_2 = _pos_score_tag(diff_2, tagger=PerceptronScoreTagger(), lang='eng')[0]
                if score_1 > score_2:
                    return counter[0][0]
                else:
                    return counter[1][0]


def log_to_dataframe(log_file, regex, headers):
    """ Function to transform log file to dataframe
    """
    log_messages = []
    linecount = 0
    with open(log_file, 'r', encoding="utf8") as fin:
        for line in fin.readlines():
            try:
                match = regex.search(line.strip())
                message = [match.group(header) for header in headers]
                log_messages.append(message)
                linecount += 1
            except Exception as e:
                pass
    logdf = pd.DataFrame(log_messages, columns=headers)
    logdf.insert(0, 'LineId', None)
    logdf['LineId'] = [i + 1 for i in range(linecount)]
    return logdf


def generate_logformat_regex(logformat):
    """
    Function to generate regular expression to split log messages
    """
    headers = []
    splitters = re.split(r'(<[^<>]+>)', logformat)
    regex = ''
    for k in range(len(splitters)):
        if k % 2 == 0:
            splitter = re.sub(' +', '\\\s+', splitters[k])
            regex += splitter
        else:
            header = splitters[k].strip('<').strip('>')
            regex += '(?P<%s>.*?)' % header
            headers.append(header)
    regex = re.compile('^' + regex + '$')
    return headers, regex

bechmark_result = []
start_time = datetime.datetime.now()
# print("start parsing! {}".format(start_time))
result = pd.DataFrame()
for dataset, setting in benchmark_settings.items():
    result = pd.DataFrame()
    headers, regex = generate_logformat_regex(setting["log_format"])
    indir = os.path.join(input_dir, os.path.dirname(setting['log_file']))
    log_file = os.path.basename(setting['log_file'])
    df_log = log_to_dataframe(os.path.join(indir, log_file), regex, headers)
    index_list = setting["index_list"]
    for i in index_list:
        indir = os.path.join(input_dir, os.path.dirname(setting['log_file']))
        log_file = os.path.basename(setting['log_file'])
        depth = setting['depth']
        st = setting['st']
        parser = XDrain.LogParser(log_format=setting['log_format'], indir=indir, outdir=output_dir,
                                 rex=setting['regex'], depth=depth, st=st, index=i,  index_list=index_list,
                                 filter=setting['filter'])
        parser.df_log = df_log
        parser.parse()
        data = parser.df_log[['EventId', 'EventTemplate']]
        if i == 0:
            result["EventTemplate"] = data["EventTemplate"].map(
                lambda x: [x, ]
            )
        else:
            temp = data.copy()
            temp["EventTemplate"] = data["EventTemplate"].map(
                lambda foobar: " ".join(foobar.split(" ")[-i:]) + " " + " ".join(foobar.split(" ")[:-i]))
            result["EventTemplate"] += temp["EventTemplate"].map(lambda x: [x, ])
    result["EventTemplate"] = result["EventTemplate"].map(lambda x: maxCounter(x))
    result.to_csv(os.path.join("output/", setting["log_file"].split("/")[1] + '_structured.csv'))
# end_time = datetime.datetime.now()
# print("end parsing! {}".format(end_time-start_time))
result = []
for dataset, setting in benchmark_settings.items():
    print('\n=== Evaluation on %s ==='%dataset)
    indir = os.path.join(input_dir, os.path.dirname(setting['log_file']))

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    log_file = os.path.basename(setting['log_file'])
    groundtruth = pd.read_csv(os.path.join(indir, log_file + '_structured.csv'), encoding="utf-8")["EventId"]
    count = 0
    data = pd.read_csv(os.path.join("output/", setting["log_file"].split("/")[1] + '_structured.csv'))["EventTemplate"]
    for parsed_eventId in data.value_counts().index:
        logIds = data[data == parsed_eventId].index
        # print(parsed_eventId, logIds)
        series_groundtruth_logId_valuecounts = groundtruth[logIds].value_counts()
        if series_groundtruth_logId_valuecounts.size == 1:
            groundtruth_eventId = series_groundtruth_logId_valuecounts.index[0]
            x = groundtruth[groundtruth == groundtruth_eventId]
            if logIds.size == groundtruth[groundtruth == groundtruth_eventId].size:
                count += logIds.size
        #     else:
        #         print(logIds.size,groundtruth[groundtruth == groundtruth_eventId].size)
        #         a = list(logIds)
        #         b = list(x.index)
        #         for i in a:
        #             if i not in b:
        #                 print(i)
        #         for i in b:
        #             if i not in a:
        #                 print(i)
        #         print("====================="+groundtruth_eventId)
        # else:
        #     print(str(logIds))
        #     print("========================"+parsed_eventId)
    accuracy = float(count) / data.size
    print(accuracy)

# word level accuracy
# for dataset, setting in benchmark_settings.items():
#     print('\n=== Evaluation on %s ==='%dataset)
#     indir = os.path.join(input_dir, os.path.dirname(setting['log_file']))
#
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir)
#     log_file = os.path.basename(setting['log_file'])
#     groundtruth = pd.read_csv(os.path.join(indir, log_file + '_structured_corrected.csv'), encoding="utf-8")["EventTemplate"]
#     count = 0
#     data = pd.read_csv(os.path.join("output/", setting["log_file"].split("/")[1] + '_structured.csv'))["EventTemplate"]
#     for i in range(len(data)):
#         data.iloc[i] = re.sub(" ", "", data.iloc[i])
#     for i in range(len(groundtruth)):
#         groundtruth.iloc[i] = re.sub(" ", "", groundtruth.iloc[i])
#     print(data.value_counts())
#     print("===============================================")
#     print(groundtruth.value_counts())
#     x = dict(data.value_counts())
#     y = dict(groundtruth.value_counts())
#     for i, j in zip(x,y):
#         if x[i] == y[j]:
#             print(i)
#             print(j)
#             print("////////////////////////////////"+str(x[i]))

