import re

files_pattern = re.compile(r"files=(\d+)")
size_pattern = re.compile(r"size=([\d.]*)(.*)")
hosts_pattern = re.compile(r"hosts=(\d+)")

keywords = {
    'agg': ":AGGREGATE",
    'exg': ":EXCHANGE",
    'alt': ":ANALYTIC",
    'select': ":SELECT",
    'hjoin': ":HASH JOIN",
    'ljoin': ":NESTED LOOP JOIN",
    'scan': ":SCAN HDFS",
    'sort': ":SORT",
    'union': ":UNION",
    'top': ":TOP-N"
}

unit_dict = {
    "PB": pow(1024, 3),
    "TB": pow(1024, 2),
    "GB": pow(1024, 1),
    "MB": pow(1024, 0),
    "KB": pow(1024, -1),
    "B": pow(1024, -2)
}


def get_features(explain_result, impala_version):
    """get feature columns from explain result.

    :param explain_result: explain result in generator format
    :return: features dict
    """
    explain_summary, files_info = extract_explain(explain_result)
    max_layer, max_size, max_files = explain_files.get(
        impala_version)(files_info)
    events = len(explain_summary)
    features_dict = {
        'mLayer': max_layer,
        'mSize': max_size,
        'mFiles': max_files,
        'events': events,
        }
    explain_summary_str = ''.join(line for line in explain_summary)
    for key, expression in keywords.items():
        features_dict.update({key: explain_summary_str.count(expression)})
    return features_dict


def extract_explain(explain_result):
    """extract explain_summary line and files_info line from explain result.

    :param explain_result: explain result in generator format
    :return: tuple made up of explain_summary list and files_info list

    """
    explain_summary = []
    files_info = []
    for line in explain_result:
        if filter_(line) == 1:
            explain_summary.append(line)
        if filter_(line) == 2:
            files_info.append(line)
    return explain_summary, files_info


def extract_files_new(files_info):
    """get explain tree depth and scan size and files information

    :param files_info: files info list
    :return: max_layer: equal to explain tree depth
             max_size: get the largest scan_size / instance value in explain
             tree except for rightmost subtree, considering Impala's left-deep
             tree strategy, keeps the right-hand table in memory and scans the
             left-hand table, thus the rightmost subtree influence less on
             memory using.
             max_files: get scan_files / instance value when
             scan_size / instance gets largest value.

    """
    line_layer = [get_layer(line) for line in files_info]
    max_layer = max(line_layer)
    # if depth of explain tree is 1, then set max_size=0 and max_files=0 to
    # keep consistent
    if max_layer == 1:
        return max_layer, 0, 0

    # since num of instance info not always appear with scan files info, we
    # need to record all instance num info on each depth of explain tree. when
    # we come across scan size and files info, we will try to find the latest
    # instance record on same depth, if not found, we get to its father tree
    # to get instance value recursively.
    layer_instances_counts = []
    max_size = 0
    max_files = 0
    for layer, line in zip(line_layer, files_info):
        # ignore rightmost subtree
        if layer == max_layer:
            continue

        if "instances=" in line:
            instances = int(line.split("instances=")[-1])
            layer_instances_counts.append((layer, instances))
        else:
            files = int(files_pattern.search(line).group(1))
            size_str = size_pattern.search(line)
            size = float(size_str.group(1)) * unit_dict.get(size_str.group(2))
            instances = 1
            for layer_, instances_ in reversed(layer_instances_counts):
                if layer_ <= layer:
                    instances = instances_
                    break
            files = round(files / instances)
            size = round(size / instances)
            if size > max_size:
                max_size = size
                max_files = files
    return max_layer, max_size, max_files


def extract_files_old(files_info):
    """get explain tree depth and scan size and files information

    :param files_info: files info list
    :return: max_layer: equal to explain tree depth
             max_size: get the largest scan_size / instance value in explain
             tree except for rightmost subtree, considering Impala's left-deep
             tree strategy, keeps the right-hand table in memory and scans the
             left-hand table, thus the rightmost subtree influence less on
             memory using.
             max_files: get scan_files / instance value when
             scan_size / instance gets largest value.

    """
    line_layer = [get_layer(line) for line in files_info]
    max_layer = max(line_layer)
    # if depth of explain tree is 1, then set max_size=0 and max_files=0 to
    # keep consistent
    if max_layer == 1:
        return max_layer, 0, 0

    # since num of instance info not always appear with scan files info, we
    # need to record all instance num info on each depth of explain tree. when
    # we come across scan size and files info, we will try to find the latest
    # instance record on same depth, if not found, we get to its father tree
    # to get instance value recursively.
    max_size = 0
    max_files = 0

    for index, (layer, line) in enumerate(zip(line_layer, files_info)):
        if layer == max_layer:
            continue

        if "partitions" in line:
            files = int(files_pattern.search(line).group(1))
            size_str = size_pattern.search(line)
            size = float(size_str.group(1)) * unit_dict.get(size_str.group(2))
            for line in files_info[index+1:]:
                if "hosts=" in line:
                    hosts = int(hosts_pattern.search(line).group(1))
                    break
            if not hosts:
                raise MemoryError("Can't find instances info in explain")
            files = round(files / hosts)
            size = round(size / hosts)
            if size > max_size:
                max_size = size
                max_files = files
    return max_layer, max_size, max_files


explain_files = {
    '2.9.0-cdh5.12.1': extract_files_new,
    '2.5.0-cdh5.7.2': extract_files_old

}


def get_layer(line):
    return line.count("|") + 1


def filter_(line):
    if "partitions" in line or "hosts" in line :
        return 2
    elif len(line.split(":")) > 1 and ("A" <= line.split(":")[1][0] <= "Z"):
        return 1
    else:
        return 0
