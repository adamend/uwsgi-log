import datetime
import os
import re
import sys
import time

# [pid: 23629|app: 0|req: 148/122077] 127.0.0.1 () {40 vars in 651 bytes} [Wed Feb 16 06:41:14 2011] GET /some/path/ => generated 18469 bytes in 496 msecs (HTTP/1.1 200) 3 headers in 195 bytes (0 async switches on async core 0)


line_re = re.compile(r'''
\[pid:\ (?P<pid>\d+)
.*]\ 
(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})
.*
\[(?P<request_date>[\w\s\:]+)]
\ (?P<method>(POST|GET))
\ 
(?P<url>[/\w-]+)
.*
generated\ (?P<bytes>\d+)\ bytes
\ in\ (?P<processing_time>\d+)\ msecs
''', re.X)

def parse_line(line):
    try:
        match = line_re.match(line)
        d = match.groupdict()
        d['request_date'] = datetime.datetime(*time.strptime(d['request_date'])[:6])
        d['processing_time'] = int(d['processing_time'])
        return d
    except AttributeError:
        return line
        print 'Could not parse line:'
        print line

def group_by(requests, key, sort=None):
    group_dict = {}
    for request in requests:
        group = group_dict.get(request[key], [])
        group.append(request)
        group_dict[request[key]] = group
    group_list = []
    for grouper, group in group_dict.items():
        group_list.append([grouper, group])
    if sort == 'alphabetical':
        group_list.sort(lambda x, y: cmp(x[0].lower(), y[0].lower()))
    elif sort == 'count':
        group_list.sort(lambda x, y: cmp(len(y[1]), len(x[1])))
    return group_list

def print_results(requests):
    requests.sort(lambda x, y: cmp(x['request_date'], y['request_date']))
    num_requests = len(requests)
    start = requests[0]['request_date']
    end = requests[-1]['request_date']
    log_time = end - start
    all_processing_time = sum([r['processing_time'] for r in requests])
    grouped_by_url = group_by(requests, 'url', sort='count')

    print '===================='
    print 'Parsed %d requests' % num_requests
    print 'Log start:', start
    print 'Log end:', end
    print 'Total log time covered: %d seconds' % log_time.seconds
    print 'Total processing time: %d seconds' % int(round(float(all_processing_time) / 1000.0))
    print 'Top 5 URLs:'
    for grouper, group in grouped_by_url[:5]:
        count = len(group)
        percent = (float(count) / float(num_requests)) * 100
        total_time = sum([r['processing_time'] for r in group])
        percent_of_all_processing_time = (float(total_time) / float(all_processing_time)) * 100
        average_time = float(total_time) / float(count)
        print grouper
        print '    requests:', count
        print '    %% of total: %.1f%%' % percent
        print '    total time: %d sec' % int(round(float(total_time) / 1000.0))
        print '    %% of all processing time: %.1f%%' % percent_of_all_processing_time
        print '    average time: %d msec' % average_time
    print '===================='

def print_errors(unparsed_lines):
    print '===================='
    print 'Unable to parse the following lines:'
    print '\n'.join(unparsed_lines)
    print '===================='

if __name__ == '__main__':
    arg = sys.argv[1]
    parsed_lines = []
    unparsed_lines = []
    if os.path.isdir(arg):
        paths = [os.path.join(arg, filename) for filename in os.listdir(arg)]
    elif os.path.isfile(arg):
        paths = [arg]
    for path in paths:
        print path
        f = open(path, 'rt')
        for line in f:
            result = parse_line(line)
            if type(result) == dict:
                parsed_lines.append(result)
            elif type(result) == str:
                unparsed_lines.append(result)
        f.close()
    print_results(parsed_lines)
