
# coding: utf-8

# Web server log analysis with Spark
#  
# Server log analysis is an ideal use case for Spark.  
# It's a very large, common data source and contains a rich set of information.  
# Spark allows you to store your logs in files on disk cheaply, while still providing a quick and simple way 
# to perform data analysis on them.  

# This program will show you how to use Apache Spark on real-world text-based production logs.
#
# Log data comes from many sources, such as web, file, and compute servers, application logs, 
# user-generated content,  and can be used for monitoring servers, improving business and customer
# intelligence, building recommendation systems, fraud detection, and much more.

# Apache Common Log Format (CLF)
# http://httpd.apache.org/docs/1.3/logs.html#common
# 
# The log file entries produced in CLF will look something like this:
# `127.0.0.1 - - [01/Aug/1995:00:00:01 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839`
#  

import sys
import os
import re
import datetime
from pyspark.sql import Row


month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

# A regular expression pattern to extract fields from the log line
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))

# Parsing Each Log Line
# The function returns a pair consisting of a Row object and 1. 
# If the log line fails to match the regular expression, the function returns a pair consisting of the log line string and 0. 
# A '-' value in the content size field is cleaned up by substituting it with 0. The function converts the log line's date string into a Python `datetime` object using the given `parse_apache_time` function.
def parseApacheLogLine(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
        host          = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = parse_apache_time(match.group(4)),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = size
    ), 1)


def parseLogs():
    """ Read and parse log file """
    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parseApacheLogLine)
                   .cache())
    # separate the logs into two: access_log (=success) and failed_logs
    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
    return parsed_logs, access_logs, failed_logs

if __name__ == "__main__":
    baseDir = os.path.join('data')
    inputPath = os.path.join('path1', 'path2', 'path3')
    logFile = os.path.join(baseDir, inputPath)
    parsed_logs, access_logs, failed_logs = parseLogs()

    # Calculate statistics based on the content size.
    content_sizes = access_logs.map(lambda log: log.content_size).cache()
    print 'Content Size Avg: %i, Min: %i, Max: %s' % (
        content_sizes.reduce(lambda a, b : a + b) / content_sizes.count(),
        content_sizes.min(),
        content_sizes.max())

    # Response Code Analysis
    responseCodeToCount = (access_logs
                        .map(lambda log: (log.response_code, 1))
                        .reduceByKey(lambda a, b : a + b)
                        .cache())
    responseCodeToCountList = responseCodeToCount.take(100)
    print 'Found %d response codes' % len(responseCodeToCountList)
    print 'Response Code Counts: %s' % responseCodeToCountList


    # Frequent Host Analysis
    # Any hosts that has accessed the server more than 10 times.
    hostCountPairTuple = access_logs.map(lambda log: (log.host, 1))
    hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b)
    hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)
    hostsPick20 = (hostMoreThan10
                 .map(lambda s: s[0])
                 .take(20))
    print 'Any 20 hosts that have accessed more then 10 times: %s' % hostsPick20


    # Top Endpoints
    endpointCounts = (access_logs
                      .map(lambda log: (log.endpoint, 1))
                      .reduceByKey(lambda a, b : a + b))
    topEndpoints = endpointCounts.takeOrdered(10, lambda s: -1 * s[1])
    print 'Top Ten Endpoints: %s' % topEndpoints


    # Top Ten Error Endpoints - What are the top ten endpoints which did not have return code 200
    not200 = access_logs.filter(lambda log: log.response_code != 200)
    endpointCountPairTuple = not200.map(lambda log: (log.endpoint, 1))
    endpointSum = endpointCountPairTuple.reduceByKey(lambda a, b: a + b)
    topTenErrURLs = endpointSum.takeOrdered(10, lambda s: -1 * s[1])
    print 'Top Ten failed URLs: %s' % topTenErrURLs


    # Number of Unique Hosts**
    hosts = access_logs.map(lambda log: log.host)
    uniqueHosts = hosts.distinct()
    uniqueHostCount = uniqueHosts.count()
    print 'Unique hosts: %d' % uniqueHostCount


    # Number of Unique Daily Hosts
    dayToHostPairTuple = access_logs.map(lambda log: (log.date_time.day, log.host))
    dayGroupedHosts = dayToHostPairTuple.groupByKey()
    dayHostCount = dayGroupedHosts.map(lambda pair: (pair[0], len(set(pair[1]))))
    dailyHosts = dayHostCount.sortByKey().cache()
    dailyHostsList = dailyHosts.take(30)
    print 'Unique hosts per day: %s' % dailyHostsList


    # Average Number of Daily Requests per Hosts
    dayAndHostTuple = access_logs.map(lambda log: (log.date_time.day, log.host))
    groupedByDay = dayAndHostTuple.groupByKey()
    sortedByDay = groupedByDay.sortByKey()
    avgDailyReqPerHost = sortedByDay.map(lambda x: (x[0], len(x[1])/len(set(x[1])))).cache()
    avgDailyReqPerHostList = avgDailyReqPerHost.take(30)
    print 'Average number of daily requests per Hosts is %s' % avgDailyReqPerHostList


    # How many 404 records are in the log?
    badRecords = access_logs.filter(lambda log: log.response_code==404).cache()
    print 'Found %d 404 URLs' % badRecords.count()


    # Listing 404 Response Code Records
    badEndpoints = badRecords.map(lambda log: log.endpoint)
    badUniqueEndpoints = badEndpoints.distinct()
    badUniqueEndpointsPick40 = badUniqueEndpoints.take(40)
    print '404 URLS: %s' % badUniqueEndpointsPick40


    # Listing the Top Twenty 404 Response Code Endpoints**
    badEndpointsCountPairTuple = badRecords.map(lambda log: (log.endpoint, 1)).reduceByKey(lambda a, b: a+b)
    badEndpointsSum = badEndpointsCountPairTuple.sortBy(lambda x: x[1], False)
    badEndpointsTop20 = badEndpointsSum.take(20)
    print 'Top Twenty 404 URLs: %s' % badEndpointsTop20


    # Listing the Top Twenty-five 404 Response Code Hosts
    errHostsCountPairTuple = badRecords.map(lambda log: (log.host, 1)).reduceByKey(lambda a, b: a+b)
    errHostsSum = errHostsCountPairTuple.sortBy(lambda x: x[1], False)
    errHostsTop25 = errHostsSum.take(25)
    print 'Top 25 hosts that generated errors: %s' % errHostsTop25


    # Listing 404 Response Codes per Day
    errDateCountPairTuple = badRecords.map(lambda log: (log.date_time.day, 1))
    errDateSum = errDateCountPairTuple.reduceByKey(lambda a, b: a+b)
    errDateSorted = errDateSum.sortByKey().cache()
    errByDate = errDateSorted.collect()
    print '404 Errors by day: %s' % errByDate


    # Visualizing the 404 Response Codes by Day
    daysWithErrors404 = errDateSorted.map(lambda x: x[0]).collect()
    errors404ByDay = errDateSorted.map(lambda x: x[1]).collect()


    # Top Five Days for 404 Response Codes 
    topErrDate = errDateSorted.sortBy(lambda x: -x[1]).take(5)
    print 'Top Five dates for 404 requests: %s' % topErrDate


    # Hourly 404 Response Codes**
    hourCountPairTuple = badRecords.map(lambda log: (log.date_time.hour, 1))
    hourRecordsSum = hourCountPairTuple.reduceByKey(lambda a, b: a+b)
    hourRecordsSorted = hourRecordsSum.sortByKey().cache()
    errHourList = hourRecordsSorted.collect()
    print 'Top hours for 404 requests: %s' % errHourList




