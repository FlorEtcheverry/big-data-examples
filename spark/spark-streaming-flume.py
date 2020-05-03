import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

# Spark Streaming (v1) Example
# Streaming pushing model: Pushing data from Flume to Spark Streaming using Avro.
# IRL a pull model is more common (flume's sink for bidirectional relationship with Spark)


parts = [
    r'(?P<host>\S+)',  # host %h
    r'\S+',  # indent %l (unused)
    r'(?P<user>\S+)',  # user %u
    r'\[(?P<time>.+)\]',  # time %t
    r'"(?P<request>.+)"',  # request "%r"
    r'(?P<status>[0-9]+)',  # status %>s
    r'(?P<size>\S+)',  # size %b (careful, can be '-')
    r'"(?P<referer>.*)"',  # referer "%{Referer}i"
    r'"(?P<agent>.*)"',  # user agent "%{User-agent}i"
]
pattern = re.compile(r'\s+'.join(parts) + r'\s*\Z')


def extract_url_request(line):
    exp = pattern.match(line)
    if exp:
        request = exp.groupdict()["request"]
        if request:
            request_fields = request.split()
            if len(request_fields) > 1:
                return request_fields[1]


if __name__ == "__main__":
    sc = SparkContext(appName="StreamingFlumeLogAggregator")
    sc.setLogLevel("ERROR")
    batch_interval_s = 1
    ssc = StreamingContext(sc, batch_interval_s)

    flumeStream = FlumeUtils.createStream(ssc, "localhost", 9092)

    lines = flumeStream.map(lambda x: x[1])
    urls = lines.map(extract_url_request)

    # Reduce by URL over a 5-minute window sliding every second
    # Reduce: Count for each distinct URL
    window_interval = 300
    slide_interval = 1
    url_counts = urls.map(lambda x: (x, 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y,
                                                                 window_interval,
                                                                 slide_interval)

    # Sort and print the results
    sortedResults = url_counts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))
    sortedResults.pprint()

    # Checkpoint to save the state and restart from there
    ssc.checkpoint("/home/maria_dev/checkpoint")

    ssc.start()
    ssc.awaitTermination()
