import time
import os
from google.cloud import pubsub_v1

# TODO(developer)
project_id = "my-project-3-150520"
topic_id = "sample_topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

futures = dict()


def follow(thefile):
    '''generator function that yields new lines in a file
    '''
    # seek the end of the file
    thefile.seek(0, os.SEEK_END)
    # start infinite loop
    while True:
        # read last line of file
        line = thefile.readline()
        # sleep if file hasn't been updated
        if not line:
            time.sleep(0.1)
            continue

        yield line

if __name__ == '__main__':
    logfile = open("/opt/gen_logs/logs/access.log","r")
    loglines = follow(logfile)
    # iterate over the generator
    for line in loglines:
        data=line
        data=data.encode("utf-8")
        future=publisher.publish(topic_path, data, origin = "python-sample", username = "aman" )
        print(future.result())
