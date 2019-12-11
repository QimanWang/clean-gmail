from __future__ import print_function
from googleapiclient.discovery import build
from googleapiclient.http import BatchHttpRequest
from pprint import pprint
from datetime import datetime
import time
import itertools
import pandas as pd
import sys
import logging
from gmail_util import get_creds, list_user_labels, list_email_ids_by_label, list_user_info

# configs
max_per_batch = 500
# default comes with [historyId,id,internalDate,labelIds,sizeEstimate,snippet,threadId, <payload:metadata>]
column_names = ['id', 'historyId', 'threadId', 'internalDate', 'labelIds', 'sizeEstimate', 'snippet', 'Message-ID',
                'From', 'Subject', 'List-Unsubscribe']
metadata = ['Message-ID', 'From', 'Subject', 'List-Unsubscribe']
msg_df = df = pd.DataFrame(columns=column_names)
logging_level = 'INFO'


# program time: 32.6133539677, 100

def main():
    start_time = time.time()

    logging.basicConfig(level=logging_level, format='%(asctime)s %(levelname)s %(funcName)s:%(lineno)d %(message)s',
                        stream=sys.stdout)

    logging.debug('building Gmail api service')
    creds = get_creds()
    service = build('gmail', 'v1', credentials=creds, cache_discovery=False)

    # list basic user info
    profile = list_user_info(service)
    logging.info(profile)

    # labels that he users emails have
    labels = list_user_labels(service)
    logging.info(labels)

    # get count of each label
    labels = ['CATEGORY_FORUMS']
    counts_per_label = {}
    for label in labels:
        logging.debug('current label'+ label)
        # gets email ids per label
        msg_ids = list_email_ids_by_label(service, label)
        logging.info(f"{label} {len(msg_ids)}")
        counts_per_label[label] = len(msg_ids)







if __name__ == '__main__':
    main()
