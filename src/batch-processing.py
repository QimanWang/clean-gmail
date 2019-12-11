from __future__ import print_function
from googleapiclient.discovery import build
from googleapiclient.http import BatchHttpRequest
from pprint import pprint
from datetime import datetime
import time
import itertools
import pandas as pd
import logging
from gmail_util import get_creds



# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# configs
max_per_batch = 500
# default comes with [historyId,id,internalDate,labelIds,sizeEstimate,snippet,threadId, <payload:metadata>]
column_names = ['id', 'historyId', 'threadId', 'internalDate', 'labelIds', 'sizeEstimate', 'snippet', 'Message-ID',
                'From', 'Subject', 'List-Unsubscribe']

metadata = ['Message-ID', 'From', 'Subject', 'List-Unsubscribe']
msg_ids = []
msg_df = df = pd.DataFrame(columns=column_names)


# program time: 32.6133539677, 100

def main():
    start_time = time.time()

    # building out the api service
    creds = get_creds()
    service = build('gmail', 'v1', credentials=creds)

    # label = 'CATEGORY_PERSONAL'
    # labels = list_user_labels(service)
    labels = ['INBOX']
    pprint(labels)
    logging.basicConfig(level=args.loglevel, format='%(asctime)s %(levelname)s %(funcName)s:%(lineno)d %(message)s',
                        stream=sys.stdout)
    # remove_bad_labels(labels)

    for label in labels:
        print('current label', label)
        # gets email ids per label
        global msg_ids
        msg_ids = list_email_ids_by_label(service, label)
        # print(len(msg_ids))

        # run each batch
        while len(msg_ids) > 0:

            # break into batches
            msg_ids_batches = break_into_n_size_each_batch(msg_ids, max_per_batch)
            print('batches to process:', len(msg_ids_batches), ',per batch:', len(msg_ids_batches[0]), ',last batch:',
                  len(msg_ids_batches[-1]))
            b_id = 0
            for batch in msg_ids_batches:
                # print('batch ids', batch)
                b_id += 1
                print('running batch', b_id)

                batch_request = BatchHttpRequest()
                for msg_id in batch:
                    # print(msg_id)
                    batch_request.add(
                        service.users().messages().get(userId='me', id=msg_id, format='metadata',
                                                       metadataHeaders=metadata),
                        get_message)
                batch_request.execute()

        print(msg_df.shape)
        print(msg_df.head())
        print('writing to', label + '.csv')
        msg_df.to_csv('new/' + label + '.csv', encoding='utf-8', index=False)

    print('program time:', time.time() - start_time)


# what to do with each request call
def get_message(request_id, response, exception):
    if exception is not None:
        pass
        # print(request_id, 'something is wrong with request')
        # print(response)
        # print(exception)
    else:
        # print(response['id']
        # pprint(response)
        # print(len(msg_ids))

        data = {}
        # add all cols that exist
        for col in column_names[:-4]:
            data[col] = response[col]

        # only add metadata payload that has value, else ''
        for col in metadata:
            for p in response['payload']['headers']:
                if col == p['name']:
                    data[col] = p['value']
                    break

        # change time format
        epoch = int(response['internalDate'][:-3])
        data['internalDate'] = datetime.fromtimestamp(epoch).strftime('%Y-%m-%d %H:%M:%S')
        # check payload, which might not contain all fields

        if response['id'] in msg_ids:
            msg_ids.remove(response['id'])
            # print('removed id')
        # else:
        # print('id not found ')
        global msg_df
        msg_df = msg_df.append(data, ignore_index=True)

        # print(msg_df.head())


def break_into_n_size_each_batch(msg_ids, n):
    print(len(msg_ids), n)
    # how many batch
    num_batches = len(msg_ids) / n
    # how many in last batch
    last_batch_size = len(msg_ids) % n
    id_sets_list = []
    for i in range(num_batches):
        id_sets_list.append(set(itertools.islice(msg_ids, n * i, n * i + n)))
    # append the last reminder
    if last_batch_size > 0:
        id_sets_list.append(set(itertools.islice(msg_ids, num_batches * n, len(msg_ids))))
    # print(id_sets_list)
    return id_sets_list

    # 20, 3


def list_email_ids_by_label(service, label, max_results=500):
    email_ids = set()

    next_page_token = ''

    try:
        while True:
            response = service.users().messages().list(userId='me', labelIds=label, maxResults=max_results,
                                                       pageToken=next_page_token).execute()
            # extract id
            for msg_id in response['messages']:
                email_ids.add(msg_id['id'])
            print(len(email_ids))

            if 'nextPageToken' in response:
                print('next page token:', response['nextPageToken'])
                next_page_token = response['nextPageToken']
            else:
                break
            # uncomment for testing cap 500
            # break
    except:
        print('cannot get emails with this label', label)
    return email_ids


def remove_bad_labels(labels):
    for label in labels:
        pass


def list_user_labels(service):
    # Call the Gmail API
    labels = service.users().labels().list(userId='me').execute()
    # pprint(labels['labels'])
    label_list = []
    # print('you have the following labels:')
    for label in labels['labels']:
        label_list.append(str(label['name']))

    return label_list





if __name__ == '__main__':
    main()

# service.users().*
# * can be any end point,
