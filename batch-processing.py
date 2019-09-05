from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import BatchHttpRequest
from pprint import pprint
import unicodecsv as csv
from datetime import datetime
import time
import concurrent.futures
import threading
import httplib2

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# gmail api setup
creds = None
service = None

# configs
batch_max = 300
metadata = ['Message-ID', 'From', 'Subject', 'List-Unsubscribe']
msg_ids = None


def main():
    start_time = time.time()

    # building out the api service
    creds = get_creds()
    service = build('gmail', 'v1', credentials=creds)

    # gets email ids per label
    msg_ids = list_email_ids_by_label(service, 'CATEGORY_PERSONAL')
    print(len(msg_ids))
    # while there's more id's , we break it up into batches and process
    while len(msg_ids) > 0:
        # break into batches
        msg_ids_batch = break_into_n_size_each_batch(msg_ids, 2)
        print('batch', msg_ids_batch)

        # create http requests for each batch
        request_batches = create_request_batch(msg_ids_batch, service)

        print('batches:', len(request_batches))

        # multi process
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(execute_batch, request_batches)


    print('program time:', time.time() - start_time)

# wrap with a httrequestbatch
def create_request_batch(list_msg_ids_batch, service):
    """
    takes in a list of msg_id_batch, then package each batch into a http request batch
    :param list_msg_ids_batch:
    :param service:
    :return:
    """
    request_batches = []
    for batch in list_msg_ids_batch:
        batch_requests = BatchHttpRequest()
        for msg_id in batch:
            # print(msg_id)
            batch_requests.add(
                service.users().messages().get(userId='me', id=msg_id, format='metadata'),
                get_message)
        request_batches.append(batch_requests)

    return request_batches


def execute_batch(batch):
    http = httplib2.Http()

    batch.execute(http=http)
    print('batch executed')


# what to do with each request call
def get_message(request_id, response, exception):
    if exception is not None:
        print(request_id, 'something is wrong with request')
    else:
        # print(request_id, response['snippet'])
        print(response)
        msg_ids.pop(response['id'])
        pass

    return 'hi'


def get_messages_batch(msg_ids):
    # create the batch request
    batch_requests = BatchHttpRequest()
    print('in get_messages_batch')
    # print(msg_ids)
    for msg_id in msg_ids:
        print('msg_id', msg_id)
        batch_requests.add(
            service.users().messages().get(userId='me', id=str(msg_id), format='metadata', metadata=metadata),
            get_message)

    batch_requests.execute()
    # print('executed batch')


def break_into_n_size_each_batch(msg_ids, n):
    print(len(msg_ids), n)
    # how many batch
    num_batches = len(msg_ids) / n
    # how many in last batch
    last_batch_size = len(msg_ids) % n
    res = []
    for i in range(num_batches):
        res += [msg_ids[n * i:n * i + n]]
    # append the last reminder
    if last_batch_size > 0:
        res += [msg_ids[num_batches * n:]]
    # print(res)
    return res

    # 20, 3


def list_email_ids_by_label(service, label, max_results=6):
    email_ids = []

    next_page_token = ''


    while True:
        response = service.users().messages().list(userId='me', labelIds=label, maxResults=max_results,
                                                   pageToken=next_page_token).execute()
        # extract id
        for msg_id in response['messages']:
            email_ids.append(msg_id['id'])
        print(len(email_ids))

        if 'nextPageToken' in response:
            print('next page token:', response['nextPageToken'])
            next_page_token = response['nextPageToken']
        else:
            break
        # uncomment for testing cap 500
        break
    return email_ids


def get_creds():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds


if __name__ == '__main__':
    main()

# service.users().*
# * can be any end point,
