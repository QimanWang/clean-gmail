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

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# gmail api setup
creds = None
service = None
# configs
batch_max = 450
metadata = ['Message-ID', 'From', 'Subject', 'List-Unsubscribe']


def main():
    start_time = time.time()

    # building out the api service
    creds = get_creds()
    service = build('gmail', 'v1', credentials=creds)

    # personal 2906
    msg_ids = list_email_ids_by_label(service, 'CATEGORY_PERSONAL')

    # metadata = ['Message-ID', 'From', 'Subject', 'List-Unsubscribe']
    i = 0
    for ids in msg_ids:
        service.users().messages().get(userId='me', id=ids, format='metadata').execute()
        i += 1
        if i % 100 == 0:
            print(i)
            break

    print('program time:', time.time() - start_time)


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


def list_email_ids_by_label(service, label):
    email_ids = []
    max_results = 100
    next_page_token = ''
    # create the batch request
    batch_requests = BatchHttpRequest()

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
