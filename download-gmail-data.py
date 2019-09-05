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

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


## add incremental loading

## test which tags are allowed to query
## 53s no batch, no thread


def main():
    start_time = time.time()
    creds = get_creds()
    service = build('gmail', 'v1', credentials=creds)

    list_user_info(service)

    # list all labels for traversing, need to keep track of which ones we cannot make calls to
    labels = ['CATEGORY_FORUMS']
    # labels.remove('CATEGORY_PROMOTIONS')
    for label in labels:
        try:
            get_user_messages(service, label)
        except:
            print(label, 'cannot get emails with this label')

    get_message('16c594bc00becce9', service)
    message = service.users().messages().get(userId='me', id='16c594bc00becce9', format='full').execute()
    pprint(message)
    # get_messages_by_page(service)
    print('program time:', time.time() - start_time)


def get_message(msg_id, service, metadata=[]):
    message = service.users().messages().get(userId='me', id=msg_id, format='metadata',
                                             metadataHeaders=metadata).execute()
    # pprint(message)
    return message


def construct_row(header, msg):
    # map msg to header , return a list
    res = []

    for col in header:

        found = False
        if col in msg:
            res.append(msg[col])
            found = True
        else:
            found = False
            for field in msg['payload']['headers']:
                if field['name'] == col:
                    res.append(field['value'])
                    found = True
                    break
        if not found:
            res.append('')

    # update datetime to format we want
    date_col_idx = header.index('internalDate')
    epoch = int(res[date_col_idx][:-3])
    res[date_col_idx] = datetime.fromtimestamp(epoch).strftime('%Y-%m-%d %H:%M:%S')

    return res


def get_user_messages(service, label):
    print('currently getting emails with label:', label)

    # max is 500
    maxResult = 500
    msgId_list = []
    messages = service.users().messages().list(userId='me', labelIds=label, maxResults=maxResult).execute()
    print("len(messages['messages']):", len(messages['messages']), "messages['resultSizeEstimate']:",
          messages['resultSizeEstimate'])

    # open writer to write each msg as processed

    curr_time = datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')
    with open(label + curr_time + '.csv', 'w+') as out_file:
        msg_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, encoding='utf-8')
        info = ['id', 'threadId', 'historyId', 'internalDate', 'labelIds', 'sizeEstimate', 'snippet']
        metadata = ['Message-ID', 'From', 'Subject', 'List-Unsubscribe']
        header = info + metadata
        msg_writer.writerow(header)

        for msgId in messages['messages']:
            msgId_list.append(str(msgId['id']))
            # print('getting message for email id:', msgId['id'])
            msg = get_message(str(msgId['id']), service, metadata)
            # pprint(msg)
            # figure out which is which
            res = construct_row(header, msg)
            # pprint(res)
            msg_writer.writerow(res)

            # if len(msgId_list) % 10 ==0:
            #     print(len(msgId_list))

        print('processed:', len(msgId_list))

        if 'nextPageToken' in messages:
            more = True
            nextPageToken = messages['nextPageToken']
            print('nextPageToken:', messages['nextPageToken'])
        else:
            print('end of page')

        # remove for page
        # more = False
        while more:
            messages = service.users().messages().list(userId='me', labelIds=label, maxResults=maxResult,
                                                       pageToken=nextPageToken).execute()

            for msgId in messages['messages']:
                msgId_list.append(str(msgId['id']))
                # print('getting message for email id:', msgId['id'])
                msg = get_message(str(msgId['id']), service, metadata)
                # pprint(msg)
                # figure out which is which
                res = construct_row(header, msg)
                # pprint(res)
                msg_writer.writerow(res)

            print('processed:', len(msgId_list))

            if 'nextPageToken' in messages:
                more = True
                nextPageToken = messages['nextPageToken']
                print('nextPageToken:', messages['nextPageToken'])
            else:
                more = False
                print('no more next page')


def list_user_labels(service):
    # Call the Gmail API
    labels = service.users().labels().list(userId='me').execute()
    # pprint(labels['labels'])
    label_list = []
    # print('you have the following labels:')
    for label in labels['labels']:
        # print(label['type'], label['name'])
        if str(label['name']) != 'Junk' or str(label['name']) != 'Notes':
            label_list.append(str(label['name']))
    pprint(label_list)
    return label_list


def list_user_info(service):
    print('user info:')
    profile = service.users().getProfile(userId='me').execute()
    pprint(profile)


def user_drafts(service):
    print('user draft:')
    drafts = service.users().drafts().list(userId='me').execute()
    # print(profile)
    # drafts = {drafts:draft[], resultSizeEstimate: 37  }
    print('You have', drafts['resultSizeEstimate'], 'drafts')


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
