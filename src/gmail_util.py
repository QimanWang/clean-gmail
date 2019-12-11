from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import BatchHttpRequest
from pprint import pprint
from datetime import datetime
import time
import itertools
import pandas as pd
import logging
import os


def get_creds():
    """
    Authenticates the user either by existing pickle token, or generate new one
    // TODO add instruction on how to get the credentials.json
    :return: creds
    """
    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    creds = None
    dir_pre = '../secrets'
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    logging.debug(f"searching for creds in path: {os.getcwd()}")
    if os.path.exists(os.path.join(dir_pre, 'token.pickle')):
        with open(os.path.join(dir_pre, 'token.pickle'), 'rb') as token:
            creds = pickle.load(token)
            logging.info('opening token.pickle')
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logging.info('token expired, refreshing token')
            creds.refresh(Request())
        else:
            logging.info('token not found, re authenticating ')
            flow = InstalledAppFlow.from_client_secrets_file(os.path.join(dir_pre, 'credentials.json'), SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(os.path.join(dir_pre, 'token.pickle'), 'wb') as token:
            pickle.dump(creds, token)

    return creds


def list_user_labels(service):
    """
    :param service:
    List all labels the user has, default + custom
    """
    logging.info('getting labels')
    labels = service.users().labels().list(userId='me').execute()
    label_list = [label['name'] for label in labels['labels']]

    return label_list


def list_email_ids_by_label(service, label, max_results=500):
    """
    :param service:
    :param label:
    :param max_results:
    :return:
    """

    email_ids = set()
    next_page_token = ''

    try:
        while True:
            response = service.users().messages().list(userId='me', labelIds=label, maxResults=max_results,
                                                       pageToken=next_page_token).execute()
            # extract id
            for msg_id in response['messages']:
                email_ids.add(msg_id['id'])
            logging.debug(f"total message ids: {len(email_ids)}")

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

def list_user_info(service):
    """
    list basic user info
    :param service:
    :return:
    """
    profile = service.users().getProfile(userId='me').execute()
    return profile
