import pandas as pd
pd.options.mode.chained_assignment = None
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from google.cloud import storage
import requests
from bs4 import BeautifulSoup

#______________________________________________________________________________

# Authentication of Google Sheets API og GSpread

scopes_sheets = [
'https://spreadsheets.google.com/feeds',
'https://www.googleapis.com/auth/drive'
]

credentials_sheets = ServiceAccountCredentials.from_json_keyfile_name("C:/path_to_service_account_key/serviceaccount_key.json", scopes_sheets) 
sheets_service = gspread.authorize(credentials_sheets)

#______________________________________________________________________________
# Authentication of Bucket Storage API

scopes_bs = ['https://www.googleapis.com/auth/devstorage.read_write']

# Service object skabes
creds_bs = service_account.Credentials.from_service_account_file("C:/path_to_service_account_key/serviceaccount_key.json",
                                                                 scopes = scopes_bs)
storage_client = storage.Client(credentials=creds_bs)


# Function to create list of available buckets
def list_buckets(storage_client):
    """
    Lists available buckets
    """
    buckets = storage_client.list_buckets()
    for bucket in buckets:
        print(bucket.name)

list_buckets(storage_client)


###############################################################################
# Google Sheet file and worksheet opened
website_tv_file = sheets_service.open('website.tv')
website_tv_sheet = website_tv_file.worksheet('Ark1')
# Url column imported and url dataframe created
urls_df = pd.DataFrame(website_tv_sheet.col_values(1))

# All empty columns and .jpg urls is removed
urls_df = urls_df[(urls_df[0] != '') & (urls_df[0].str.contains(".jpg|orderby=|js|css|/tag/|channel|source=site|mp3") == False)].reset_index(drop=True)

# First url removed (frontpage)
urls_df = urls_df[0][1:].reset_index(drop=True)

####################### SCRAPING OF VIDEO-URLS  ###############################

# Function to scrape video_src aka video sources from urls
def extract_video_url(html_content):
    """
    Scrapes video sources from urls
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    video_link = soup.find('link', rel='video_src')
    if video_link and video_link.has_attr('href'):
        return video_link['href']
    return None

# Empty list for video sources
video_src_df = []

# For loop for scraping video sources from the url dataframe
for i in range(len(urls_df)):
    response = requests.get(urls_df[i])
    video_src_url = extract_video_url(response.text)
    if video_src_url == None:
        continue
    else:
        video_src_df.append(video_src_url)
    
####################### VIDEOER EKSPORTERES TIL BUCKET ########################

# Function to stream videos from url sources into Bucket Storage
def stream_video_to_bucket(url, bucket_name, destination_blob_name, source_url):
    """
    Streams a video from a url directly to a Storage bucket and add source url as metadata.
    """
    # Video scraped from url
    response = requests.get(url, stream=True)
    # If request is succesfull the video will be exported
    if response.status_code == 200:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)  # Naming for video/blob

        # Source url added as metadata
        blob.metadata = {'source_url': source_url}

        # Video exported as an MP4 file
        blob.upload_from_file(response.raw, content_type='video/mp4')
        print(f"Video streamed to {destination_blob_name} in bucket {bucket_name}.")
    else:
        print(f"Failed to download from {url}")


# Function to generate unique name
def generate_unique_name(index):
    """
    Generates a unique name based on index
    """
    return f"video_{index}.mp4"

# Name of the bucket is defined
bucket_name = "website_tv_videos"
    
# For loop for exporting all video sources
for i in range(len(video_src_df)):
    video_url = video_src_df[i]
    source_url = urls_df[i]  # Original url is defined
    unique_name = generate_unique_name(i)  # Video being named
    # Video exported
    stream_video_to_bucket(video_url, bucket_name, unique_name, source_url)


