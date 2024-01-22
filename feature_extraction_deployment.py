from tensorflow.keras.applications.vgg16 import VGG16, preprocess_input
from google.cloud import storage
import pandas as pd
import cv2
import numpy as np
import tempfile
import os
from google.cloud import bigquery
import pandas_gbq
import google.auth

#______________________________________________________________________________
# Authentication of Bucket Storage API

storage_client = storage.Client()
#______________________________________________________________________________
# Authentication of BigQuery API

scopes_bq = ['https://www.googleapis.com/auth/bigquery',
          'https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/drive']

creds_bq, _ = google.auth.default(scopes = scopes_bq)

bigquery_client = bigquery.Client(credentials=creds_bq)
#______________________________________________________________________________
# Function to pull videos from blobs based on specific blob name
def get_video_blob(bucket_name, blob_name):
    """
    Pulls blob content from specific blob name
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob

# Function to extract visual features from videos
def extract_features_from_blob(blob, model):
    """
    Extracts visual features from imported videos
    """
    features = []
    
    # Blob is saved in a temporary file.
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        blob.download_to_filename(temp_file.name)
        
        # Check if the video file exists or is empty
        if not os.path.exists(temp_file.name) or os.path.getsize(temp_file.name) == 0:
            print(f"Failed to download blob to {temp_file.name} or file is empty.")
            return np.array([])  # Return an empty array if file is invalid

        cap = cv2.VideoCapture(temp_file.name)

        # Features are created based on a pre-trained Convolutional Neural Network model.
        while cap.isOpened():  # Checks if the video can be read
            ret, frame = cap.read()  # ret = Check if the individual frame is read. frame = The content of the frame
            if not ret:
                break
            # Preprocessing of the frame
            frame = cv2.resize(frame, (224, 224))
            frame = frame.astype("float32")
            frame = preprocess_input(frame)
            frame = np.expand_dims(frame, axis=0)
            
            # Features are extracted based on the CNN model
            feature = model.predict(frame)
            # Features are "flattened" to fit in a single row in a dataframe
            features.append(feature.flatten())
        # The frame is released to make room for the next one
        cap.release()

    # Temporary file is removed
    os.remove(temp_file.name)

    return np.array(features).mean(axis=0) if features else np.array([])
#______________________________________________________________________________

# Bucket and blobs are defined
bucket_name = 'website_tv_videos'
bucket = storage_client.bucket(bucket_name)
blobs = bucket.list_blobs()

# VGG16 model is activated
model = VGG16(weights='imagenet', include_top=False)

# Empty dataframe is created
features_df = pd.DataFrame()

# An empty list is created
all_features = []

# Original table is defined
existing_table_id = 'gcp-project.website_tv.video_features'       

for blob in blobs:
    video_features = extract_features_from_blob(blob, model)
    if video_features.size > 0:
        # Metadata containing original url is extracted
        source_url = blob.metadata.get('source_url', 'No URL Found')
        # Dataframe with video features is created and transposed
        features_df = pd.Series(video_features).to_frame().T
        # Dataframe with original URL is created
        url_df = pd.DataFrame({'source_url': [source_url]})
        # Features and original URL are combined into a temporary dataframe
        temp_df = pd.concat([url_df, features_df], axis=1).T
        
        # All data is converted to string
        temp_df = temp_df.astype(str)

        # Index column is added to the dataframe
        temp_df = temp_df.reset_index(drop=True).reset_index().rename(columns={'index': 'row_index'})

        # Periods are replaced with underscores in the blob name to be exported to BigQuery
        modified_blob_name = blob.name.replace('.', '_')
        # The dataframe's column name is changed to the same as the blob name
        temp_df = temp_df.rename(columns={temp_df.columns[1]: modified_blob_name})

        # Table name is defined for temporary table
        new_table_id = f'gcp-project.website_tv.temp_{modified_blob_name}'
        new_table_name = f'temp_{modified_blob_name}'

        # Dataframe is exported to temporary table
        pandas_gbq.to_gbq(
            dataframe=temp_df,
            destination_table=new_table_id,
            project_id='gcp-project',
            if_exists='replace',
            credentials=creds_bq
        )

        # Query to merge temporary and original tables in BigQuery
        query = f"""
            CREATE OR REPLACE TABLE `gcp-project.website_tv.video_features` AS
            SELECT `gcp-project.website_tv.video_features`.*, `gcp-project.website_tv.{new_table_name}`.{modified_blob_name}
            FROM `gcp-project.website_tv.video_features`
            JOIN `gcp-project.website_tv.{new_table_name}`
            ON `gcp-project.website_tv.video_features`.row_index = `gcp-project.website_tv.{new_table_name}`.row_index
        """

        # Query execution
        bigquery_client.query(query).result()

        # Temporary table is deleted in BigQuery
        try:
            bigquery_client.delete_table(new_table_id)
            print(f"Deleted table {new_table_id}")
        except Exception as e:
            print(f"Error with deleting table. Error: {e}")
        
        print(f"Processed and merged data for blob: {blob.name}")


