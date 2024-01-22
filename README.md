# Video_Processing_And_Categorization

## Overview
This repository hosts an advanced Python pipeline designed for categorizing a large number of disorganized videos on a website. The pipeline streamlines the process of scraping videos from the website, processing them in a Google Cloud virtual machine, and categorizing them using machine learning techniques. It's an end-to-end solution aimed at bringing order and understanding to a previously unstructured collection of video data.

## Workflow
### 1. Video Scraping and Storage (video_import_and_storage.py)
This script is responsible for scraping videos from the website and storing them in Google Cloud Storage. This ensures all videos are centrally and securely stored, making them easily accessible for further processing.

### 2. Feature Extraction Deployment (feature_extraction_deployment.py)
Designed to run on a Google Cloud virtual machine, this script continuously processes the stored videos until all are handled. It employs a convolutional neural network model, VGG16, to extract meaningful visual features from each video. The use of cloud computing resources allows for efficient handling of large-scale data.

### 3. Data Categorization (categorizing.py)
After feature extraction, this module categorizes the videos based on the extracted features. It uses Principal Component Analysis (PCA) for reducing the dimensionality of the data and the KMeans clustering algorithm to categorize the videos. This step is crucial in organizing the videos into meaningful groups, making the vast array of video data more manageable and understandable.

## Technologies
The project is built using:   
-BeautifulSoup for web scraping  
-TensorFlow for visual feature extraction  
-Scikit-Learn for machine learning tasks  
-Google Cloud services for scalable data storage and processing  
-Pandas and NumPy for data manipulation

## Goal
The primary goal of this project is to get an overview of a chaotic collection of videos on a website and being able to categorize them by content/visual features. By doing so, it provides a structured and accessible way to handle and analyze video content.
