from google.cloud import bigquery
import pandas_gbq
from google.oauth2 import service_account
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import pandas as pd

#______________________________________________________________________________
# Authentication of BigQuery API

scopes_bq = ['https://www.googleapis.com/auth/bigquery',
          'https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/drive']

# Service object created
creds_bq = service_account.Credentials.from_service_account_file("C:/path_to_service_account_key/serviceaccount_key.json",
                                                                 scopes = scopes_bq)

bigquery_client = bigquery.Client(credentials=creds_bq)

####################### DATA IMPORT FROM BIGQUERY  ############################

# Query is defined
query = """
SELECT *
FROM `gcp-project.website_tv.video_features`
"""
# Data is imported using the query
features_import = pandas_gbq.read_gbq(query, project_id='gcp-project')

#___________________
# Dataframe is sorted by "row_index"
features_df = features_import.sort_values(by='row_index')
# "row_index" column is dropped
features_df = features_df.drop(columns='row_index')
# Dataframe's index is reset
features_df = features_df.reset_index(drop=True)
# Dataframe is transposed
features_df = features_df.T
# Dataframe's index is reset again
features_df = features_df.reset_index(drop=True)

#################### IDEAL NUMBER OF CLUSTERS IS FOUND ########################

# X-variables are defined. First column with video URLs is excluded
X = features_df.iloc[:, 1:]

# PCA is performed for dimensionality reduction
pca = PCA(n_components=0.95)  # Retain 95% variance
X_reduced = pca.fit_transform(X)

#______________________________________________________________________________
# Best number of clusters is investigated

# Elbow Method
sse = []
for k in range(1, 11):
    kmeans = KMeans(n_clusters=k, random_state=0).fit(X_reduced)
    sse.append(kmeans.inertia_)

# Plot is created
plt.plot(range(1, 11), sse)
plt.title('Elbow Method')
plt.xlabel('Number of clusters')
plt.ylabel('SSE')
plt.show()

#______________________________________________________________________________

# Silhouette Score
# Empty dataframe
results_df = pd.DataFrame(columns=['Number of Clusters', 'Silhouette Score'])

# Silhouette score is calculated and inserted into the dataframe
for k in range(2, 11):
    kmeans = KMeans(n_clusters=k, random_state=0).fit(X_reduced)
    score = silhouette_score(X_reduced, kmeans.labels_)
    new_row = pd.DataFrame({'Number of Clusters': [k], 'Silhouette Score': [score]})
    results_df = pd.concat([results_df, new_row], ignore_index=True)

########################## CLUSTERING PREDICTION ##############################

# Number of desired clusters is defined
n_clusters = 10

# K-means model is created
kmeans = KMeans(n_clusters=n_clusters, random_state=42).fit(X_reduced)

# K-means model is run
clusters = kmeans.predict(X_reduced)

# Cluster numbers are inserted into the dataframe
features_df.insert(0, 'cluster', clusters)

# Clusters are split individually
cluster_0_df = features_df[features_df['cluster'] == 0]
cluster_1_df = features_df[features_df['cluster'] == 1]
cluster_2_df = features_df[features_df['cluster'] == 2]
cluster_3_df = features_df[features_df['cluster'] == 3]
cluster_4_df = features_df[features_df['cluster'] == 4]
cluster_5_df = features_df[features_df['cluster'] == 5]
cluster_6_df = features_df[features_df['cluster'] == 6]
cluster_7_df = features_df[features_df['cluster'] == 7]
cluster_8_df = features_df[features_df['cluster'] == 8]
cluster_9_df = features_df[features_df['cluster'] == 9]
