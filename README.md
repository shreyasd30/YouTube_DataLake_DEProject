# YouTube_DataLake_DEProject
# Project Overview
The primary objective of this project is to securely handle, streamline, and analyze structured and semi-structured data from YouTube videos, focusing on video categories and trending metrics.

# Project Goals 
A. Data Lake - We will get the data from multiple data sources and hence we will be needing a centralized repository to store them.  
B. Ingestion of Data - Develop a mechanism to collect data from various sources in different formats or same.  
C. ETL - We will be getting the data in raw format, so we will have to transform the data from raw data to clean data.  
D. Scalability - As our size of data increases, we will have to ensure the system can scale as the data volume increases.  
E. Cloud Infrastructure - As our data will be of vast amount, we won't be able to process it on the local machine, so we will be using the AWS 		 
                          cloud infrastructure for this.  
F. Data Reporting - Build a dashboard to answer key questions about the data.  

# Services Used
1. AWS Identity Access Management: This service is used to enable to manage access to the other AWS services and resources fast and securely.
2. Amazon S3: The Amazon S3 is an object storage service which basically provides the us the scalability, data availability, security and performance.  
3. Amazon Quicksight: Amazon Quicksight is a scalable, serverless, embeddable and machine learning powered business intelligence tool whic his built for the cloud.
4. AWS Glue: AWS Glue is a serverless data integration service that makes it easy to discover, prepare and combine all the data which is to be used to analytics, machine learning and any other application development.
5. AWS Athena: AWS Athena is an user friendly query service generally used for S3 in which there is no need to upload new data, it will always stay in S3 buckets.


#Dataset Used-

The dataset is taken from Kaggle and it contains statistics on daily YouTube Videos over the period of many countries of many months. In this dataset, there are upto 300 trending videos which are published every day for multiple locations. The data for each region is in its very own file. Some of attributes of it are as video title, channel title, publication time, tags, views, likes, dislikes, description of the videos, and the comment count are among the attributes included. A field called Category_id, is also included in the JSON file which is linked to the region which generally differs by the area/region.  

[Kaggle Dataset Link](https://www.kaggle.com/datasets/datasnaek/youtube-new/download?datasetVersionNumber=115)

# Data Architecture Diagram    


![Data_Architecture (1)](https://github.com/shreyasd30/YouTube_DataLake_DEProject/assets/56275546/43e0d06c-4e00-4ab9-a8a9-453385fd97e3)




