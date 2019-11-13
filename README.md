# Sparkify Data Analysis
## Purpose
The entire data of Sparkify resides in JSON Files which makes it difficult to query and interpret. The data is loaded into parquet files using Spark after loading into Star Schema to store the data in various dimensional tables and one fact table that has consolidated data. 

## Schema
Two sources of data are given in form of JSON. One is the song data which has information regarding songs and artists. The other data source has log information regarding songs played by different users. These data sources are aggregated and a start schema is developed with 
- Fact Table: 
    - Songplay
- Dimensional Tables:
    - Users
    - Songs
    - Artists
    - Time
 
The data is stored in parquet. 

## How to Run? 
All the code has been written in python. 
This repository has one script. 
- etl.py: This script is used in extracting the data from JSON files and inserting them into respective tables. In this script, we read the JSON data residing in files and load the data into Dataframes. We then extract the required columns from the data frame and write the data to parquet files. We create temporary views whenever necessary else directly manipulate the dataframes. 

## Results
### Sample input JSON Data

#### Songs Data - 
![Image of Song JSON Data](/images/songs_json.png)  

#### Log Data - 
![Image of Log JSON Data](/images/log_json.png)  

The above tables are loaded into a star schema. 
#### Star Schema -
![Image of Star Schema](/images/star_schema.png)  
