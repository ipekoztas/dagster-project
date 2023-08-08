
import base64
from io import BytesIO
import matplotlib.pyplot as plt
import pandas as pd
import requests
from selenium import webdriver
from bs4 import BeautifulSoup 
from dagster import (
    AssetKey,
    DagsterInstance,
    MetadataValue,
    Output,
    asset,
    get_dagster_logger,
    AssetIn,
    Config,
    job,
    op,
    Out
)
from dagstermill import define_dagstermill_asset
from dagster import file_relative_path
import sys, os
from tutorial.db_con import get_db
import logging
#import needed libraries
from sqlalchemy import create_engine, text
import pandas as pd
import os

class MyAssetConfig(Config):
    timestamp: str

class DatabaseConfig(Config):
    timestamp: str
    station: str


@asset
def topstory_ids(context, config: MyAssetConfig): 

    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    timestamp = config.timestamp   
    file_path = os.path.join(os.path.join(os.getcwd(), '..'), 'timestamps.txt')
    # Load existing timestamps from the text file
    with open(file_path, "r") as file:
        existing_timestamps = file.read().splitlines()

    found = False
    if timestamp in existing_timestamps:
        context.log.info(f"Files already downloaded for timestamp: {timestamp}")
        found = True
    
    else:
        context.log.info(f"Files are not downloaded for timestamp: {timestamp}")
        with open(file_path, "a") as file:
            file.write(timestamp + "\n")



    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    rows = soup.findAll('tr')

    mylist =[]
    for row in rows:
        cells = row.find_all('td')
        if len(cells) == 4 and cells[1].text.strip() == timestamp:
            file_name = cells[0].find('a').get('href')
            context.log.debug(f"Found link: {file_name}")
            mylist.append(url + file_name)

    context.log.info(file_relative_path(__file__, "..\\..\\test.ipynb"))
    if found:
        mylist.append(-1)   #meaning the files exist
    return mylist



@asset
def topstories(context, topstory_ids): 
    filenames = []

    download = True
    if topstory_ids[-1] == -1:
        download = False
        del topstory_ids[-1]

    if topstory_ids:
        for file in topstory_ids:

            context.log.debug(f"downloading: {file}")
            
            r = requests.get(file, stream=True)

            file_name = file.split('/')[-1]

            if download:
                save_path = "my-tutorial-project" +file_name
                with open(file_name, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

                context.log.info(f"downloading: {file}")
            else: 
                context.log.info(f"file exists: {file}")

            filenames.append(file_name) 
    else:
        print("File not found for the specified timestamp.")
    return filenames

#merge all the files
@asset
def most_frequent_words( context, topstories):
    frames = []
    for file in topstories:
        context.log.debug(f"file is is {file}")
        df = pd.read_csv(file)
        frames.append(df)
    result = pd.concat(frames)
    return result
  
my_jupyter_notebook = define_dagstermill_asset(
    name="my_jupyter_notebook",
    notebook_path=os.path.join(os.path.join(os.getcwd(), '..'), 'test2.ipynb'),
    ins={"iris": AssetIn("most_frequent_words")}

)

@asset
def find_file(context, config: DatabaseConfig):
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    timestamp = config.timestamp
    station = config.station

    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    rows = soup.findAll('tr')

    file_to_download = None
    for row in rows:
        cells = row.find_all('td')
        if len(cells) == 4 and cells[1].text.strip() == timestamp and cells[0].find('a').get('href') == (station + ".csv"):
            file_name = cells[0].find('a').get('href')
            context.log.debug(f"Found link: {file_name}")
            file_to_download = url + file_name
            break
    context.log.debug(f"FILENAME: {file_to_download}")
    """
    if file_to_download:
        destination_path = os.path.join(os.getcwd(), f'{timestamp}_{station}.csv')
        success = download_file(url, file_to_download, destination_path)
        if success:
            context.log.info(f"Downloaded file: {file_to_download}")
        else:
            context.log.error(f"Failed to download file: {file_to_download}")
    else:
        context.log.info(f"File not found for timestamp: {timestamp} and station: {station}")
"""
    return file_to_download

def download_file(url, file_url, destination_path):
    # Send an HTTP GET request to download the file
    response = requests.get(file_url)
    if response.status_code == 200:
        # Save the file content to the specified destination path
        with open(destination_path, 'wb') as file:
            file.write(response.content)
        return True
    else:
        return False

@asset
def download_file(context, find_file):
    file = find_file
    if file:
        context.log.debug(f"downloading: {file}")
        
        r = requests.get(file, stream=True)

        file_name = file.split('/')[-1]

        save_path = "my-tutorial-project" +file_name
        with open(file_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

        context.log.info(f"downloading: {file}")
        
        return file_name
    else:
        print("File not found for the specified timestamp.")

@asset
def download_data_todb(context, download_file) -> pd.DataFrame:
   
    df = pd.read_csv(download_file)
    df['DATE'] = pd.to_datetime(df['DATE'], format='ISO8601')
    context.log.debug(df.head())

    query = text("""
    CREATE TABLE IF NOT EXISTS stationdata (
        "DATE" TIMESTAMP,
        "LATITUDE" FLOAT,
        "LONGITUDE" FLOAT,
        "ELEVATION" FLOAT,
        "NAME" VARCHAR(100),
        "REPORT_TYPE" VARCHAR(10),
        "SOURCE" INT,
        "HourlyAltimeterSetting" INT,
        "HourlyDewPointTemperature" INT,
        "HourlyDryBulbTemperature" INT,
        "HourlyPrecipitation" INT,
        "HourlyPresentWeatherType" VARCHAR(30),
        "HourlyPressureChange" FLOAT,
        "HourlyPressureTendency" INT,
        "HourlyRelativeHumidity" INT,
        "HourlySkyConditions" VARCHAR(100),
        "HourlySeaLevelPressure" FLOAT,
        "HourlyStationPressure" FLOAT,
        "HourlyVisibility" FLOAT,
        "HourlyWetBulbTemperature" INT,
        "HourlyWindDirection" INT,
        "HourlyWindGustSpeed" INT,
        "HourlyWindSpeed" INT,
        "REM" VARCHAR(100)

    );
    """)

    db = get_db()
    db.execute(query)    
    

    df.to_sql('stationdata', db.get_bind(), if_exists='replace', index = False)

    db.commit()

    df = pd.read_sql_query("select * FROM users", db.get_bind())
    context.log.info(df.head())
    return df[['full_name']]
