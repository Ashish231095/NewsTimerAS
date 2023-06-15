from .newsapi_downloader import NewsAPIArticles
from azure.storage.blob import BlobServiceClient
from openpyxl import load_workbook
from io import BytesIO
import snowflake.connector
from datetime import datetime
import pandas as pd
import numpy as np
import json
import logging
import os
#from blob_service import upload_csv_to_blob,read_excel_from_blob_storage

import json



class Config:
    """Config class which contains data, train and model hyperparameters"""

    def __init__(self,data, topic_uris):
        self.data = data
        self.topic_uris = topic_uris

    @classmethod # using config to define constructor of the class
    def from_json(cls, cfg):
        """Creates config from json"""
        params = json.loads(json.dumps(cfg), object_hook=HelperDict)
        # init all class instance with data and train attributes
        return cls(params.data, params.topic_uris) 


class HelperDict(object):
    """Helper class to convert json into Python object"""
    def __init__(self, dict_):
        self.__dict__.update(dict_)

def demo_main():

    CFG_demo = {
        "topic_uris": { # change path and file accordingly
            "path": "DefaultEndpointsProtocol=https;AccountName=bdpewsapitestd645ea;AccountKey=rbrsvvKpjkwUrDCVmKpseGGj7EjZCfL/8jknzkgGqFfat2Xtmdc9wpkt+KWzHo/477xUcsMt3AJS+AStUms1rg==;EndpointSuffix=core.windows.net",
            "file_name": "input/topic_uris_infer.xlsx"
        },
        "data": {
            "y": "relevance_class",
            "y_hat": "predicted_relevance_class",
            "x2": "body",
            "x1": "title",
            "selected_topics": ['train', 'air', 
                                'marine', 'weather', 'warehouse_fire'],
            "ordered_columns": ['body', 'title', 'relevance_class', 'topic'],
            "topic_mapping": [('weather_naturalevent', 'weather'), 
                            ('weather_generalnews', 'weather'),
                            ('weather_cyclone', 'weather')],
        },
    }


    logging.info('Demo main called')
    config = Config.from_json(CFG_demo)
    retrieved_articles = NewsAPIArticles(config=config)
    retrieved_articles.retrieve_articles_by_topics()
    df = retrieved_articles.df_articles

    df = df.astype({'source': 'string', 'title': 'string', 'body': 'string', 'authors': 'string'})

    # Create a connection to Snowflake
    conn = snowflake.connector.connect(
        user='2CHIRAGDASH',
        password='Qwerty3385#',
        account='uz58229.central-india.azure',
        warehouse='COMPUTE_WH',
        database='LOCATION_TEST',
        schema='MYSCHEMA1'
    )

    print("Connection successful")

    # Specify the target table in Snowflake where you want to insert the data
    target_table1 = 'NEWS_API_DATA'

    # Replace NaN values with None in the DataFrame
    df.replace({np.nan: None}, inplace=True)

    # Convert the URI column to string representation
    df['uri'] = df['uri'].astype(str)

    # Generate the SQL query to insert the data into the target table
    insert_query = f"INSERT INTO {target_table1} (URI, LANG, ISDUPLICATE, DATE, TIME, DATETIME, DATETIMEPUB, DATATYPE, SIM, URL, TITLE, BODY, SOURCE, AUTHORS, IMAGE, EVENTURI, SENTIMENT, WGT, RELEVANCE, TOPIC, TOPIC_URI, TITLE_CLEANED, TOPIC_ORIGINAL, RELEVANCE_CLASS) VALUES "

    # Generate the placeholders for the values
    value_placeholders = ', '.join(['%s'] * len(df.columns))

    # Fetch the data from the DataFrame
    data = df.values.tolist()

    # Execute the insert query using the executemany() method
    cursor = conn.cursor()
    cursor.executemany(insert_query + f"({value_placeholders})", data)

    # Commit the changes
    conn.commit()

    # Close the Snowflake connection
    # conn.close()

    #########

    # Table names
    target_table = 'LOCATION_TEST.MYSCHEMA1.NEWS_API_DATA_MAIN'
    stage_table = 'LOCATION_TEST.MYSCHEMA1.NEWS_API_DATA'

    # Merge query
    merge_query = f"""
    MERGE INTO {target_table} AS t
    USING (
        SELECT *
        FROM {stage_table} AS s
        WHERE s.uri NOT IN (
            SELECT uri
            FROM {target_table}
        )
    ) AS s
    ON t.uri = s.uri
    WHEN MATCHED THEN
        UPDATE SET t.lang = s.lang, t.isDuplicate = s.isDuplicate, t.date = s.date, t.time = s.time,
        t.dateTime = s.dateTime, t.dateTimePub = s.dateTimePub, t.dataType = s.dataType, t.sim = s.sim,
        t.url = s.url, t.title = s.title, t.body = s.body, t.source = s.source, t.authors = s.authors,
        t.image = s.image, t.eventUri = s.eventUri, t.sentiment = s.sentiment, t.wgt = s.wgt,
        t.relevance = s.relevance, t.topic = s.topic, t.topic_uri = s.topic_uri, t.title_cleaned = s.title_cleaned,
        t.topic_original = s.topic_original, t.relevance_class = s.relevance_class
    WHEN NOT MATCHED THEN
        INSERT (uri, lang, isDuplicate, date, time, dateTime, dateTimePub, dataType, sim, url, title,
        body, source, authors, image, eventUri, sentiment, wgt, relevance, topic, topic_uri,
        title_cleaned, topic_original, relevance_class)
        VALUES (s.uri, s.lang, s.isDuplicate, s.date, s.time, s.dateTime, s.dateTimePub, s.dataType,
        s.sim, s.url, s.title, s.body, s.source, s.authors, s.image, s.eventUri, s.sentiment, s.wgt,
        s.relevance, s.topic, s.topic_uri, s.title_cleaned, s.topic_original, s.relevance_class);
    """
    logging.info('Demo main executing')
    print(merge_query)
    # Execute the merge query
    cursor = conn.cursor()
    cursor.execute(merge_query)

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()
