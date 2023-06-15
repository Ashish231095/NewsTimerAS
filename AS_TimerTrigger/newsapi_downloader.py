import pandas as pd
import os 
from eventregistry import *
from .helpers import get_current_timestamp
#from dataloader import clean_string
from azure.storage.blob import BlobServiceClient
from openpyxl import load_workbook
from io import BytesIO


er = EventRegistry(apiKey = '2a7714d1-0234-4382-a783-09f80311cc3f')

def clean_string(string: str):
    """
    Removes non alphabetical characters (case insensitive) from input string, including leading and trailing white spaces
    """
    string = re.sub("[^A-Z]", " ", string,0,re.IGNORECASE) # only keeping a-z, numbers and other special chars are removed
    string = string.lower()
    string = string.strip()
    return string
class NewsAPIArticles():
    """
    Class to download articles from NewsAPI according to the topic URIs.
    Required configs:
    - `topic_uris`: for file specifying topic URIs
    - `data`: for remapping of weather topics into 1 combined weather topic
    - `inference_data`: for location to save the inferred dataframe
    """
    # def __init__(self, config):
    #     self.config = config
    #     topic_uris_path = config.topic_uris.path
    #     topic_uris_file = config.topic_uris.file_name
        
    #     self.file_path = os.path.join(topic_uris_path, topic_uris_file)
    #     self.uris_df = pd.read_excel(self.file_path)
    #     self.uris_dict = self.uris_df.to_dict(orient='list')
    #     print("Topics for retrieving articles:")
    #     print(self.uris_df)

    def __init__(self, config):
        self.config = config
        topic_uris_path = config.topic_uris.path
        topic_uris_file = config.topic_uris.file_name
        
        # Connect to the Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=bdpewsapitestd645ea;AccountKey=rbrsvvKpjkwUrDCVmKpseGGj7EjZCfL/8jknzkgGqFfat2Xtmdc9wpkt+KWzHo/477xUcsMt3AJS+AStUms1rg==;EndpointSuffix=core.windows.net")
        container_name = "input"
        blob_name = "topic_uris_infer.xlsx"

        # Get a reference to the Excel file in the blob storage
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        # Download the blob data as bytes
        blob_data = blob_client.download_blob().readall()

        # Load the Excel file from bytes using pandas
        excel_data = pd.read_excel(BytesIO(blob_data))

        # Assign the DataFrame to uris_df
        self.uris_df = excel_data
        self.uris_dict = self.uris_df.to_dict(orient='list')

        print("Topics for retrieving articles:")
        print(self.uris_df)
    
        
    def retrieve_articles_by_topics(self):
        self.articles = []
        self._topic_log = []
        for item in range(len(self.uris_dict['topic'])):
            topic = self.uris_dict['topic'][item]
            uri = self.uris_dict['uri'][item]
            t = TopicPage(er)
            t.loadTopicPageFromER(uri)
            res = t.getArticles(page = 1)
            topic_details = res['topicPage']
            topic_details['topic'] = topic
            self._topic_log.append(topic_details)
            max_page_number = res['articles']['pages']
            print(f'Retrieving topic: {topic}')
            for page_number in range(1,max_page_number+1):
                result = t.getArticles(page = page_number)
                print(f'Retrieving page: {page_number}/{max_page_number}')
                for article in result['articles']['results']:
                    article['topic'] = topic
                    article['topic_uri'] = uri
                    self.articles.append(article)
        self.df_articles = pd.DataFrame(self.articles)
        # clean and remove duplicated titles:
        self.df_articles["title_cleaned"] = self.df_articles['title'].apply(func=clean_string)
        self.df_articles = self.df_articles.drop_duplicates(subset = ['title_cleaned'],keep='first', inplace=False)
        self.df_articles['topic_original'] = self.df_articles['topic'] # create a copy of the original topics
        self.df_articles['topic'] = self.df_articles['topic'].replace(dict(self.config.data.topic_mapping))
        # create a column for manual labelling later, fill with -1 for now
        self.df_articles[self.config.data.y] = -1

    def save_articles_df_with_timestamp(self):
        inference_data_config = self.config.inference_data
        filename = get_current_timestamp() + '_' + 'inference_articles' + '.xlsx'
        filepath = os.path.join(inference_data_config.path, filename)
        self.df_articles.to_excel(filepath)
        print('Saved NewsAPI articles to: ', filepath)







