import datetime
import os 
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io

def get_current_timestamp():
    """
    Returns a string of current timestamp in YYYYmmdd_HHMMSS format
    """
    now = datetime.datetime.now()
    return now.strftime("%Y%m%d_%H%M%S")

def save_dataframe_with_timestamp(input_dataframe, path, name):
    filename = get_current_timestamp() + '_' + name + '.xlsx'
    filepath = os.path.join(path, filename)
    input_dataframe.to_excel(filepath)
    print('Saved to: ', filepath)

def write_dataframe_to_azure_blob(dataframe, connection_string, container_name, file_name, file_extension):
    """
    Directly writes an in-memory dataframe to azure blob storage container
    """
    # Convert DataFrame to CSV or Excel file in memory
    file_name_full = file_name + file_extension
    if file_extension == '.csv':
        file_content = dataframe.to_csv(index=False)
        content_type = 'text/csv'
    elif file_extension == '.xlsx':
        with io.BytesIO() as excel_buffer:
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                dataframe.to_excel(writer, index=False)
            excel_buffer.seek(0)
            file_content = excel_buffer.read()
        content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif file_extension == '.json':
        file_content = dataframe.to_json(orient='split')
        content_type='application/json'
    else:
        raise ValueError("File format not supported. Please provide either CSV, Excel or JSON file format ending.")

    # Create BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    # Upload the file to blob storage
    try: 
        blob_file_name = get_current_timestamp() + '_' + file_name_full
        blob_client = container_client.get_blob_client(blob_file_name)
        blob_client.upload_blob(file_content, blob_type="BlockBlob", content_type=content_type)
        print(f"Successfully uploaded '{file_name}' to Azure Blob Storage.")
    except Exception as e:
        print(f"An error occurred while uploading '{file_name}': {str(e)}")
