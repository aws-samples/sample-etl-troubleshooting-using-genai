from urllib.parse import urlparse, unquote
import requests; 
import os
import zipfile
from io import BytesIO

def download_aws_sdk(url):


    response = requests.get(url, stream=True)
    response.raw.decode_content = True
    
    zip_file = get_filename_from_url(url)
    #'bedrock-python-sdk-reinvent.zip'
    
    with open(zip_file, 'wb') as file:
        file.write(response.content)
        print(f"File {zip_file} downloaded.")

    def extract_file_from_zip_to_disk(zip_file_path, file_to_extract):
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            with zip_ref.open(file_to_extract) as file_data:
                file_bytes = BytesIO(file_data.read())
                # Write file_bytes to disk
                with open(file_to_extract, 'wb') as output_file:
                    output_file.write(file_bytes.getvalue())

    # Example usage
    zip_file_path = 'bedrock-python-sdk-reinvent.zip'
    file_to_extract = 'botocore-1.32.4-py3-none-any.whl'

    extract_file_from_zip_to_disk(zip_file_path, file_to_extract)
    
    print( f"File: {file_to_extract} extracted from zip file successfully")

    file_to_extract = 'boto3-1.29.4-py3-none-any.whl'
    extract_file_from_zip_to_disk(zip_file_path, file_to_extract)
    
    print( f"File: {file_to_extract} extracted from zip file successfully")

    # Let's delete the download ZIP archive
    try:
        os.remove(zip_file_path)
        print(f"{zip_file_path} has been deleted successfully.")
    except OSError as e:
        print(f"Error: {e.strerror}")

def get_filename_from_url(url):
    parsed_url = urlparse(url)
    path = unquote(parsed_url.path)  # Decode the URL-encoded path
    return path.split('/')[-1]