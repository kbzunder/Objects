# v8
import io
from google.cloud import storage
from abc import ABC, abstractmethod
import PyPDF2
# kuku
import docx2txt  # Import the library for extracting text from docx files
from pathlib import Path

# Download file from Google Cloud Bucket
class DownloadFileFromBucket:
    def __init__(self, path):
        self.path = path

    def download_file(self):
        bucket_name = self.path.split('/')[0]
        file_name = self.path.split('/')[1]
        file_path = '/'.join(self.path.split('/')[1:])  # Assuming the full path after the bucket name is the file path
        
        # Download from cloud storage
        if bucket_name is not None:
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(file_path)

            # Create a BytesIO object to hold the file in memory
            file = io.BytesIO()
            try:
                blob.download_to_file(file)
                file.seek(0)  # Reset the stream position to the beginning
                print('file from bucket uploaded')
            except Excetion as e:
                print(e)
        else:
            # If not downloading from cloud storage, just read from the local path
            with open(self.path, 'rb') as file:
                file = io.BytesIO(file.read())
        
        # Save the file locally (if needed)
        with open(f'{file_name}', 'wb') as f:
            f.write(file.getvalue())  # Write the binary content to disk
        
        return file


# Class to get PDF metadata
class GetPDFMetaData:
    def __init__(self, file):
        self.file = file

    def get_metadata(self):
        reader = PyPDF2.PdfReader(self.file)
        metadata = reader.metadata
        return metadata

    
class DocsSpecs:
    def __init__(self, file_id=None, file = None, dest_bucket=None, dest_path=None, extractor=None, writer=None):
        self.dest_bucket = dest_bucket
        self.dest_file = dest_path
        self.file = file
        self.extractor = extractor
        self.writer = writer
        self.bucket_factory = BucketFactory()
        self.file_id = file_id
        
    def set_file_id(self, file_id):
        self.file_id = file_id
    def get_file_id(self):
        return self.file_id
    
    def set_file(self, file):
        self.file = file
        
    def set_dest_bucket(self, dest_bucket):
        self.dest_bucket = dest_bucket 
        
    def set_dest_file(self, dest_file):
        self.dest_file = dest_file
    
    def set_extractor(self, extractor):
        self.extractor = extractor
    
    def set_writer(self,writer):
        self.writer = writer
    
    def get_bq_parameters(self):
        return self.bucket_factory.get_bq_parameters()
    

class BucketFactory:
    def __init__(self):
        self.types = {
            'pdf': {
                'bucket_ocr': 'ngcs-chat-poc-temp-bucket',
                'bucket_txt': 'ngcs-chat-poc-txt-bucket'
            },
            'docx': 'ngcs-chat-poc-txt-bucket',
            'bq': {
                'project_id': 'ai-services-401511',
                'dataset_id': 'chat',
                'table_id': 'dt_Document'
            }
        }

    def get_buckets(self, file_type):
        return self.types.get(file_type, None)
    
    def get_bq_parameters(self):
        return self.types.get('bq')
    
    
class DocumentCassifier:
    def __init__(self,path,file):
        self.path = path
        self.file = file
        self.docs_specs = DocsSpecs()
        self.docs_specs.set_file(self.file)
        self.type = self.path.split('.')[-1]
        self.bucket_factory = BucketFactory()
        self.file_id = self.path.split('/')[-1].split('.')[0]
        

    def check_file_type(self):
        if self.type == 'pdf':
            metadata = GetPDFMetaData(self.file).get_metadata()
            if ('ocrmypdf' in metadata.get('/Creator', '')):
                dest_path = str(Path(self.path.split('/')[-1]).with_suffix('.txt'))
                dest_bucket = self.bucket_factory.get_buckets('pdf')['bucket_txt']
                extractor = PDFTextExtractor(self.path.split('/')[-1])
                writer = WriteTextToBucket(dest_bucket, dest_path)
            else:
                dest_bucket = self.bucket_factory.get_buckets('pdf')['bucket_ocr']
                dest_path = str(Path(self.path.split('/')[-1]).with_suffix('.pdf'))
                writer = WriteObjectToBucket(dest_bucket, dest_path)
        elif self.type == 'docx':
            extractor = DOCXTextExtractor(self.path.split('/')[-1])
            dest_bucket = self.bucket_factory.get_buckets('docx')
            dest_path = str(Path(self.path.split('/')[-1]).with_suffix('.txt'))
            writer = WriteObjectToBucket(dest_bucket,dest_path)
        self.docs_specs.set_extractor(extractor) if extractor else self.docs_specs.set_extractor(None)
        self.docs_specs.set_writer(writer)
        self.docs_specs.set_dest_bucket(dest_bucket)
        self.docs_specs.set_dest_file(dest_path)
        self.docs_specs.set_file_id(self.file_id)
        return  self.docs_specs
    
# Abstract TextExtractor class
class TextExtractor(ABC):
    def __init__(self, obj):
        self.obj = obj

    @abstractmethod
    def text_extract(self):
        pass


# PDF Text Extractor
class PDFTextExtractor(TextExtractor):
    def __init__(self, obj):
        super().__init__(obj)

    def text_extract(self):
        text = ""
        with open(self.obj, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page_number, page in enumerate(reader.pages):
                if page_number >= 10:  # Limit to the first 10 pages
                    break
                if page.extract_text():
                    text += page.extract_text() + "\n"
        return text


# DOCX Text Extractor
class DOCXTextExtractor(TextExtractor):
    def __init__(self, obj):
        super().__init__(obj)

    def text_extract(self):
        # Extract text from .docx file
        text = docx2txt.process(self.obj)
        return text



from google.cloud import storage
import io
class WriteFileToBucket(ABC):
    def __init__(self, bucket_name, dest_file):
        """
        Initialize the class with the bucket name and destination file path.
        """
        if not isinstance(bucket_name, str) or not bucket_name.strip():
            raise ValueError("Invalid bucket name provided.")
        self.bucket_name = re.sub(r'[^a-zA-Z0-9\-]', '', bucket_name)
        
        print(self.bucket_name)
        self.dest_file = dest_file
        storage_client = storage.Client()

        # Get the bucket object
        try:
            self.bucket = storage_client.get_bucket(self.bucket_name)
            self.blob = self.bucket.blob(self.dest_file)
        except Exception as e:
            print(e)
    @abstractmethod
    def upload_file(self):
        pass

class WriteTextToBucket(WriteFileToBucket):
    def __init__(self, bucket_name, dest_file):
        super().__init__(bucket_name, dest_file)
    
    def upload_file(self, file_data):
        file_content = file_data.encode('utf-8')  # Encode the string as UTF-8
        file_stream = io.BytesIO(file_content)  # Create a BytesIO stream from the bytes
        self.blob.upload_from_file(file_stream, rewind=True)

class WriteObjectToBucket(WriteFileToBucket):
    def __init__(self, bucket_name, dest_file):
        super().__init__(bucket_name, dest_file) 
                          
    def upload_file(self, file_data):
        if isinstance(file_data, str):
            # If file_data is a string (e.g., extracted text), encode it as UTF-8
            file_data = io.BytesIO(file_data.encode('utf-8'))
        
        # Upload from BytesIO object
        self.blob.upload_from_file(file_data, rewind=True)
        file_data.seek(0)  # Reset the stream to the beginning

class DocumentFlow:
    def __init__(self,bucket, name):
        self.path = f"{bucket}/{name}"
        self.bucket = bucket
        self.name = name
            
    def process(self):
        
        #download
        file = DownloadFileFromBucket(self.path).download_file()
        #check type
        specs = DocumentCassifier(self.path,file).check_file_type()
        #get content
        content = specs.extractor.text_extract() if specs.extractor else specs.file
        #bq

        bq_updater = BigqueryUpdater(specs)
        #write to bucket and update
        try:
            specs.writer.upload_file(content)
            bq_updater.update_bigquery_row()
        except Exception as e:
            print(e)
        


from google.cloud import bigquery

class BigqueryUpdater:
    
    def __init__(self,specs):
        self.specs = specs
        self.parameters = self.specs.get_bq_parameters()
    
    def update_bigquery_row(self):
        """
        Updates specific columns in a row in a BigQuery table.

        :param project_id: GCP project ID where the BigQuery table resides
        :param dataset_id: Dataset ID containing the table
        :param table_id: Table ID (name of the table)
        :param column_updates: Dictionary of column names and their new values
        :param condition: Condition for the row(s) to be updated (e.g., "id = 123")
        """
        client = bigquery.Client(project=self.parameters['project_id'])
                
        column_updates = {'Path' : self.specs.dest_file,
                              'POCCreateDate' : datetime.now().strftime("%Y-%m-%d")}
        condition = f"FileID = '{self.specs.get_file_id()}'"
        # Correctly format each column and value with space after literals
        set_clauses = ", ".join([f"`{column}` = '{value}'" if isinstance(value, str) else f"`{column}` = {value}"
                                 for column, value in column_updates.items()])
        
        # Ensure that the condition is formatted correctly as well
        query = f"""
        UPDATE `{self.parameters['project_id']}.{self.parameters['dataset_id']}.{self.parameters['table_id']}`
        SET {set_clauses}
        WHERE {condition}
        """

        print("Constructed query:", query)  # Debug print to check query
        query_job = client.query(query)
        query_job.result()  # Wait for the query to finish
        print("Update completed successfully")
