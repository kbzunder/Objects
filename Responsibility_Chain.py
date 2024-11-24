import io
import os
from google.cloud import storage
from google.cloud import bigquery
from abc import ABC, abstractmethod
import PyPDF2
import docx2txt  # Import the library for extracting text from docx files
from pathlib import Path
import re
from datetime import datetime
from abc import ABC, abstractmethod
from google.cloud import firestore
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
cred = credentials.ApplicationDefault()

firebase_admin.initialize_app(cred)

class ProjectSpecs:
    def __init__(self, bucket, filename, destination):
        self.input_pdf_bucket = bucket
        self.input_fileid = filename
        self.file_id = self.input_fileid.split('_')[0]
        self.file_type = self.input_fileid.split('.')[-1]
        self.project_id = os.environ.get("PROJECT_ID",'gcp-dev-netcourt-mewest1')
        self.location = os.environ.get("LOCATION",'me-west1')
        self.dataset_id = os.environ.get("DATASET_ID",'ngcs_chat_poc_dataset_DEV')
        self.doc_table_id = os.environ.get("DOC_TABLD_ID",'dt_Document')
        self.output_txt_bucket = os.environ.get("OutputTextBucket",'ngcs-chat-poc-txt-bucket')
        self.firestore_database = os.environ.get("FireDatabase", 'ngcs-chat')
        self.firestore_collection =  os.environ.get("FireCollection", 'Docs')
        self.text_extractor = None
        self.writer = None
        self.doc_type = None
        self.case_id = None
        self.file_object = None
        self.text = None
        self.destination = destination
        self.output_bucket = None
        self.output_path = None
        self.num_chars = None
        self.max_firestore_size = 850_000
    
    def get_max_firestore_size(self):
        return self.max_firestore_size
    
    def get_dataset_id(self):
        return self.dataset_id
    
    def get_doc_table_id(self):
        return self.doc_table_id
    def get_num_characters(self):
        return self.num_chars
    
    def set_num_characters(self, num_chars):
        self.num_chars = num_chars
        
    def get_output_txt_bucket(self):
        return self.output_txt_bucket
    
    def get_output_path(self):
        return self.output_path
    
    def set_output_path(self, path):
        self.output_path = path
        
    def set_output_bucket(self, output_bucket):
        self.output_bucket = output_bucket
        
    def get_output_bucket(self):
        return self.output_bucket
    
    def get_destination(self):
        return self.destination
    
    def get_text(self):
        return self.text
    
    def set_text(self, text):
        self.text = text
        
    def get_file_object(self):
        return self.file_object
    
    def set_file_object(self, file_object):
        self.file_object = file_object
    
    def get_file_type(self):
        return self.file_type
    
    def get_input_pdf_bucket(self):
        return self.input_pdf_bucket
    
    def get_input_fileid(self):
        return self.input_fileid
    
    def get_file_id(self):
        return self.file_id
    
    def get_firestore_database(self):
        return self.firestore_database
    
    def get_firestore_collection(self):
        return self.firestore_collection
    
    def get_file_type(self):
        return self.file_type
    
    def get_project_id(self):
        return self.project_id
    
    def get_location(self):
        return location
    
    def get_text_extractor(self):
        return self.text_extractor
    
    def set_text_extractor(self, text_extractor):
        self.text_extractor = text_extractor
        
    def get_writer(self):
        return self.writer
    
    def set_writer(self, writer):
        self.writer = writer

    def get_case_id(self):
        """
        Fetches metadata CaseID from BigQuery for a given FileID.
        """
        client = bigquery.Client()
        query = f"""
            SELECT distinct CaseID
            FROM `{self.project_id}.{self.dataset_id}.{self.doc_table_id}`
            WHERE FileID = '{self.file_id}'
        """
        query_job = client.query(query)
        results = [dict(row) for row in query_job]
        if results:
            return results[0]["CaseID"]
        else:
            raise ValueError(f"No metadata found in BigQuery for FileID: {self.file_id}")

    def get_document_type(self):
        """
        Fetches metadata DocumentTypeID from BigQuery for a given FileID.
        """
        client = bigquery.Client()
        query = f"""
            SELECT distinct DocumentTypeID
            FROM `{self.project_id}.{self.dataset_id}.{self.doc_table_id}`
            WHERE FileID = '{self.file_id}'
        """
        query_job = client.query(query)
        results = [dict(row) for row in query_job]
        if results:
            return results[0]["DocumentTypeID"]
        else:
            raise ValueError(f"No metadata found in BigQuery for FileID: {self.file_id}")


class Worker(ABC):
    def __init__(self, specs, next_worker=None):
        self.specs = specs
        self.next_worker = next_worker

    @abstractmethod
    def handle(self):
        """Process the current step and pass to the next worker if needed."""
        pass


class DownloadFileWorker(Worker):
    def handle(self):
        bucket_name = self.specs.get_input_pdf_bucket()
        file_name = self.specs.get_input_fileid()
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        file = io.BytesIO()
        try:
            blob.download_to_file(file)
            file.seek(0)
            print(f"File {file_name} downloaded from bucket {bucket_name}.")
            self.specs.set_file_object(file)
        except Exception as e:
            print(f"Failed to download file: {e}")
            return  # Stop the chain if download fails
        self.next_worker = CheckFileTypeWorker(self.specs)
        
        if self.next_worker:
            self.next_worker.handle()


class CheckFileTypeWorker(Worker):
    def handle(self):
        file_type = self.specs.get_file_type()
        print(f"Checking file type: {file_type}")
        
        if file_type == "pdf":
            print("File type is PDF. Setting CheckOCRWorker as the next worker.")
            self.next_worker = CheckOCRWorker(self.specs)
        elif file_type == "docx":
            print("File type is DOCX. Setting TextExtractorWorker as the next worker.")
            self.next_worker = TextExtractorWorker(self.specs)
        else:
            print(f"Unsupported file type: {file_type}")
            return  # Stop the chain if the file type is unsupported
        
        if self.next_worker:
            self.next_worker.handle()


class CheckOCRWorker(Worker):   
    def handle(self):
        reader = PyPDF2.PdfReader(self.specs.get_file_object())
        metadata = reader.metadata
        creator = metadata.get('/Creator', '').lower()  # Make it case-insensitive

        if 'ocrmypdf' in creator:
            print("OCR detected. Setting TextExtractorWorker as the next worker.")
            self.next_worker = TextExtractorWorker(self.specs)
        else:
            print("OCR not detected. Setting WriteObjectToBucketWorker as the next worker.")
            self.specs.set_output_bucket(self.specs.get_input_pdf_bucket())
            self.specs.set_output_path(self.specs.get_input_fileid())
            self.next_worker = WriteObjectToBucketWorker(self.specs)
        
        if self.next_worker:
            self.next_worker.handle()

class WriteObjectToBucketWorker(Worker):
    def handle(self):
        writer = WriteObjectToBucket(self.specs)
        writer.upload_file()
        print(f'Pdf uploaded to input bucket. Work done')
        
            
class TextExtractorWorker(Worker):
    def handle(self):
        TextExtractorFactory(self.specs).get_extractor()
        extractor = self.specs.get_text_extractor()
        text = extractor.text_extract()
        self.specs.set_text(text)
        self.specs.set_num_characters(len(text))
        self.specs.set_output_bucket(self.specs.get_output_txt_bucket())
        self.specs.set_output_path(f"{self.specs.get_input_fileid().split('.')[0]}.txt")
        print("Setting SaveTextWorker as the next worker.")
        self.next_worker = SaveTextWorker(self.specs)
    
        if self.next_worker:
            self.next_worker.handle()
        
class CheckDestinationFactory:
    def __init__(self, specs):
        self.specs = specs
    
    def get_destination(self):
        if self.specs.get_destination() == 'storage':
            writer = WriteTextToBucket(self.specs)
        elif self.specs.get_destination() =='datastore':
            writer = WriteTextToDatastore(self.specs)
        self.specs.set_writer(writer)
        return writer
    
class SaveTextWorker(Worker): 
    def handle(self):
        CheckDestinationFactory(self.specs).get_destination()
        writer = self.specs.get_writer()
        writer.upload_file()
        print("Setting BigqueryUpdaterWorker as the next worker.")
        self.next_worker = BigqueryUpdaterWorker(self.specs)
    
        if self.next_worker:
            self.next_worker.handle()

class TextExtractor(ABC):
    def __init__(self, specs):
        self.specs = specs

    @abstractmethod
    def text_extract(self):
        pass


# PDF Text Extractor
class PDFTextExtractor(TextExtractor):
    def text_extract(self):
        text = ""
        file = self.specs.get_file_object()
        reader = PyPDF2.PdfReader(file)
        for page_number, page in enumerate(reader.pages):
            # if page_number >= 10:  # Limit to the first 10 pages
            #     break
            if page.extract_text():
                page_text = page.extract_text()
                for line in page_text.splitlines():
                    rev_line = line.split()[::-1]
                    rev_txt = " ".join(rev_line)

                    text += rev_txt + "\n"
        return text


# DOCX Text Extractor
class DOCXTextExtractor(TextExtractor):
    def text_extract(self):
        # Extract text from .docx file
        file = self.specs.get_file_object()
        text = docx2txt.process(file)
        return text
    


class TextExtractorFactory:
    def __init__(self, specs):
        self.specs = specs
        
    def get_extractor(self):
        if self.specs.get_file_type() == 'pdf':
            extractor = PDFTextExtractor(self.specs)
            
        elif self.specs.get_file_type() == 'docx':
            extractor = DOCXTextExtractor(self.specs)
        self.specs.set_text_extractor(extractor)
        return extractor
        
    




from google.cloud import storage
import io
class WriteFileToBucket(ABC):
    def __init__(self, specs):
        """
        Initialize the class with the bucket name and destination file path.
        """
        self.specs = specs
        bucket_name = self.specs.get_output_bucket()
        self.dest_file = self.specs.get_output_path() #f"{self.specs.get_input_fileid().split('.')[0]}.txt"
        if not isinstance(bucket_name, str) or not bucket_name.strip():
            raise ValueError("Invalid bucket name provided.")
        self.bucket_name = re.sub(r'[^a-zA-Z0-9\-]', '', bucket_name)
        
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
    def __init__(self, specs):
        super().__init__(specs)
    
    def upload_file(self):
        file_data = self.specs.get_text()
        file_content = file_data.encode('utf-8')  # Encode the string as UTF-8
        file_stream = io.BytesIO(file_content)  # Create a BytesIO stream from the bytes
        self.blob.upload_from_file(file_stream, rewind=True)
        print(f'File {self.specs.get_output_path()} uploaded to {self.specs.get_output_bucket()}')

class WriteObjectToBucket(WriteFileToBucket):
    def __init__(self, specs):
        super().__init__(specs) 
                          
    def upload_file(self):
        file_data = self.specs.get_file_object()
        if isinstance(file_data, str):
            # If file_data is a string (e.g., extracted text), encode it as UTF-8
            file_data = io.BytesIO(file_data.encode('utf-8'))
        
        # Upload from BytesIO object
        self.blob.upload_from_file(file_data, rewind=True)
        file_data.seek(0)  # Reset the stream to the beginning


class WriteTextToDatastore:
    def __init__(self, specs):
        self.specs = specs
        self.db = firestore.client(database_id=self.specs.get_firestore_database())
        self.collection_name = self.specs.get_firestore_collection()
        
    def upload_file(self):
        file_content = self.specs.get_text()
        """
        Uploads the file content to Firestore, splitting into parts if necessary.
        """
        # Fetch metadata from BigQuery
        
        case_id = self.specs.get_case_id()
        d_type = self.specs.get_document_type()
        
        # File size and splitting
        file_bytes = file_content.encode("utf-8")
        max_size = self.specs.get_max_firestore_size()  # Max size per part (in bytes)
        parts = []
        start = 0
        part_number = 1

        while start < len(file_bytes):
            end = start + max_size
            part_content = file_bytes[start:end].decode("utf-8", errors="ignore")
            parts.append({
                "part_number": part_number,
                "text": part_content,
                "chars_count": self.specs.get_num_characters(),
            })
            start = end
            part_number += 1

        # Metadata for Firestore
        n_parts = len(parts)
        for part in parts:
            doc_ref = self.db.collection(self.collection_name).document(f"{self.specs.get_file_id()}_part_{part['part_number']}")
            doc_data = {
                "Path": f"{self.specs.get_input_fileid().split('.')[0]}_part_{part['part_number']}.txt",
                "CaseID": case_id,
                "DocumentID": self.specs.get_file_id(),
                "CharsCount": part["chars_count"],
                "DocumentTypeID": d_type,
                "FileID": f"{self.specs.get_input_fileid().split('.')[0]}_part_{part['part_number']}",
                "Parts": n_parts,
                "text": part["text"],
            }
            doc_ref.set(doc_data)

        print(f"Uploaded {n_parts} part(s) for FileID: {self.specs.get_file_id()} to Firestore.")



class BigqueryUpdaterWorker(Worker):
        
    def handle(self):
        
        client = bigquery.Client(project=self.specs.get_project_id())
        print(self.specs.get_num_characters())      
        column_updates = {'Path' : self.specs.get_output_path(),
                              'POCCreateDate' : datetime.now().strftime("%Y-%m-%d"), 'CharsCount':self.specs.get_num_characters()}
        condition = f"FileID = '{self.specs.get_file_id()}'"
        # Correctly format each column and value with space after literals
        set_clauses = ", ".join([f"`{column}` = '{value}'" if isinstance(value, str) else f"`{column}` = {value}"
                                 for column, value in column_updates.items()])
        
        # Ensure that the condition is formatted correctly as well
        query = f"""
        UPDATE `{self.specs.get_project_id()}.{self.specs.get_dataset_id()}.{self.specs.get_doc_table_id()}`
        SET {set_clauses}
        WHERE {condition}
        """

        print("Constructed query:", query)  # Debug print to check query
        query_job = client.query(query)
        query_job.result()  # Wait for the query to finish
        print("Update completed successfully")


class DocumentFlow:
    def __init__(self, specs):
        self.specs = specs
        
    def process(self):
        download_worker = DownloadFileWorker(self.specs)
        download_worker.handle()
