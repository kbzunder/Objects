import io
from google.cloud import storage
from abc import ABC, abstractmethod
import PyPDF2
import docx2txt  # Import the library for extracting text from docx files
from pathlib import Path


# Validator class
class Validator(ABC):
    def __init__(self, obj):
        self.obj = obj

    @abstractmethod
    def validate(self):
        pass


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
    def __init__(self, path, DownloadFileFromBucket):
        self.path = path
        self.file = DownloadFileFromBucket(path).download_file()

    def get_metadata(self):
        reader = PyPDF2.PdfReader(self.file)
        metadata = reader.metadata
        return metadata


# OCR Validator class
class NeedOCRValidator(Validator):
    def __init__(self, obj):
        super().__init__(obj)
        self.obj = obj
        if obj.endswith('.pdf'):
            self.metadata = GetPDFMetaData(self.obj, DownloadFileFromBucket).get_metadata()

    def validate(self):
        if self.obj.endswith(".docx"):
            return False
        elif self.metadata and ('ocrmypdf' in self.metadata.get('/Creator', '')):
            return False
        elif self.obj.endswith('.pdf'):
            return True


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


# Factory class for creating TextExtractor instances
class TextExtractorFactory:
    @staticmethod
    def create_text_extractor(obj):
        # Determine the file type based on the extension and return the appropriate class
        if obj.endswith('.pdf'):
            return PDFTextExtractor(obj)
        elif obj.endswith('.docx'):
            return DOCXTextExtractor(obj)
        else:
            raise ValueError("Unsupported file type")


from google.cloud import storage
import io


class WriteFileToBucket:
    def __init__(self, bucket_name, dest_file):
        """
        Initialize the class with the bucket name and destination file path.
        """
        self.bucket_name = bucket_name
        self.dest_file = dest_file

    def upload_file(self, file_data):
        """
        Upload the file to the specified Google Cloud Storage bucket from memory.

        :param file_data: The content to upload (can be a string or BytesIO).
        """
        # Create a client to interact with Google Cloud Storage
        storage_client = storage.Client()

        # Get the bucket object
        bucket = storage_client.get_bucket(self.bucket_name)

        # Create a blob object in the bucket where the file will be stored
        blob = bucket.blob(self.dest_file)

        # Upload the file from memory (string or BytesIO)
        if isinstance(file_data, str):
            file_content = file_data.encode('utf-8')  # Encode string to UTF-8
            file_stream = io.BytesIO(file_content)  # Create a BytesIO stream from the bytes
            blob.upload_from_file(file_stream, rewind=True)
        elif isinstance(file_data, io.BytesIO):
            blob.upload_from_file(file_data, rewind=True)  # Upload from BytesIO object
            file_data.seek(0)  # Reset the stream to the beginning
        else:
            raise ValueError("Unsupported file format or type.")

        print(f"File uploaded to {self.dest_file} in bucket {self.bucket_name}.")


class WriteFileFactory:
    @staticmethod
    def create_writer(file_type, file_name):
        """
        Create a WriteFileToBucket instance based on the file type.

        :param file_type: A string representing the file type (e.g., 'pdf', 'txt').
        :param file_name: The name of the file to be saved in the bucket.
        :return: An instance of WriteFileToBucket for the specified file type.
        """
        # Determine the destination bucket and file path based on the file type
        if file_type == 'pdf':
            return WriteFileToBucket('chat-poc-temp-bucket', str(Path(file_name).with_suffix('.pdf')))
        elif file_type == 'txt':
            return WriteFileToBucket('chat-poc-txt-bucket', str(Path(file_name).with_suffix('.txt')))
        else:
            raise ValueError(f"Unsupported file type: {file_type}")


class FileProcessor:
    def __init__(self, bucket, name, NeedOCRValidator, DownloadFileFromBucket, TextExtractorFactory, WriteFileFactory):
        """
        Initialize the FileProcessor with the bucket, file name, and the components required for validation,
        downloading, extracting, and writing the file.
        """
        self.bucket = bucket
        self.name = name
        self.file_path = f"{bucket}/{name}"

        # Initialize the validator, downloader, and extractor
        self.validator = NeedOCRValidator(self.file_path)
        self.downloader = DownloadFileFromBucket(self.file_path)
        self.extractor = TextExtractorFactory.create_text_extractor(self.name)
        self.writer_factory = WriteFileFactory  # Use the factory for creating the writer

    def process(self):
        """
        Process the file: validate, download or extract, then upload.
        """
        # Validate the file

        if self.validator.validate():
            # If validation passes, download the file and upload it as a PDF
            downloaded_file_content = self.downloader.download_file()  # Assuming this returns a BytesIO or file content
            writer = self.writer_factory.create_writer('pdf', self.name)  # Get PDF writer
            writer.upload_file(downloaded_file_content)
        else:
            # If validation fails, extract text and upload it as a TXT file
            extracted_text = self.extractor.text_extract()
            writer = self.writer_factory.create_writer('txt', self.name)  # Get TXT writer
            writer.upload_file(extracted_text)
