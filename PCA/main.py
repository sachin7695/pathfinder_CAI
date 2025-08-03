import pymongo
from pymongo import MongoClient
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CallTrackingProcessor:
    def __init__(self, connection_string, database_name):
        """
        Initialize the MongoDB connection
        
        Args:
            connection_string (str): MongoDB connection string
            database_name (str): Name of the database
        """
        try:
            self.client = MongoClient(connection_string)
            self.db = self.client[database_name]
            self.collection = self.db['call_tracking']
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def process_document(self, document):
        """
        Process a single document - customize this method based on your needs
        
        Args:
            document (dict): The document to process
        
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            # Add your custom processing logic here
            logger.info(f"Processing document with ID: {document.get('_id')}")
            
            # Example processing logic (replace with your actual logic)
            # You might want to:
            # - Extract data from the document
            # - Perform calculations
            # - Call external APIs
            # - Transform data
            # etc.
            
            print(f"Document data: {document}")
            
            # Simulate some processing time
            # time.sleep(1)
            
            logger.info(f"Successfully processed document: {document.get('_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing document {document.get('_id')}: {e}")
            return False
    
    def update_flag(self, document_id, flag_value=True):
        """
        Update the flag field for a processed document
        
        Args:
            document_id: The _id of the document to update
            flag_value (bool): The new flag value (default: True)
        """
        try:
            result = self.collection.update_one(
                {"_id": document_id},
                {
                    "$set": {
                        "flag": flag_value,
                        "processed_at": datetime.utcnow()
                    }
                }
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated flag for document: {document_id}")
            else:
                logger.warning(f"No document updated for ID: {document_id}")
                
        except Exception as e:
            logger.error(f"Error updating flag for document {document_id}: {e}")
    
    def process_unprocessed_documents(self, batch_size=100):
        """
        Find and process all documents where flag is False
        
        Args:
            batch_size (int): Number of documents to process in each batch
        """
        try:
            # Query for documents where flag is False
            query = {"flag": False}
            
            # Count total documents to process
            total_count = self.collection.count_documents(query)
            logger.info(f"Found {total_count} documents to process")
            
            if total_count == 0:
                logger.info("No documents to process")
                return
            
            processed_count = 0
            failed_count = 0
            
            # Process documents in batches
            cursor = self.collection.find(query).batch_size(batch_size)
            
            for document in cursor:
                try:
                    # Process the document
                    success = self.process_document(document)
                    
                    if success:
                        # Update flag to True after successful processing
                        self.update_flag(document['_id'], True)
                        processed_count += 1
                    else:
                        failed_count += 1
                        logger.error(f"Failed to process document: {document['_id']}")
                
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Error processing document {document.get('_id')}: {e}")
                
                # Log progress every 10 documents
                if (processed_count + failed_count) % 10 == 0:
                    logger.info(f"Progress: {processed_count + failed_count}/{total_count} documents processed")
            
            logger.info(f"Processing complete. Processed: {processed_count}, Failed: {failed_count}")
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            raise
    
    def get_unprocessed_count(self):
        """
        Get the count of unprocessed documents
        
        Returns:
            int: Number of documents with flag=False
        """
        try:
            count = self.collection.count_documents({"flag": False})
            return count
        except Exception as e:
            logger.error(f"Error getting unprocessed count: {e}")
            return 0
    
    def close_connection(self):
        """Close the MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

def main():
    # Configuration
    CONNECTION_STRING = "mongodb://localhost:27017/"  # Update with your MongoDB connection string
    DATABASE_NAME = "your_database_name"  # Update with your database name
    
    try:
        # Initialize processor
        processor = CallTrackingProcessor(CONNECTION_STRING, DATABASE_NAME)
        
        # Check how many documents need processing
        unprocessed_count = processor.get_unprocessed_count()
        print(f"Documents awaiting processing: {unprocessed_count}")
        
        if unprocessed_count > 0:
            # Process all unprocessed documents
            processor.process_unprocessed_documents()
        else:
            print("No documents to process")
        
    except Exception as e:
        logger.error(f"Application error: {e}")
    
    finally:
        # Clean up
        if 'processor' in locals():
            processor.close_connection()

if __name__ == "__main__":
    main()