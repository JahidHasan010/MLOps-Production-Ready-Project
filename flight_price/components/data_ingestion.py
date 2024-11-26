import os
import sys

from pandas import DataFrame
from sklearn.model_selection import train_test_split

from flight_price.entity.config_entity import DataIngestionConfig
from flight_price.entity.artifact_entity import DataIngestionArtifact
from flight_price.exception import FlightException
from flight_price.logger import logging
from flight_price.data_access.flight_data import FlightData


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig = DataIngestionConfig()):
        """
        :param data_ingestion_config: configuration for data ingestion
        """
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise FlightException(e, sys)

    def export_data_into_feature_store(self) -> DataFrame:
        """
        Method Name :   export_data_into_feature_store
        Description :   This method exports data from MongoDB to a CSV file.
        
        Output      :   DataFrame is returned as an artifact of data ingestion components.
        On Failure  :   Logs an exception and raises it.
        """
        try:
            logging.info(f"Exporting data from MongoDB")
            flight_data = FlightData()
            dataframe = flight_data.export_collection_as_dataframe(
                collection_name=self.data_ingestion_config.collection_name
            )
            
            if dataframe.empty:
                logging.error("The exported DataFrame is empty. Check the MongoDB collection.")
                raise ValueError("The exported DataFrame is empty.")

            logging.info(f"Shape of DataFrame: {dataframe.shape}")
            
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            
            logging.info(f"Saving exported data into feature store file path: {feature_store_file_path}")
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            
            return dataframe
        except Exception as e:
            raise FlightException(e, sys)

    def split_data_as_train_test(self, dataframe: DataFrame) -> None:
        """
        Method Name :   split_data_as_train_test
        Description :   Splits the DataFrame into train and test sets based on the split ratio.
        
        Output      :   Train and test datasets are saved as CSV files.
        On Failure  :   Logs an exception and raises it.
        """
        logging.info("Entered split_data_as_train_test method of DataIngestion class")

        try:
            if dataframe.empty:
                logging.error("Input DataFrame is empty. Cannot perform train-test split.")
                raise ValueError("Input DataFrame is empty.")

            train_set, test_set = train_test_split(
                dataframe, test_size=self.data_ingestion_config.train_test_split_ratio, random_state=42
            )
            
            if train_set.empty or test_set.empty:
                logging.error("Train or test set is empty after the split. Check split ratio and input data.")
                raise ValueError("Train or test set is empty after the split.")

            logging.info("Performed train-test split on the DataFrame")
            
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)
            
            logging.info(f"Exporting train and test datasets to file paths")
            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False, header=True)

            logging.info("Exported train and test datasets to file paths")
        except Exception as e:
            raise FlightException(e, sys) from e

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """
        Method Name :   initiate_data_ingestion
        Description :   Initiates the data ingestion component of the training pipeline.
        
        Output      :   Returns DataIngestionArtifact containing paths for train and test datasets.
        On Failure  :   Logs an exception and raises it.
        """
        logging.info("Entered initiate_data_ingestion method of DataIngestion class")

        try:
            dataframe = self.export_data_into_feature_store()
            logging.info("Data successfully exported from MongoDB")

            self.split_data_as_train_test(dataframe)
            logging.info("Train-test split successfully performed on the dataset")

            logging.info("Exited initiate_data_ingestion method of DataIngestion class")

            data_ingestion_artifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path,
            )
            
            logging.info(f"Data ingestion artifact: {data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise FlightException(e, sys) from e
