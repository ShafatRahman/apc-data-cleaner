"""
To run this file, simply call 'python apc_processor.py <start_date> <end_date>'
"""
import argparse
import datetime
from src.utils.data_loaders import APCDataLoader
from src.exceptions.data_loading import DataLoaderError
from src.utils.data_processors import APCDataProcessor
from src.exceptions.data_processing import DataProcessorError

def process_apc_data(folder_path: str, start_date_and_time: datetime.datetime, end_date_and_time: datetime.datetime, 
                    output_file: str, vehicle_ids: list[int] = []) -> str:
    try:
        if not isinstance(folder_path, str):
            raise TypeError("folder_path must be a string")
        if not isinstance(start_date_and_time, datetime.datetime) or not isinstance(end_date_and_time, datetime.datetime):
            raise TypeError("start_date and end_date must be datetime.date objects")
        if start_date_and_time > end_date_and_time:
            raise ValueError("start_date cannot be later than end_date")

        loader = APCDataLoader({folder_path}, {"ridership-data-*.csv"})
        processor = APCDataProcessor(loader.get_data())

        processor.filter_by_date_and_time(start_date_and_time, end_date_and_time)

        if vehicle_ids:
            processor.filter_by_vehicle(set(vehicle_ids))

        result = processor.process()
        result.to_csv(output_file, index=False)
        return output_file

    except (DataLoaderError, DataProcessorError) as e:
        print(f"Error processing APC data: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Parse APC data for upload to Hopthru")

        parser.add_argument(
            "folder_path",
            type=str,
            help="The folder path to the APC data",
        )
        parser.add_argument(
            "start_date",
            type=datetime.datetime.fromisoformat,
            help="The first date to upload, in YYYY-MM-DDTHH:MM:SS format.",
        )
        parser.add_argument(
            "end_date",
            type=datetime.datetime.fromisoformat,
            help="The first date to upload, in YYYY-MM-DDTHH:MM:SS format.",
        )
        parser.add_argument(
            "--output",
            "-o",
            help="Output file name",
            default="results.csv",
        )
        parser.add_argument(
            "--vehicle-ids",
            '-v',
            type=int,
            help="Optional list of vehicle IDs to process",
            nargs="*",
        )


        options = parser.parse_args()
        print(process_apc_data(
            options.folder_path,
            options.start_date,
            options.end_date,
            options.output,
            options.vehicle_ids
        ))

    except Exception as e:
        print(f"Error: {str(e)}")
        exit(1)
