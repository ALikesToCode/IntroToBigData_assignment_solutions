from google.cloud import storage
import sys
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s', stream=sys.stderr)

def count_lines_in_gcs_file(bucket_name: str, blob_name: str) -> int | None:
    """Counts the number of lines in a file stored in GCS.

    Downloads the blob's content as text and splits it into lines.
    For very large files, consider streaming the download to conserve memory
    (e.g., using blob.open('r') and iterating line by line).

    Args:
        bucket_name: The name of the GCS bucket.
        blob_name: The name of the blob (file) in the GCS bucket.

    Returns:
        The number of lines in the file, or None if an error occurs.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        logging.info(f"Attempting to download file gs://{bucket_name}/{blob_name}...")
        
        # Check if the blob exists before attempting to download
        if not blob.exists():
            logging.error(f"File not found: gs://{bucket_name}/{blob_name}")
            return None

        # download_as_text() is convenient but loads the whole file into memory.
        # For extremely large files, blob.open('r') as f: for line in f: ... would be more memory-efficient.
        content = blob.download_as_text()
        
        lines = content.splitlines()
        line_count = len(lines)
        
        logging.info(f"Successfully downloaded and counted lines for gs://{bucket_name}/{blob_name}.")
        return line_count
    except storage.exceptions.NotFound:
        logging.error(f"File not found (NotFound exception): gs://{bucket_name}/{blob_name}")
        return None
    except storage.exceptions.Forbidden as e:
        logging.error(f"Permission denied for gs://{bucket_name}/{blob_name}. Details: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while processing gs://{bucket_name}/{blob_name}: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Count lines in a file stored in Google Cloud Storage."
    )
    parser.add_argument(
        "bucket_name", 
        help="The name of the GCS bucket (e.g., 'my-bucket')."
    )
    parser.add_argument(
        "blob_name", 
        help="The name/path of the file in the GCS bucket (e.g., 'data/my_file.txt')."
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging (DEBUG level)."
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG) # Set root logger to DEBUG
        logging.debug("Verbose logging enabled.")


    line_count = count_lines_in_gcs_file(args.bucket_name, args.blob_name)

    if line_count is not None:
        # Print only the count to stdout for easy redirection
        print(line_count)
    else:
        # Error messages are already handled by the logging configuration to stderr
        sys.exit(1) 