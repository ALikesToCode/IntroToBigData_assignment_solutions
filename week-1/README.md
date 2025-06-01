# Assignment: Line Count of a GCS File via GCP VM

## 1. Explaining the Problem Statement

The primary objective of this assignment is to count the number of lines in a text file that is stored in Google Cloud Storage (GCS). This task needs to be performed by a Python script running on a Google Cloud Platform (GCP) Virtual Machine (VM). The deliverables are the Python script itself and a text file containing the output (the line count).

This involves:
-   Accessing a file in GCS from a GCP VM.
-   Reading the file content.
-   Processing the content to count lines.
-   Running the script in a GCP environment.
-   Storing the result in a specified output format.

## 2. Explaining your approach to reach the objective

The approach involves the following key steps:

1.  **Develop a Python Script:**
    *   Utilize the `google-cloud-storage` Python library to interact with GCS.
    *   The script will be designed to accept the GCS bucket name and the object (file) path as command-line arguments for flexibility.
    *   It will download the specified file from GCS.
    *   It will then read the content and count the lines (typically by splitting the content by newline characters).
    *   Standard output (`stdout`) will be used for the final line count, allowing easy redirection to a file.
    *   Error messages and informational logs will be directed to standard error (`stderr`) using the `logging` module.
    *   The `argparse` module will be used for robust command-line argument parsing.

2.  **Set up GCP Environment (Manual Steps for the User):**
    *   **Create a GCP Project:** Ensure a GCP project is available.
    *   **Enable APIs:** Enable the "Compute Engine API" and "Cloud Storage API".
    *   **Create a GCS Bucket and Upload File:** A GCS bucket must exist, and the target text file should be uploaded to this bucket.
    *   **Spin up a GCP VM:**
        *   Create a new Google Compute Engine (GCE) VM instance (e.g., a small e2-micro or e2-small instance running a common Linux distribution like Debian or Ubuntu).
        *   Ensure the VM has the necessary scopes or a service account with permissions to read from GCS (at least "Storage Object Viewer" role).
    *   **Configure VM:**
        *   SSH into the VM.
        *   Install Python 3 and `pip` if not already present.
        *   Install the required Python library: `pip install google-cloud-storage argparse`.
        *   Transfer the developed Python script to the VM (e.g., using `gcloud compute scp`).

3.  **Execute and Generate Output:**
    *   Run the Python script on the VM, providing the bucket name and file path as arguments.
    *   Redirect the script's standard output to a text file (e.g., `output.txt`).
    *   Download the script and the output file from the VM.

## 3. Explaining and demonstrating the configuration of cloud compute setup

*This section describes the steps you would typically take. Actual demonstration requires access to your GCP console.*

**A. Creating a Virtual Machine (VM) in GCP:**

1.  **Navigate to Compute Engine:**
    *   Open the Google Cloud Console (`console.cloud.google.com`).
    *   In the navigation menu, go to "Compute Engine" > "VM instances".
2.  **Create Instance:**
    *   Click on "CREATE INSTANCE".
    *   **Name:** Give your VM a descriptive name (e.g., `gcs-line-counter-vm`).
    *   **Region and Zone:** Choose a region and zone (e.g., `us-central1` and `us-central1-a`).
    *   **Machine Configuration:**
        *   Series: E2 (cost-effective)
        *   Machine type: `e2-small` or `e2-medium` (sufficient for this task).
    *   **Boot Disk:**
        *   Click "Change".
        *   Operating System: Choose a Linux distribution (e.g., "Debian GNU/Linux 11 (bullseye)" or "Ubuntu 22.04 LTS").
        *   Version: Select a recent stable version.
        *   Size: The default (e.g., 10 GB) is usually fine.
        *   Click "Select".
    *   **Identity and API access:**
        *   **Service account:** You can use the default Compute Engine service account.
        *   **Access scopes:** Select "Allow default access". Critically, ensure that the access scopes include "Storage" with at least "Read Only" or "Read Write" permission. Alternatively, and more securely for fine-grained control, you would assign the "Storage Object Viewer" IAM role to the VM's service account on the specific GCS bucket or project.
            *   To set specific access scopes: Choose "Set access for each API" and ensure "Storage" is set to "Read Only" or "Read Write".
    *   **Firewall:**
        *   Allow HTTP/HTTPS traffic if you plan to run a web server (not needed for this assignment).
        *   Ensure SSH is allowed (usually default).
    *   Click "Create". Wait for the VM to be provisioned.

**B. SSH into the VM:**

1.  Once the VM is running (green checkmark next to its name in the VM instances list), click the "SSH" button in the "Connect" column.
2.  This will open an SSH terminal session in your browser or provide a `gcloud` command to connect from your local terminal (`gcloud compute ssh YOUR_VM_NAME --zone YOUR_VM_ZONE`).

**C. Installing necessary software on the VM:**

Once connected via SSH:

1.  **Update package lists:**
    ```bash
    sudo apt update
    ```
2.  **Install Python 3 and pip (if not present, usually they are):**
    ```bash
    sudo apt install python3 python3-pip -y
    ```
3.  **Install the Google Cloud Storage client library and argparse (if not using Python 3.2+ where it's standard):**
    ```bash
    pip3 install google-cloud-storage argparse
    ```
    *(Note: `argparse` is part of the standard library since Python 3.2, so explicitly installing it might not be needed for modern Python versions but doesn't hurt).*

**D. Authenticating to Google Cloud (if not using default VM service account with correct scopes):**

If the VM's service account doesn't have the necessary permissions or you want to use user credentials (less common for automated scripts on VMs):
```bash
gcloud auth application-default login
```
Follow the prompts to authenticate. For production/automated scenarios, it's best practice to ensure the VM's *service account* has the necessary IAM roles (e.g., "Storage Object Viewer" on the relevant bucket or project).

## 4. Explaining Input files/data

*   **Python Script (`count_gcs_lines.py`):** This is the code you develop/are provided with, which performs the line counting. It expects two command-line arguments: the GCS bucket name and the blob (file) name within that bucket.
*   **Target Text File (in GCS):** This is the actual file whose lines need to be counted.
    *   It must be a text-based file (e.g., `.txt`, `.csv`, `.log`, etc.). Binary files will not give a meaningful line count in the traditional sense.
    *   This file needs to be uploaded to a GCS bucket accessible by the VM.
    *   **Example:** Let's say you have a file named `sample_data.txt` with the following content:
        ```
        Hello world
        This is the second line.
        And a third.

        A fifth line after an empty one.
        ```
        This file would be uploaded to `gs://your-chosen-bucket/path/to/sample_data.txt`.

## 5. Explaining and demonstrating sequence of actions performed

*(This outlines the typical workflow you would execute. Actual demonstration requires an active GCP session.)*

1.  **Prepare Local Environment:**
    *   Have the `count_gcs_lines.py` script ready on your local machine.

2.  **GCP Setup (as described in section 3):**
    *   Ensure your GCP project is active.
    *   Create a GCS bucket (e.g., `my-assignment-bucket-unique-name`).
    *   Upload your target text file (e.g., `sample_data.txt`) to the bucket (e.g., `gs://my-assignment-bucket-unique-name/input_files/sample_data.txt`).
    *   Create and configure your GCE VM (e.g., `gcs-line-counter-vm`) with Python, pip, and the `google-cloud-storage` library installed. Ensure it has GCS read access.

3.  **Transfer Script to VM:**
    *   From your local machine's terminal, use `gcloud compute scp`:
        ```bash
        gcloud compute scp ./count_gcs_lines.py gcs-line-counter-vm:~ --zone=your-vm-zone
        ```
        (Replace `gcs-line-counter-vm` and `your-vm-zone` with your VM's name and zone. The `~` uploads it to the home directory of the user on the VM).

4.  **SSH into VM:**
    ```bash
    gcloud compute ssh gcs-line-counter-vm --zone=your-vm-zone
    ```
    Or use the SSH button in the Cloud Console.

5.  **Run the Script on the VM:**
    *   Navigate to where you copied the script (e.g., it should be in your home directory).
    *   Make the script executable (optional, but good practice):
        ```bash
        chmod +x count_gcs_lines.py
        ```
    *   Execute the script, providing your bucket name and the full path to your file in GCS. Redirect the output to `output.txt`:
        ```bash
        python3 ./count_gcs_lines.py my-assignment-bucket-unique-name input_files/sample_data.txt > output.txt
        ```
        *   If you want verbose logging to the terminal (stderr) while still capturing the count in `output.txt`:
            ```bash
            python3 ./count_gcs_lines.py -v my-assignment-bucket-unique-name input_files/sample_data.txt > output.txt
            ```

6.  **Verify Output on VM:**
    *   Check the contents of `output.txt`:
        ```bash
        cat output.txt
        ```
        This should show just the number of lines.
    *   If there were errors, they would have been printed to your terminal (stderr) by the logging mechanism.

7.  **Download Output File (and script, if needed for submission):**
    *   From your *local machine's terminal* (exit the VM SSH session if still in it):
        ```bash
        gcloud compute scp gcs-line-counter-vm:~/output.txt ./output.txt --zone=your-vm-zone
        # If you also need to download the version of the script from the VM (e.g., if modified there)
        # gcloud compute scp gcs-line-counter-vm:~/count_gcs_lines.py ./count_gcs_lines_from_vm.py --zone=your-vm-zone
        ```

8.  **Submit Deliverables:**
    *   `count_gcs_lines.py` (the Python script)
    *   `output.txt` (the file containing the line count)

## 6. Exhaustive explanation of scripts/code and its objective

The script `count_gcs_lines.py` is designed to count lines in a text file stored in Google Cloud Storage.

```python
from google.cloud import storage  # Client library to interact with GCS
import sys                       # For system-specific parameters and functions (exit, stderr, stdout)
import argparse                  # For parsing command-line arguments
import logging                   # For logging informational messages and errors

# Configure basic logging:
# - Level: INFO (and above: WARNING, ERROR, CRITICAL) will be shown. DEBUG is hidden by default.
# - Format: Specifies how log messages will look (e.g., "LEVEL: Message").
# - Stream: Directs log output to sys.stderr (standard error).
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s', stream=sys.stderr)

def count_lines_in_gcs_file(bucket_name: str, blob_name: str) -> int | None:
    """
    Counts the number of lines in a file stored in GCS.

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
        # Initialize a GCS client.
        # If GOOGLE_APPLICATION_CREDENTIALS env var is set, it uses that.
        # On a GCE VM, it automatically uses the VM's service account credentials.
        storage_client = storage.Client()

        # Get a reference to the GCS bucket.
        bucket = storage_client.bucket(bucket_name)

        # Get a reference to the blob (file) within the bucket.
        blob = bucket.blob(blob_name)

        logging.info(f"Attempting to download file gs://{bucket_name}/{blob_name}...")
        
        # Check if the blob actually exists in GCS before trying to download.
        if not blob.exists():
            logging.error(f"File not found: gs://{bucket_name}/{blob_name}")
            return None

        # Download the file's content as a string.
        # Note: This loads the entire file into memory. For extremely large files,
        # streaming (blob.open('r')) would be more memory-efficient.
        content = blob.download_as_text()
        
        # Split the content by newline characters to get a list of lines.
        lines = content.splitlines()
        line_count = len(lines) # The number of items in the list is the line count.
        
        logging.info(f"Successfully downloaded and counted lines for gs://{bucket_name}/{blob_name}.")
        return line_count

    # Specific exception for "file not found" errors from GCS.
    except storage.exceptions.NotFound:
        logging.error(f"File not found (NotFound exception): gs://{bucket_name}/{blob_name}")
        return None
    # Specific exception for permission errors from GCS.
    except storage.exceptions.Forbidden as e:
        logging.error(f"Permission denied for gs://{bucket_name}/{blob_name}. Details: {e}")
        return None
    # Catch any other unexpected exceptions during the process.
    except Exception as e:
        logging.error(f"An unexpected error occurred while processing gs://{bucket_name}/{blob_name}: {e}")
        return None

# This block executes only when the script is run directly (not imported as a module).
if __name__ == "__main__":
    # Set up an argument parser to handle command-line inputs.
    parser = argparse.ArgumentParser(
        description="Count lines in a file stored in Google Cloud Storage."
    )
    # Define the 'bucket_name' argument (required).
    parser.add_argument(
        "bucket_name", 
        help="The name of the GCS bucket (e.g., 'my-bucket')."
    )
    # Define the 'blob_name' argument (required).
    parser.add_argument(
        "blob_name", 
        help="The name/path of the file in the GCS bucket (e.g., 'data/my_file.txt')."
    )
    # Define an optional '--verbose' or '-v' flag.
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true", # If flag is present, set to True.
        help="Enable verbose logging (DEBUG level)."
    )

    # Parse the arguments provided when the script was run.
    args = parser.parse_args()

    # If the verbose flag was used, set the logging level to DEBUG.
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG) # Get the root logger.
        logging.debug("Verbose logging enabled.")

    # Call the main function with the parsed bucket and blob names.
    line_count = count_lines_in_gcs_file(args.bucket_name, args.blob_name)

    if line_count is not None:
        # If successful, print ONLY the line count to standard output (stdout).
        # This allows easy redirection to a file (e.g., `> output.txt`).
        print(line_count)
    else:
        # If an error occurred (line_count is None), exit with a non-zero status code.
        # Error messages would have already been printed to stderr by the logging setup.
        sys.exit(1)
```

**Objective of the script:**
To provide a reusable and robust command-line tool that can accurately count the lines in any specified text file within a Google Cloud Storage bucket, handling common errors gracefully and integrating well with shell pipelines by sending primary output to `stdout` and diagnostic messages to `stderr`.

## 7. Errors that you have encountered in the process and how did you overcome them (if any)

*(This section is hypothetical, as I am an AI and don't "encounter" errors in a live environment. However, these are common issues a user might face.)*

1.  **Error: `google.auth.exceptions.DefaultCredentialsError: Could not automatically determine credentials.`**
    *   **Cause:** The Python script cannot find Google Cloud credentials. This happens if `gcloud auth application-default login` hasn't been run, or if the VM's service account doesn't have permissions, or `GOOGLE_APPLICATION_CREDENTIALS` environment variable is not set correctly for a service account key.
    *   **Overcome:**
        *   On a local machine: Run `gcloud auth application-default login`.
        *   On a GCE VM: Ensure the VM was created with appropriate API access scopes (Storage: Read Only or higher) OR that the VM's service account has the "Storage Object Viewer" (or higher) IAM role for the target bucket/project.
        *   For service account keys (less common for simple VM scripts): Ensure `GOOGLE_APPLICATION_CREDENTIALS` env variable points to the valid JSON key file.

2.  **Error: `google.api_core.exceptions.Forbidden: 403 GET ... : caller does not have storage.objects.get access to the Google Cloud Storage object.`**
    *   **Cause:** The authenticated user or service account does not have permission to read the specified GCS object.
    *   **Overcome:**
        *   Check IAM permissions in the Google Cloud Console.
        *   Ensure the service account used by the VM (or the user credentials) has at least the "Storage Object Viewer" role on the bucket or the specific object.

3.  **Error: `FileNotFoundError: [Errno 2] No such file or directory` (when running the script itself)**
    *   **Cause:** The Python script `count_gcs_lines.py` is not in the current directory or the path specified.
    *   **Overcome:** Ensure you are in the directory where the script was saved, or provide the correct path to it (e.g., `python3 ./count_gcs_lines.py ...` or `python3 /path/to/script/count_gcs_lines.py ...`).

4.  **Error: Script outputting `None` or error messages instead of a count.**
    *   **Cause:** The `blob_name` (file path in GCS) or `bucket_name` provided to the script is incorrect, or the file doesn't exist at that GCS location.
    *   **Overcome:**
        *   Double-check the bucket name and the full path to the file in GCS. Use `gsutil ls gs://your-bucket-name/path/to/your/` to verify.
        *   Ensure there are no typos. GCS paths are case-sensitive.

5.  **Error: `pip: command not found` or `python3: command not found` on VM.**
    *   **Cause:** Python 3 or pip is not installed, or not in the system's PATH.
    *   **Overcome:** Install them using the VM's package manager (e.g., `sudo apt install python3 python3-pip -y` on Debian/Ubuntu).

6.  **MemoryError for very large files:**
    *   **Cause:** `blob.download_as_text()` reads the entire file into memory. If the file is many gigabytes, this can exceed available RAM.
    *   **Overcome (as noted in script comments):** For extremely large files, modify the script to stream the content:
        ```python
        # Instead of: content = blob.download_as_text()
        # Use:
        line_count = 0
        with blob.open("r") as f: # Opens the blob as a text stream
            for _ in f: # Iterate line by line
                line_count += 1
        # lines = content.splitlines() # No longer needed
        # line_count = len(lines) # No longer needed
        ```
        This reads the file line by line without loading it all into memory at once.

## 8. Working demonstration of your pipeline in GCP Environment

*(This describes what a successful demonstration would look like.)*

**Scenario:**
*   **GCS Bucket:** `my-assignment-bucket-2023`
*   **File in GCS:** `input_data/dataset.txt`
*   **Content of `dataset.txt`:**
    ```
    Line one
    Line two
    Line three
    Line four
    ```
*   **VM Name:** `line-counter-vm`

**Steps & Expected Output in Terminal:**

1.  **(Local Terminal) SCP script to VM:**
    ```bash
    gcloud compute scp ./count_gcs_lines.py line-counter-vm:~ --zone=us-central1-a
    ```
    *Expected Output:* Progress bar and confirmation of transfer.

2.  **(Local Terminal) SSH into VM:**
    ```bash
    gcloud compute ssh line-counter-vm --zone=us-central1-a
    ```
    *Expected Output:* SSH connection established, VM terminal prompt appears.

3.  **(VM Terminal) Run the script:**
    ```bash
    python3 ./count_gcs_lines.py my-assignment-bucket-2023 input_data/dataset.txt > output.txt
    ```
    *Expected Terminal Output (stderr, from logging):*
    ```
    INFO: Attempting to download file gs://my-assignment-bucket-2023/input_data/dataset.txt...
    INFO: Successfully downloaded and counted lines for gs://my-assignment-bucket-2023/input_data/dataset.txt.
    ```
    *(No output to stdout directly visible in terminal because it's redirected to `output.txt`)*

4.  **(VM Terminal) Check the content of `output.txt`:**
    ```bash
    cat output.txt
    ```
    *Expected Terminal Output (stdout):*
    ```
    4
    ```

5.  **(VM Terminal) Exit SSH:**
    ```bash
    exit
    ```

6.  **(Local Terminal) SCP output file from VM:**
    ```bash
    gcloud compute scp line-counter-vm:~/output.txt ./final_output.txt --zone=us-central1-a
    ```
    *Expected Output:* Progress bar and confirmation of transfer.

7.  **(Local Terminal) Check local output file:**
    ```bash
    cat ./final_output.txt
    ```
    *Expected Output:*
    ```
    4
    ```
This sequence demonstrates the end-to-end pipeline: script deployment, execution on VM against GCS data, and retrieval of results.

## 9. Explaining output files/data

*   **Primary Output File (`output.txt` or similar name chosen by user):**
    *   This file is generated by redirecting the standard output (`stdout`) of the `count_gcs_lines.py` script.
    *   **Content:** It contains a single number, which is the total count of lines in the processed GCS file. For example, if the GCS file had 150 lines, `output.txt` would contain:
        ```
        150
        ```
    *   **Purpose:** This is one of the main deliverables of the assignment, providing the result of the line counting operation.

*   **Log Messages (to `stderr` / Console):**
    *   While not an "output file" in the same sense, the script also produces log messages to the standard error stream (`stderr`). These are visible in the console when the script runs (unless `stderr` is also redirected).
    *   **Content:** Includes informational messages (e.g., "Attempting to download...", "Successfully downloaded...") and any error messages if problems occur (e.g., "File not found...", "Permission denied...").
    *   **Purpose:** Useful for debugging, understanding the script's progress, and diagnosing issues. If the `-v` or `--verbose` flag is used, more detailed DEBUG level logs are also shown.

## 10. Learnings from this Assignment

1.  **GCP Fundamentals:**
    *   Understanding the basics of Google Compute Engine (VM creation, SSH, software installation).
    *   Understanding Google Cloud Storage (buckets, objects/blobs, paths like `gs://`).
    *   Importance of IAM roles and service accounts for secure access between GCP services (VM accessing GCS).

2.  **Python for Cloud Interaction:**
    *   Using the `google-cloud-storage` client library to programmatically access and manipulate data in GCS.
    *   Instantiating clients (`storage.Client()`), referencing buckets (`client.bucket()`), and blobs (`bucket.blob()`).
    *   Methods for accessing blob data (`blob.download_as_text()`, `blob.exists()`, and awareness of streaming alternatives like `blob.open()`).

3.  **Robust Scripting Practices:**
    *   **Command-Line Arguments:** Using `argparse` to create flexible and user-friendly command-line interfaces, rather than hardcoding values.
    *   **Logging:** Implementing proper logging with the `logging` module to distinguish between primary output (`stdout`) and diagnostic/error messages (`stderr`). This is crucial for script automation and debugging.
    *   **Error Handling:** Using `try-except` blocks to gracefully handle potential issues like file not found, permission errors, or other GCS API exceptions.
    *   **Modularity:** Writing functions (like `count_lines_in_gcs_file`) to encapsulate logic, making the code cleaner and more reusable.
    *   **Exit Codes:** Using `sys.exit(1)` to indicate script failure, which is a standard practice for shell scripting and automation.

4.  **File Operations:**
    *   Understanding how to count lines in a string (e.g., `content.splitlines()`).
    *   Awareness of memory implications when handling large files (`download_as_text()` vs. streaming).

5.  **DevOps/Workflow:**
    *   The process of deploying code to a remote server (VM).
    *   Executing scripts remotely.
    *   Retrieving results from a remote server.
    *   The importance of clear instructions for setting up the environment and running the solution.

6.  **Problem Decomposition:** Breaking down the overall problem (count lines in GCS file via VM) into smaller, manageable tasks (script development, VM setup, execution, output generation). 