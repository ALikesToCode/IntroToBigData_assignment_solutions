# Practice Folder - Google Cloud Storage Examples

This folder contains practice scripts for working with Google Cloud Storage.

## ✅ Fixed Issues

The `week-1.py` script was failing due to two issues:

1. **Missing Library**: The `google-cloud-storage` Python package wasn't installed
2. **Authentication**: Application Default Credentials weren't set up properly

## 🛠️ Quick Setup

Run the setup script:
```bash
./setup.sh
```

Or manually:
```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install required library
pip install google-cloud-storage

# Set up authentication (one-time setup)
gcloud auth application-default login
```

## 📝 Files

- `week-1.py` - Downloads and displays content from GCS bucket
- `setup.sh` - Automated setup script
- `venv/` - Python virtual environment (created after setup)

## 🚀 Usage

```bash
cd pratice
source venv/bin/activate  # Activate virtual environment
python3 week-1.py         # Run the script
```

## 📊 What week-1.py Does

The script:
1. Connects to Google Cloud Storage
2. Accesses the `intro-to-big-data-week-1` bucket
3. Downloads the `Week-2-test-file.txt` file
4. Prints its contents (24 lines of sample text about Cloud Functions)

## 🐛 Common Issues

- **ModuleNotFoundError**: Install google-cloud-storage in virtual environment
- **Authentication Error**: Run `gcloud auth application-default login`
- **Permission Denied**: Check your Google Cloud project access and billing 