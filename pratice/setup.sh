#!/bin/bash
echo "🔧 Setting up pratice folder for Google Cloud development..."

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment and install dependencies
echo "Installing Google Cloud Storage library..."
source venv/bin/activate
pip install google-cloud-storage

echo "✅ Setup complete!"
echo ""
echo "To use:"
echo "  1. cd pratice"
echo "  2. source venv/bin/activate"
echo "  3. python3 week-1.py"
echo ""
echo "💡 Make sure you're authenticated with: gcloud auth application-default login" 