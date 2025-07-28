#!/bin/bash
# Quick Pipeline Test Script

echo "🧪 QUICK PIPELINE TEST"
echo "====================="

echo "1️⃣ Creating test file..."
echo "Line 1: Quick test $(date)" > quick_test.txt
echo "Line 2: Pipeline verification" >> quick_test.txt
echo "Line 3: Real-time processing" >> quick_test.txt

echo "2️⃣ Uploading to trigger pipeline..."
gcloud storage cp quick_test.txt gs://steady-triumph-447006-f8-week6-linecount-bucket/

echo "3️⃣ Waiting 5 seconds for processing..."
sleep 5

echo "4️⃣ Checking for pipeline message..."
source venv/bin/activate && python3 show_line_count.py

echo "5️⃣ Cleaning up..."
rm -f quick_test.txt

echo "✅ Test complete!"