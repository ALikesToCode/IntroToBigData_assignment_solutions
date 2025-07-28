#!/usr/bin/env python3
"""
Pipeline Verification Script
Shows how to verify the real-time file processing pipeline is working
"""

import subprocess
import time
import json
from google.cloud import pubsub_v1

def verify_pipeline():
    print("🔍 REAL-TIME PIPELINE VERIFICATION GUIDE")
    print("=" * 60)
    
    print("\n📋 STEP 1: Upload a test file")
    print("Run this command to upload a file:")
    print("echo 'Line 1: Test verification' > test_verify.txt")
    print("gcloud storage cp test_verify.txt gs://steady-triumph-447006-f8-week6-linecount-bucket/")
    
    print("\n📋 STEP 2: Check Cloud Function logs")
    print("gcloud functions logs read process-file-upload --gen2 --region=us-central1 --limit=10")
    
    print("\n📋 STEP 3: Check Pub/Sub messages")
    print("python3 show_line_count.py")
    
    print("\n📋 STEP 4: Manual verification commands")
    print("# List files in bucket:")
    print("gcloud storage ls gs://steady-triumph-447006-f8-week6-linecount-bucket/")
    print("\n# Check Pub/Sub topic:")
    print("gcloud pubsub topics list --filter='name:file-processing-topic'")
    print("\n# Check subscription:")
    print("gcloud pubsub subscriptions list --filter='name:file-processing-subscription'")
    
    print("\n🎯 WHAT YOU SHOULD SEE:")
    print("✅ File appears in bucket listing")
    print("✅ Cloud Function logs show 'CLOUD FUNCTION TRIGGERED'")
    print("✅ Pub/Sub message contains file details")
    print("✅ Line count displayed from real file")
    
    print("\n" + "=" * 60)
    
    # Auto-run verification
    print("\n🚀 RUNNING AUTO-VERIFICATION...")
    
    try:
        # Check if bucket exists
        result = subprocess.run(['gcloud', 'storage', 'ls', 'gs://steady-triumph-447006-f8-week6-linecount-bucket/'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            files = result.stdout.strip().split('\n')
            print(f"✅ Bucket exists with {len(files)} files:")
            for file in files:
                if file.strip():
                    print(f"   📄 {file.split('/')[-1]}")
        
        # Check Pub/Sub topic
        result = subprocess.run(['gcloud', 'pubsub', 'topics', 'list', '--filter=name:file-processing-topic'], 
                              capture_output=True, text=True)
        if 'file-processing-topic' in result.stdout:
            print("✅ Pub/Sub topic exists")
        
        # Check subscription
        result = subprocess.run(['gcloud', 'pubsub', 'subscriptions', 'list', '--filter=name:file-processing-subscription'], 
                              capture_output=True, text=True)
        if 'file-processing-subscription' in result.stdout:
            print("✅ Pub/Sub subscription exists")
            
        # Check for messages
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path('steady-triumph-447006-f8', 'file-processing-subscription')
        
        response = subscriber.pull(
            request={'subscription': subscription_path, 'max_messages': 1, 'allow_ack_renegotiation': False}
        )
        
        if response.received_messages:
            print("✅ Messages found in subscription")
            message = response.received_messages[0]
            data = json.loads(message.message.data.decode('utf-8'))
            print(f"   📄 Latest file: {data.get('file_name', 'unknown')}")
            print(f"   🔔 Event: {data.get('event_type', 'unknown')}")
            # Don't acknowledge - leave for show_line_count.py
        else:
            print("⚠️  No messages in queue (upload a file to test)")
            
    except Exception as e:
        print(f"❌ Verification error: {e}")
    
    print("\n🎉 PIPELINE VERIFICATION COMPLETE!")

if __name__ == "__main__":
    verify_pipeline()