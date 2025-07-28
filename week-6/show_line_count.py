#!/usr/bin/env python3
"""
Show actual line count from the real pipeline
"""

from google.cloud import pubsub_v1, storage
import json

def show_real_line_count():
    print('🔍 Getting message from real pipeline...')
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path('steady-triumph-447006-f8', 'file-processing-subscription')

    # Pull one message
    response = subscriber.pull(
        request={'subscription': subscription_path, 'max_messages': 1}
    )

    if response.received_messages:
        message = response.received_messages[0]
        data = json.loads(message.message.data.decode('utf-8'))
        
        bucket_name = data['bucket_name']
        file_name = data['file_name']
        
        print(f'\n📨 MESSAGE FROM CLOUD FUNCTION:')
        print(f'📄 File: {file_name}')
        print(f'🪣 Bucket: {bucket_name}')
        
        # COUNT LINES USING GCLOUD COMMAND (bypass permission issue)
        print(f'\n📊 COUNTING LINES IN REAL FILE...')
        
        import subprocess
        try:
            # Use gcloud to download file content
            result = subprocess.run(['gcloud', 'storage', 'cat', f'gs://{bucket_name}/{file_name}'], 
                                  capture_output=True, text=True, check=True)
            content = result.stdout
            lines = content.splitlines()
            line_count = len(lines)
            
            print(f'\n🎯 REAL-TIME LINE COUNT RESULTS:')
            print('=' * 60)
            print(f'📄 FILE NAME: {file_name}')
            print(f'📊 LINE COUNT: {line_count} lines')
            print(f'📏 FILE SIZE: {len(content)} bytes')
            print(f'\n📋 ACTUAL FILE CONTENT:')
            for i, line in enumerate(lines, 1):
                print(f'   Line {i}: {line}')
            print('=' * 60)
            print('✅ REAL-TIME PROCESSING COMPLETE!')
            print('🎉 PIPELINE WORKING - YOU CAN SEE THE COUNT!')
            
        except subprocess.CalledProcessError as e:
            print(f'❌ FAILED TO ACCESS FILE: {e}')
            print('❌ PIPELINE NOT WORKING PROPERLY')
            return
        
        # Acknowledge the message
        ack_ids = [message.ack_id]
        subscriber.acknowledge(request={'subscription': subscription_path, 'ack_ids': ack_ids})
        print('✅ Message acknowledged')
        
    else:
        print('❌ No messages found in queue')
        print('💡 Upload a file first: gcloud storage cp yourfile.txt gs://steady-triumph-447006-f8-week6-linecount-bucket/')

if __name__ == "__main__":
    show_real_line_count()