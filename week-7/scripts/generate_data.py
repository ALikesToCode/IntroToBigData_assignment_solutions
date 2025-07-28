#!/usr/bin/env python3
"""
Data Generator for Week 7 Kafka + Spark Streaming Assignment
Course: Introduction to Big Data - Week 7

This script generates a CSV file with 1000+ rows of sample customer transaction data
for use in the Kafka Producer-Consumer streaming pipeline.

Author: Abhyudaya B Tharakan 22f3001492
Date: July 2025
"""

import csv
import random
import datetime
from pathlib import Path

def generate_customer_data(num_records=1200):
    """
    Generate sample customer transaction data.
    
    Args:
        num_records: Number of records to generate (default: 1200)
        
    Returns:
        List of dictionaries containing customer transaction data
    """
    
    # Sample data for realistic generation
    first_names = [
        'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
        'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
        'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher', 'Nancy', 'Daniel', 'Lisa',
        'Matthew', 'Betty', 'Anthony', 'Helen', 'Mark', 'Sandra', 'Donald', 'Donna',
        'Steven', 'Carol', 'Paul', 'Ruth', 'Andrew', 'Sharon', 'Joshua', 'Michelle',
        'Kenneth', 'Laura', 'Kevin', 'Sarah', 'Brian', 'Kimberly', 'George', 'Deborah',
        'Edward', 'Dorothy', 'Ronald', 'Lisa', 'Timothy', 'Nancy', 'Jason', 'Karen'
    ]
    
    last_names = [
        'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
        'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas',
        'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White',
        'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker', 'Young',
        'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
        'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell',
        'Carter', 'Roberts', 'Gomez', 'Phillips', 'Evans', 'Turner', 'Diaz', 'Parker'
    ]
    
    cities = [
        'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
        'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville',
        'Fort Worth', 'Columbus', 'Charlotte', 'San Francisco', 'Indianapolis', 'Seattle',
        'Denver', 'Washington', 'Boston', 'El Paso', 'Nashville', 'Detroit', 'Oklahoma City',
        'Portland', 'Las Vegas', 'Memphis', 'Louisville', 'Baltimore', 'Milwaukee', 'Albuquerque'
    ]
    
    states = [
        'NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA', 'TX', 'FL',
        'TX', 'OH', 'NC', 'CA', 'IN', 'WA', 'CO', 'DC', 'MA', 'TX', 'TN', 'MI',
        'OK', 'OR', 'NV', 'TN', 'KY', 'MD', 'WI', 'NM'
    ]
    
    product_categories = [
        'Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports & Outdoors',
        'Health & Beauty', 'Toys & Games', 'Automotive', 'Food & Beverages',
        'Office Supplies', 'Pet Supplies', 'Jewelry', 'Movies & Music', 'Software'
    ]
    
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Bank Transfer', 'Gift Card']
    
    # Generate data
    data = []
    base_date = datetime.datetime(2025, 1, 1)
    
    for i in range(num_records):
        # Generate customer info
        customer_id = f"CUST_{10001 + i}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        # Generate address
        city_idx = random.randint(0, len(cities) - 1)
        city = cities[city_idx]
        state = states[city_idx]
        zip_code = random.randint(10000, 99999)
        
        # Generate transaction details
        transaction_id = f"TXN_{random.randint(100000, 999999)}"
        transaction_date = base_date + datetime.timedelta(
            days=random.randint(0, 200),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        product_category = random.choice(product_categories)
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(5.99, 499.99), 2)
        total_amount = round(quantity * unit_price, 2)
        discount = round(random.uniform(0, total_amount * 0.2), 2) if random.random() < 0.3 else 0.0
        final_amount = round(total_amount - discount, 2)
        
        payment_method = random.choice(payment_methods)
        
        # Customer demographics
        age = random.randint(18, 80)
        gender = random.choice(['M', 'F', 'Other'])
        email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])}"
        phone = f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        
        # Additional fields for analytics
        is_premium_customer = random.choice([True, False])
        customer_since = base_date - datetime.timedelta(days=random.randint(30, 1800))
        loyalty_points = random.randint(0, 10000) if is_premium_customer else random.randint(0, 1000)
        
        record = {
            'customer_id': customer_id,
            'transaction_id': transaction_id,
            'transaction_date': transaction_date.strftime('%Y-%m-%d %H:%M:%S'),
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'phone': phone,
            'age': age,
            'gender': gender,
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'product_category': product_category,
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'discount': discount,
            'final_amount': final_amount,
            'payment_method': payment_method,
            'is_premium_customer': is_premium_customer,
            'customer_since': customer_since.strftime('%Y-%m-%d'),
            'loyalty_points': loyalty_points,
            'record_id': i + 1
        }
        
        data.append(record)
    
    return data

def save_to_csv(data, filename):
    """
    Save generated data to CSV file.
    
    Args:
        data: List of dictionaries containing the data
        filename: Output CSV filename
    """
    if not data:
        raise ValueError("No data to save")
    
    # Get field names from first record
    fieldnames = list(data[0].keys())
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"✅ Generated {len(data)} records and saved to {filename}")
    print(f"📊 File size: {Path(filename).stat().st_size:,} bytes")

def main():
    """
    Main function to generate customer transaction data.
    """
    print("🚀 Generating customer transaction data for Week 7...")
    print("📝 Creating 1200 records of realistic transaction data...")
    
    # Generate data
    data = generate_customer_data(num_records=1200)
    
    # Save to CSV
    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / "customer_transactions_1200.csv"
    save_to_csv(data, output_file)
    
    # Display sample records
    print(f"\n📋 Sample records (first 3):")
    for i, record in enumerate(data[:3]):
        print(f"\nRecord {i + 1}:")
        for key, value in record.items():
            print(f"  {key}: {value}")
    
    print(f"\n✅ Data generation completed!")
    print(f"📁 Output file: {output_file}")
    print(f"📊 Total records: {len(data)}")
    print(f"🔢 Fields per record: {len(data[0])}")
    
    # Generate summary statistics
    total_amount_sum = sum(float(record['final_amount']) for record in data)
    avg_transaction = total_amount_sum / len(data)
    
    print(f"\n📈 Data Summary:")
    print(f"   Total transaction value: ${total_amount_sum:,.2f}")
    print(f"   Average transaction: ${avg_transaction:.2f}")
    print(f"   Date range: {min(record['transaction_date'] for record in data)} to {max(record['transaction_date'] for record in data)}")
    
    premium_customers = sum(1 for record in data if record['is_premium_customer'])
    print(f"   Premium customers: {premium_customers} ({premium_customers/len(data)*100:.1f}%)")

if __name__ == '__main__':
    main()