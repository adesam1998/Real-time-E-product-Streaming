import pandas as pd
from kafka import KafkaProducer
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import boto3
import redshift_connector
import configparser
import logging
from bs4 import BeautifulSoup
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize and set up the Chrome webdriver
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless=new')  # Use headless mode if you don't want to see the browser
chrome = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Open the preferred website and in this case is search result from www.trustpilot.com
base_url = 'https://www.trustpilot.com/review/www.aliexpress.com'

# Extract the HTML of all 'quote' elements, after parse them with BS4 and save as CSV
users = []
userReviewNum = []
ratings = []
locations = []
dates = []
reviews = []

def data_extract():
    for page_number in range(1, 1500):
        try:
            url = f'{base_url}?page={page_number}'
            chrome.get(url)
            
            # Wait for the page to load
            wait = WebDriverWait(chrome, 20)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.styles_cardWrapper__LcCPA')))
            
            r = chrome.page_source
            soup = BeautifulSoup(r, 'html.parser')
            aliexpress_quote_electronics = soup.find_all('div', class_='styles_cardWrapper__LcCPA styles_show__HUXRb styles_reviewCard__9HxJJ')

            for aliexpress_quote in aliexpress_quote_electronics:
                quote_user = aliexpress_quote.find('span', class_='typography_heading-xxs__QKBS8 typography_appearance-default__AAY17')
                quote_location = aliexpress_quote.find('div', class_='typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_detailsIcon__Fo_ua')
                quote_userReviewNum = aliexpress_quote.find('span', class_='typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l')
                quote_date = aliexpress_quote.find('p', class_='typography_body-m__xgxZ_ typography_appearance-default__AAY17')
                quote_rating = aliexpress_quote.find('div', class_='styles_reviewHeader__iU9Px')
                quote_review = aliexpress_quote.find('p', class_='typography_body-l__KUYFJ typography_appearance-default__AAY17 typography_color-black__5LYEn')

                users.append(quote_user.get_text(strip=True) if quote_user else '')
                locations.append(quote_location.get_text(strip=True) if quote_location else '')
                userReviewNum.append(quote_userReviewNum.get_text(strip=True) if quote_userReviewNum else '')
                dates.append(quote_date.get_text(strip=True) if quote_date else '')
                ratings.append(quote_rating['data-service-review-rating'] if quote_rating else '')
                reviews.append(quote_review.get_text(strip=True) if quote_review else '')

            logging.info(f"Page {page_number} processed successfully.")
            
            # Add a delay to prevent overwhelming the server
            time.sleep(2)
            
        except Exception as e:
            logging.error(f"Error processing page {page_number}: {e}")
            continue  # Skip to the next page if there's an error

    # Create a dataframe for the extracted electronics elements
    aliexpress_electronics_review = pd.DataFrame({
        'Username': users,
        'Location': locations,
        'Total Review': userReviewNum,
        'Date of Experience': dates,
        'Content': reviews,
        'Rating': ratings
    })

    # Close the Chrome browser
    chrome.quit()
    logging.info("Chrome browser closed.")
    
    return aliexpress_electronics_review

def data_transform(aliexpress_electronics_review):
    # Data Transformation
    aliexpress_electronics_review['Date of Experience'] = aliexpress_electronics_review['Date of Experience'].str.replace('Date of experience', '') \
        .str.replace(':', '')

    # Save the extracted data in CSV format
    aliexpress_electronics_review.to_csv('aliexpress_electronics_review.csv', index=False)
    logging.info("Data saved to aliexpress_electronics_review.csv successfully.")
    
    return aliexpress_electronics_review

def kafka_producer(aliexpress_electronics_review):
    # Initialize the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: v.encode('utf-8'),
        max_block_ms=5000
    )

    # Iterate over each row in the DataFrame
    for _, row in aliexpress_electronics_review.iterrows():
        try:
            # Convert the row to a comma-separated string
            row_data = ','.join(row.astype(str))
            # Send the row data to the Kafka topic
            producer.send('Review_Ratings_Data', value=row_data)
        except Exception as e:
            logging.error(f"Error sending data to Kafka: {e}")
            continue  # Skip the problematic row

    # Close the Kafka producer
    producer.close()
    logging.info("Kafka producer closed.")

if __name__ == "__main__":
    aliexpress_electronics_review = data_extract()
    aliexpress_electronics_review = data_transform(aliexpress_electronics_review)
    kafka_producer(aliexpress_electronics_review)
    

                                #2 CONFIGURATION PARSER TO ACCESS SOME OF THE FOLDERS

# Reading the configuration from the .env file
config = configparser.ConfigParser()                # To manage the keys that want to be keep out of public
config.read('.env')                                 # To read the private keys 

access_key=config['AWS']['access_key']                   # Acessing the access_key 
secret_key=config['AWS']['secret_key']                   # Accessing the secret_key 
bucket_name=config['AWS']['bucket_name']                 # Accessing the S3 bucket name
region=config['AWS']['region']                           # Accessing the nearest region to the data

                        #3 CREATING THE S3 CLOUD BUCKET REFERING TO AS DATA LAKE
def create_bucket():
    client = boto3.client(                               # Initialize and connect to AWS S3 bucket through boto3
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    client.create_bucket(                                # Creating AWS S3 bucket      
        Bucket=bucket_name,
        CreateBucketConfiguration={
           'LocationConstraint': region,
        },
    )
create_bucket()
print(f'{bucket_name} is now available on your AWS account')

                                # LOADING DATA TO THE AWS S3 BUCKET
                                
# Upload the raw data from the extraction
def upload_to_s3_bucket(dataframes, bucket_name):       # Upload Aliexpress review dataframe to AWS S3 bucket
    upload_s3_client = boto3.client(                    # Initialize and connect to AWS S3 bucket through boto3
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    for name, df in dataframes.items():
        try:
            csv_buffer = df.to_csv(index=False)     
            # Uploading the CSV files to S3 bucket         
            upload_s3_client.put_object(Body=csv_buffer, Bucket=bucket_name, Key=f'aliexpress_data/{name}.csv')
            print(f'File {name} is uploaded successfully to {bucket_name}')
        except Exception as e:
            print(f'Error in uploading aliexpress_data/{name}.csv to {bucket_name}: {str(e)}')
            
# The main function
if __name__ == '__main__':
    
    # Reading CSV files to DataFrame
    dataframes ={
        'aliexpress_data/aliexpress_electronics_review': aliexpress_electronics_review

    }

# Upload the DataFrames to the S3 bucket
upload_to_s3_bucket(dataframes, bucket_name)

                            # CREATE DATABASE TO ACCOMMODATE THE DATA FROM S3 BUCKET

# Connect to Redshift cluster using AWS credentials
conn = redshift_connector.connect(
    host=config['NUNU_REDSHIFT']['host'],
    database=config['NUNU_REDSHIFT']['database'],
    user=config['NUNU_REDSHIFT']['username'],
    password=config['NUNU_REDSHIFT']['password']
    )

conn.autocommit = True
cursor = redshift_connector.Cursor = conn.cursor()

cursor.execute("""
CREATE TABLE "AliExpress_Review" (
  'Username' VARCHAR,
  'Location' VARCHAR,
  'Total_Review' VARCHAR,
  'Date of Experience' DATETIME,
  'Content' VARCHAR,
  'Rating' INTEGER
)
""")

# Copy data from the csv file in team-bravo-bucket S3 bucket to our team-bravo-DB redshift tables
cursor.execute("""
copy df_data_energy_dim from 's3://team-bravo-review-bucket/aliexpress_data/aliexpress_data/aliexpress_electronics_review.csv' 
credentials 'aws_iam_role=arn:aws:iam::975049991061:role/service-role/AmazonRedshift-CommandsAccessRole-20240109T101728'
delimiter ','
region 'eu-north-1'
IGNOREHEADER 1
""")




###################################################


# Create database to accommodate the data from S3 bucket

# Connect to Redshift cluster using AWS credentials
conn = redshift_connector.connect(
    host=config['NUNU_REDSHIFT']['host'],
    database=config['NUNU_REDSHIFT']['database'],
    user=config['NUNU_REDSHIFT']['username'],
    password=config['NUNU_REDSHIFT']['password']
)

conn.autocommit = True
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS "AliExpress_Review" (
    "Username" VARCHAR,
    "Location" VARCHAR,
    "Total_Review" VARCHAR,
    "Date_of_Experience" VARCHAR,
    "Content" VARCHAR,
    "Rating" INTEGER
)
""")

# Copy data from the CSV file in team-bravo-bucket S3 bucket to our team-bravo-DB Redshift tables
cursor.execute(f"""
COPY AliExpress_Review
FROM 's3://{bucket_name}/aliexpress_data/aliexpress_electronics_review.csv'
IAM_ROLE 'arn:aws:iam::975049991061:role/service-role/AmazonRedshift-CommandsAccessRole-20240109T101728'
DELIMITER ','
REGION '{region}'
IGNOREHEADER 1
""")

# Close Redshift connection
conn.close()
logging.info("Redshift connection closed.")



COPY dev.public.aliexpress_electronics_review 
FROM 's3://team-bravo-intern-bucket/aliexpress_data/aliexpress_electronics_review.csv' 
IAM_ROLE 'arn:aws:iam::839519159203:role/service-role/AmazonRedshift-CommandsAccessRole-20240614T021335' 
FORMAT AS CSV DELIMITER ',' 
QUOTE '"' 
IGNOREHEADER 1 
REGION AS 'eu-north-1'