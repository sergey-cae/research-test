# ITEMS CME_NymexFutures
import os
import shutil
import requests
from requests.auth import HTTPBasicAuth
import zipfile

# Function to clean up the download directory
def clean_download_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory)

# Function to unzip files recursively
def unzip_file(zip_file_path, extract_to):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
        for file in zip_ref.namelist():
            if file.endswith('.zip'):
                unzip_file(os.path.join(extract_to, file), extract_to)

def download_file(feedName, data_type='src', idx=1, limit='50000'):
    base_url = f"https://mp.morningstarcommodity.com/lds/feeds/{feedName}/items"

    params = {
        'type': data_type,  # all, src, cf, and delta
        'limit': limit
    }

    r = requests.get(base_url, params=params, auth=HTTPBasicAuth(uname, pword))
    items = r.json()

    counter = 0

    for i in range(1, idx + 1):
        url = items[-i]['uri']


        # Define the download directory
        store_time = items[-i]['storeTime']
        store_time = store_time.replace(':', '')

        if counter % 100 == 0:
            print(counter, feedName, data_type, store_time, len(items))

        download_directory = os.path.join('downloads', feedName, data_type, store_time)
        clean_download_directory(download_directory)

        response = requests.get(url, auth=HTTPBasicAuth(uname, pword), stream=True)

        if response.status_code == 200:
            zip_file_path = os.path.join(download_directory, 'downloaded_file.zip')

            with open(zip_file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=128):
                    file.write(chunk)

            # Unzip the file recursively
            unzip_file(zip_file_path, download_directory)

            # Remove zip to free space
            os.remove(zip_file_path)
        else:
            print(f"Failed to download file: {response.status_code}")

        counter += 1

    # return items[idx]['storeTime']
    return items

feeds = [
    'FastMarkets_BaseMetals',
    'FastMarkets_MinorMetals',
    'FastMarkets_IndustrialMinerals',
    'FastMarkets_OresAlloys',
    'FastMarkets_SteelRawMaterials'
]

types = ['src', 'cf', 'delta']

for feed in feeds:
    for t in types:
        df_test_store_time = download_file(feed, t, 1000)
        print(feed, t, 'done\n')







# ITEMS CME_NymexFutures
import os, shutil, requests, json, zipfile
from requests.auth import HTTPBasicAuth
import pandas as pd

# Function to clean up the download directory
def clean_download_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory)

# Function to unzip files recursively
def unzip_file(zip_file_path, extract_to):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
        for file in zip_ref.namelist():
            if file.endswith('.zip'):
                unzip_file(os.path.join(extract_to, file), extract_to)

# Function to open a JSON file
def open_json(path):
    with open(path, 'r') as file:
        json_data = json.load(file)
    return json_data

feed_names = [
    'FastMarkets_BaseMetals',
    'FastMarkets_MinorMetals',
    'FastMarkets_IndustrialMinerals',
    'FastMarkets_OresAlloys',
    'FastMarkets_SteelRawMaterials'
]

daily_dfs = []
weekly_dfs = []

for feed in feed_names:
    path = os.path.join('downloads', feed, 'src')
    date_folders = os.listdir(path)


    for folder in date_folders:
        # store_time = pd.to_datetime(folder, format='%Y-%m-%dT%H%M%SZ')
        store_time = pd.to_datetime(folder, format='%Y-%m-%dT%H%M%S%z')

        files_path = os.path.join(path, folder)
        files = os.listdir(files_path)

        instruments_file = [f for f in files if f.endswith('_FastMarketsJsonInstrument.txt')][0]
        daily_file = [f for f in files if f.endswith('_FastMarketsJsonPrice_None.txt')][0]
        weekly_file = [f for f in files if f.endswith('_FastMarketsJsonPrice_WeeklyAverage.txt')][0]
        monthly_file = [f for f in files if f.endswith('_FastMarketsJsonPrice_MonthlyAverage.txt')][0]
        yearly_file = [f for f in files if f.endswith('_FastMarketsJsonPrice_YearlyAverage.txt')][0]

        json_data = open_json(os.path.join(files_path, instruments_file))
        instruments_df = pd.json_normalize(json_data['instruments'])

        json_data = open_json(os.path.join(files_path, daily_file))

        # DAILY DATA
        daily_df = pd.json_normalize(
            json_data['instruments'],
            record_path='prices',
            meta=['firstDate', 'lastDate', 'symbol'],
            errors='ignore'
        )

        daily_df['storeTime'] = store_time
        daily_df = daily_df[['assessmentDate', 'storeTime', 'date', 'symbol'] + list(daily_df.columns[2:-2])]
        daily_df = daily_df.merge(instruments_df, on=['symbol'], how='left')
        daily_df.drop(columns=['priceCalculationTypeIds'], inplace=True)

        daily_dfs.append(daily_df.copy())


        # WEEKLY DATA
        json_data = open_json(os.path.join(files_path, weekly_file))
        weekly_df = pd.json_normalize(
            json_data['instruments'],
            record_path='prices',
            meta=['firstDate', 'lastDate', 'symbol'],
            errors='ignore'
        )

        weekly_df['storeTime'] = store_time
        weekly_df = weekly_df[['assessmentDate', 'storeTime', 'date', 'symbol'] + list(weekly_df.columns[2:-2])]
        weekly_df = weekly_df.merge(instruments_df, on=['symbol'], how='left')
        weekly_df.drop(columns=['priceCalculationTypeIds'], inplace=True)

        weekly_dfs.append(weekly_df.copy())

        # break
        # break

# Concatenate and clean daily data
daily_data = pd.concat(daily_dfs, ignore_index=True).sort_values(by=['storeTime', 'symbol'])
daily_data = daily_data.drop_duplicates(subset=daily_data.columns[2:], keep='last').reset_index(drop=True)

# Concatenate and clean weekly data
weekly_data = pd.concat(weekly_dfs, ignore_index=True).sort_values(by=['storeTime', 'symbol'])
weekly_data = weekly_data.drop_duplicates(subset=weekly_data.columns[2:], keep='last').reset_index(drop=True)




# Extract commodity group (e.g., based on keywords in description)
df = daily_data.copy()

df_filtered = df[df['locationId'].str.contains('CHN|QGD|KOR|JPN|IND|VNM|TWN|SGP', case=False)]

df_filtered['commodity_group'] = df_filtered['description'].str.extract(r'(aluminium|copper|iron|lithium|nickel|zinc|gold|silver)', expand=False)

# Drop rows where grouping keyword is missing
df_filtered = df_filtered.dropna(subset=['commodity_group'])

# Calculate arbitrage opportunities per date and commodity group
arbitrage_data = (
    df_filtered.groupby(['commodity_group', 'assessmentDate'])
    .agg(min_price=('mid', 'min'), max_price=('mid', 'max'))
)
arbitrage_data['price_difference'] = arbitrage_data['max_price'] - arbitrage_data['min_price']

# Filter for significant price differences
arbitrage_opportunities = arbitrage_data[arbitrage_data['price_difference'] > 0]

# Reset index for easier handling
arbitrage_opportunities = arbitrage_opportunities.reset_index()

# Sort by price difference
arbitrage_opportunities = arbitrage_opportunities.sort_values(by='price_difference', ascending=False)








# Sort the data by price_difference to get the top 10 arbitrage opportunities
top_arbitrage = arbitrage_opportunities.sort_values(by="price_difference", ascending=False).head(10)

# Visualization of price differences
plt.figure(figsize=(10, 6))
plt.barh(top_arbitrage.index, top_arbitrage['price_difference'], color='skyblue', edgecolor='black')
plt.xlabel('Price Difference')
plt.ylabel('Commodity')
plt.title('Top 10 Arbitrage Opportunities by Price Difference')
plt.gca().invert_yaxis()  # Invert y-axis to show the highest at the top
plt.tight_layout()
plt.show()







# Assuming your dataframe is loaded into a variable named `df`
# Replace 'description' and 'locationId' with the actual column names in your dataset
df = daily_data.copy()

df_filtered = df[df['locationId'].str.contains('CHN|QGD|KOR|JPN|IND|VNM|TWN|SGP', case=False)]

# Grouping by commodity and finding min and max prices for each commodity
arbitrage_data = (
    df_filtered.groupby('description')
    .agg(min_price=('mid', 'min'), max_price=('mid', 'max'))
)
arbitrage_data['price_difference'] = arbitrage_data['max_price'] - arbitrage_data['min_price']

# Filter for significant price differences
arbitrage_opportunities = arbitrage_data[arbitrage_data['price_difference'] > 0]

# Sort by price difference for easy identification
arbitrage_opportunities = arbitrage_opportunities.sort_values(by='price_difference', ascending=False)







# Sort the data by price_difference to get the top 10 arbitrage opportunities
top_arbitrage = arbitrage_opportunities.sort_values(by="price_difference", ascending=False).head(10)

# Visualization of price differences
plt.figure(figsize=(10, 6))
plt.barh(top_arbitrage.index, top_arbitrage['price_difference'], color='skyblue', edgecolor='black')
plt.xlabel('Price Difference')
plt.ylabel('Commodity')
plt.title('Top 10 Arbitrage Opportunities by Price Difference')
plt.gca().invert_yaxis()  # Invert y-axis to show the highest at the top
plt.tight_layout()
plt.show()







