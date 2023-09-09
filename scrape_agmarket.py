import argparse
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from sqlalchemy import create_engine
import time

# Drop down dictionary
commodity_dict = {
    "Ajwan": "137",
    "Alasande Gram": "281",
    "Almond(Badam)": "325",
    "Alsandikai": "166",
    "Amaranthus": "86",
    "Tomato": "78",
    "Onion": "23",
    "Potato": "24",
    "Wheat": "1"
}

state_dict = {
    "Andaman and Nicobar": "AN",
    "Andhra Pradesh": "AP",
    "Arunachal Pradesh": "AR",
    "Assam": "AS",
    "Bihar": "BI",
    "Maharashtra": "MH",
    "Karnataka": "KK",
    "West Bengal": "WB"
}
# Aggrigation dictionary
agg_dict = {"dialy": "%a", "weekly": "%W", "monthly": "%b-%y", "yearly": "%Y"}

# Argument Parser
parser = argparse.ArgumentParser(description='Scrape data from Agmarknet.')

parser.add_argument('--commodity', type=str, help='Commodity name')
parser.add_argument('--start_date', type=str, help='Start date')
parser.add_argument('--end_date', type=str, help='End date')
parser.add_argument('--time_agg', type=str, default='weekly',
                    help='Time aggregation (default: weekly)', choices=list(agg_dict.keys()))
parser.add_argument('--states', type=str, help='State name')

args = parser.parse_args()

# Access the values of the arguments
commodity = args.commodity.capitalize()
states = args.states.split(',') if args.states else []
start_date = args.start_date
end_date = args.end_date
time_agg = args.time_agg


# Web scrap and data frame
df = pd.DataFrame()

for i, state in enumerate(states):
    url = f'https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_Commodity={commodity_dict.get(commodity, "")}&Tx_State={state_dict.get(state, "")}&Tx_District=0&Tx_Market=0&DateFrom={start_date}&DateTo={end_date}&Fr_Date={start_date}&To_Date={end_date}&Tx_Trend=2&Tx_CommodityHead={commodity}&Tx_StateHead={state}&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--'

    try:
        print(f"Initiating Web Request for {state}...")
        page = requests.get(url, timeout=10)
        # Raise an exception for HTTP errors
        page.raise_for_status()
        soup = BeautifulSoup(page.text, 'html.parser')
        print("Data Extraction and Structuring...")
        if i < 1:
            titles = soup.find_all('th')
            header_list = [title.text for title in titles]
            header_list.insert(3, "Commodity")
            df = pd.DataFrame(columns=header_list)

        col_data = soup.find_all('tr')

        value = []
        data_value = []
        pre_arr = None
        for row in col_data[1:-2]:
            row_data = row.find_all('td')
            ind_row_data = [data.text for data in row_data]
            last_item = ind_row_data[-1]
            if re.match(r'\d{2} \w{3} \d{4}', last_item):
                if len(ind_row_data) == 9:
                    if pre_arr is not None:
                        ind_row_data.insert(5, pre_arr)
                pre_arr = ind_row_data[5]
                ind_row_data.insert(3, commodity)

                length = len(df)
                df.loc[length] = ind_row_data
    # Catch an exception for HTTP errors
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {state}: {e}")
        continue
    # Catch an exception for data processing
    except Exception as e:
        print(f"Error processing data for {state}: {e}")
        continue

    # Delay to avoid overloading the server
    time.sleep(2)

if not df.empty:
    df['Arrivals (Tonnes)'] = df['Arrivals (Tonnes)'].str.replace(
        ',', '', regex=True).astype(float).astype(int)
    df[['Min Price (Rs./Quintal)', 'Max Price (Rs./Quintal)', 'Modal Price (Rs./Quintal)']] = df[[
        'Min Price (Rs./Quintal)', 'Max Price (Rs./Quintal)', 'Modal Price (Rs./Quintal)']].astype(int)
    df['Reported Date'] = pd.to_datetime(
        df['Reported Date'], format='%d %b %Y')
    grouped = df.groupby([df['Reported Date'].dt.strftime(agg_dict.get(time_agg, "")), 'State Name', 'Commodity']).agg(
        {'Arrivals (Tonnes)': 'sum', 'Min Price (Rs./Quintal)': 'mean', 'Max Price (Rs./Quintal)': 'mean', 'Modal Price (Rs./Quintal)': 'mean'})
    grouped['Start Date'] = grouped.index.to_series().apply(lambda x: df[(df['Reported Date'].dt.strftime(agg_dict.get(
        time_agg, "")) == x[0]) & (df['State Name'] == x[1]) & (df['Commodity'] == x[2])]['Reported Date'].min())
    grouped['End Date'] = grouped.index.to_series().apply(lambda x: df[(df['Reported Date'].dt.strftime(agg_dict.get(
        time_agg, "")) == x[0]) & (df['State Name'] == x[1]) & (df['Commodity'] == x[2])]['Reported Date'].max())
    grouped.to_csv("grouped.csv")
    print("Data Download Complete")
    try:
        print("Data Storage Operations...")
        # Create a database connection and append data to a table
        #conn = create_engine('postgresql://postgres:xxxxx@xx.xx.xx.xxx:xxxx/agriiq')
        #grouped.to_sql('agmarket_monthly', conn, if_exists='append')
        print("Data Append Confirmation")
    except:
        print("Failed to append data to the database!!!")
else:
    print("No data was available")
