import pandas as pd
from pytrends.request import TrendReq
import time

# initialize pytrends
pytrend = TrendReq(retries=3, backoff_factor=2)

# artists and their google trends topics identifiers
artists_half_1 = {
    "Ed Sheeran": "/m/0g9sr1k", 
    "Guns N' Roses": "/m/081wh1", 
    "Coldplay": "/m/0kr_t", 
    "Metallica": "/m/04rcr", 
    "Alphabet": "Alphabet", # control
}
artists_half_2 = {
    "The Rolling Stones": "/m/07mvp", 
    "Pink": "/m/01vrt_c", 
    "U2": "/m/0dw4g" , 
    "Bruno Mars": "/m/0bs1g5r",
    "Alphabet": "Alphabet", # control
}

# all US 50 states' abbreviations
states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
          'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
          'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
          'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
          'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

# function to download google trends data for a given artist's topic identifier
# returns two dataframes -> one df of artists in `artists_half_1` for state
#                        -> one df of artists in `artists_half_1` for state
def download_trends_for_state(state: str, timeframe='2015-01-01 2019-12-31') -> list[pd.DataFrame]:
    
    def download_trends_for_state_for_artists(artists_dict: dict[str, str]) -> pd.DataFrame:
        # create payload for half of artists for given state
        pytrend.build_payload(kw_list=list(artists_dict.values()), timeframe=timeframe, geo=f'US-{state}')
        data = pytrend.interest_over_time()
        
        if not data.empty:
            data = data.drop(labels=['isPartial'], axis='columns')  # Drop the isPartial column
            data.columns = artists_dict.keys()
            data.reset_index(inplace=True)
        return data
                
    return [download_trends_for_state_for_artists(artist_dict) for artist_dict in (artists_half_1, artists_half_2)]

for state in states:
    print(f"Downloading trend data for {state}")
    
    df_half_1, df_half_2 = download_trends_for_state(state)
     # save the datasets to a CSV file
    df_half_1.to_csv(f"{state}_1_trend_data.csv")
    df_half_2.to_csv(f"{state}_2_trend_data.csv")
    
    print(f"Trend data for {state} downloaded and saved.")
    time.sleep(5) # sleep for 5 seconds to avoid rate limiting

print("All data has been downloaded.")

