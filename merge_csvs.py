import pandas as pd
# all US 50 states' abbreviations
states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
          'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
          'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
          'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
          'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

artists = [
    "Ed Sheeran",
    "Guns N' Roses",
    "Coldplay",
    "Metallica",
    "The Rolling Stones",
    "Pink",
    "U2",
    "Bruno Mars",
]

for state in states:
    artist_dfs = [pd.read_csv(f"new_csvs/{state}_{artist.replace(' ', '-')}_trend_data.csv") for artist in artists]
    # now, lets merge the artist dfs
    # first, we have to divide each artist's trend scores by the alphabet scores for their csv. 
    # That way, we can normalize all the artists with respect to Alphabet so that we can then 
    # combine everyone's trend scores together.
    for artist_df, artist in zip(artist_dfs, artists):
        artist_df[artist] = artist_df[artist] / artist_df['Alphabet']
    
    # then, merge the dfs together
    merged_df = artist_dfs[0].loc[:, ['date', artists[0]]]
    for i, next_df in enumerate(artist_dfs[1:], 1):
        merged_df = pd.merge(merged_df, next_df.loc[:, ['date', artists[i]]], on='date', how='inner')
    
    # finally, save the merged df
    merged_df.to_csv(f'merged_csvs/{state}_trend_data.csv')
    print(f'Finished merging {state}')