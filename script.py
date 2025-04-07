import pandas as pd

def fix_market_cap(eos_file, all_crypto_file, output_file):
    # Load the EOS data
    eos_df = pd.read_csv(eos_file)
    eos_df['Start'] = pd.to_datetime(eos_df['Start'])
    
    # Load the all crypto data
    all_crypto_df = pd.read_csv(all_crypto_file)
    
    # Filter for only EOS data and convert dates
    eos_in_all = all_crypto_df[all_crypto_df['Symbol'] == 'EOS'].copy()
    eos_in_all['Date'] = pd.to_datetime(eos_in_all['Date'], format='%d-%m-%Y %H:%M')
    
    # Normalize dates by removing time components for comparison
    eos_df['Start_date'] = eos_df['Start'].dt.normalize()
    eos_in_all['Date_date'] = eos_in_all['Date'].dt.normalize()
    
    # Create a dictionary of date to market cap for faster lookup
    market_cap_dict = dict(zip(eos_in_all['Date_date'], eos_in_all['Marketcap']))
    
    # Update market cap in EOS data where it's 0.0
    updated_count = 0
    for index, row in eos_df.iterrows():
        if row['Market Cap'] == 0.0 or row['Market Cap'] == -1.0:
            start_date = row['Start_date']
            if start_date in market_cap_dict:
                eos_df.at[index, 'Market Cap'] = market_cap_dict[start_date]
                updated_count += 1
                print(f"Updated market cap for {start_date}: {market_cap_dict[start_date]}")
    
    # Save the updated data
    eos_df.drop(columns=['Start_date'], inplace=True)
    eos_df.to_csv(output_file, index=False)
    print(f"\nUpdated {updated_count} market cap values")
    print(f"Updated file saved to {output_file}")

# Usage
fix_market_cap(
    eos_file='Data/CryptoData/PreProcessedData/EOS.csv',
    all_crypto_file='Data/KaggleData/All_Crypto.csv',
    output_file='EOS_updated.csv'
)