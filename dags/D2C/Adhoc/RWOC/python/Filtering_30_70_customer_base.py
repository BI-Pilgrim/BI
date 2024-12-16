import pandas as pd
import random

def tag_customers(df):
    # Randomly select 30% of the customers to be tagged as 'A'
    num_customers = len(df)
    num_tag_a = int(0.3 * num_customers)
    tag_a_indices = random.sample(range(num_customers), num_tag_a)

    # Create a new column 'tag' and assign values
    df['tag'] = 'B'
    df.loc[tag_a_indices, 'tag'] = 'A'

    return df

# Read the CSV file
input_file = 'Customer_Data_01_12_24'
df = pd.read_csv(f'{input_file}.csv')

df_tagged = tag_customers(df)

# Write the tagged DataFrame to a new CSV file
df_tagged.to_csv(f'{input_file}_tagged.csv', index=False)
