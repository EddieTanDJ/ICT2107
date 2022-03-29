
import pandas as pd
 
# read DataFrame
data = pd.read_csv("tweets_usa_id_cleaned_all.csv")
 
# no of csv files with row size
k = 10
size = 4000000
 
for i in range(k):
    df = data[size*i:size*(i+1)]
    df.to_csv(f'tweets_usa_id_cleaned_{i+1}.csv', index=False)
 