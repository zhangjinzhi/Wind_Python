import numpy as np
import pandas as pd

df_empty = pd.DataFrame(columns=['A', 'B'])  
df_empty["A"] = [1,1,1,2,2,2]
df_empty["B"] = [3,3,3,-100,0,100]

print df_empty

print "df_empty['A'] mean is :",np.average(df_empty["A"]),"df_empty['A'] std is :",np.std(df_empty["A"])

print "df_empty['B'] mean is :",np.average(df_empty["B"]),"df_empty['B'] std is :",np.std(df_empty["B"])

for item in df_empty['A']:
	print "(x - np.average(df_empty['A'])) / np.std(df_empty['A']) is :",(item - np.average(df_empty['A'])) / np.std(df_empty['A'])

for item in df_empty['B']:
	print "(x - np.average(df_empty['B'])) / np.std(df_empty['B']) is :",(item - np.average(df_empty['B'])) / np.std(df_empty['B'])

print "clean_data_df.apply(lambda x: (x - np.average(x)) / np.std(x))"
df_empty = df_empty.apply(lambda x: (x - np.average(x)) / np.std(x)) 
print df_empty