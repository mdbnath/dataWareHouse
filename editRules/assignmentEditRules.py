import os,sys
import pandas as pd

df = pd.read_csv (r'C:\Users\manja\OneDrive\Documents\Advanced Database\DatawareHouseProject\editRules\clinical_trials.csv')
###print (df)
number_of_rows = len(df.index)
print("Number of rows: {:d}".format(number_of_rows)) 

number_of_columns = len(df.columns)
print("Number of rows: {:d}".format(number_of_columns)) 

resultSet=df.head(10)
print("First ten rows of the dataset: " )
print(resultSet)

print("All columns and column values: ")
print(df.all)

countNullvalues = df.isna().sum().sum()
print(countNullvalues)

rowNullvalues = df.loc[[index value]].isna().sum().sum()
print(countNullvalues)