import pandas as pd
import numpy as np

np.random.seed(101)

df = pd.DataFrame(np.random.randn(5,4), index='A B C D E'.split(), columns='W X Y Z'.split())


df['NEW'] = df['W'] + df['Z']


df.drop('NEW', axis=1, inplace=True)



print(df)

# print(df.loc['A', 'W'])
#
# print(df.loc[['A', 'B'], ['X', 'W', 'X']])

print(df.iloc[1:4, 2:])



