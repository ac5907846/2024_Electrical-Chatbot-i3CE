import pandas as pd

# Load the data
df = pd.read_csv('02_Problems.csv')

# Split each item in 'Problems/Challenges' into separate rows
df_exploded = df.assign(Problems_Challenges=df['Problems/Challenges'].str.split(', ')).explode('Problems_Challenges')

# Drop the original 'Problems/Challenges' column
df_exploded = df_exploded.drop(columns=['Problems/Challenges'])

# Rename the exploded column for clarity
df_exploded.rename(columns={'Problems_Challenges': 'Problem_Challenge'}, inplace=True)

# Save the result to a new CSV file or display it
df_exploded.to_csv('03_exploded_problems_challenges.csv', index=False)
print(df_exploded)
