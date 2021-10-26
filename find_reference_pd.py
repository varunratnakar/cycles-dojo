#!/usr/bin/env python3

import pandas
import numpy as np
import os

CROPLAND_FILE = "data/HOACropland.csv"
OUTPUT_FILE = "data/HOACropland_new.csv"

countries = [
    'Djibouti',
    'Eritrea',
    'Kenya',
    'Ethiopia',
    'South Sudan',
    'Sudan',
    'Uganda',
    'Somalia',
    'Eritrea',
    'Djibouti',
    ]

crops = ['Maize']

pds = [
    '15',
    '46',
    '74',
    '105',
    '135',
    '166',
    '196',
    '227',
    '258',
    '288',
    '319',
    '345',
]

# Open cropland file
cropland_df = pandas.read_csv(CROPLAND_FILE)
cropland_df.fillna('NaN', inplace=True)     # Fill NaN values incase 'admin3' does not exist
cropland_df.set_index(['country', 'admin1', 'admin2', 'admin3'], inplace=True)

for crop in crops:
    # Create empty data frame
    results = pandas.DataFrame()

    for country in countries:
        for pd in pds:
            # Read output files
            output_df = pandas.read_csv('outputs/%s.%s.%s.csv' % (country, crop, pd), usecols=range(1,9))
            output_df.fillna('NaN', inplace=True)   # Fill NaN values incase 'admin3' does not exist
            output_df['pd'] = pd    # Add planting date as a column

            # Combine all results
            results = results.append(output_df)

    # Calculate mean grain yield for each planting date
    results = pandas.DataFrame(results.groupby(['country', 'admin1', 'admin2', 'admin3', 'pd'])['grain_yield'].mean())

    # Reset 'pd' to regular column
    results = results.reset_index(level=['pd'])

    # Find maximum yield
    ref_pd = results[results.groupby(['country', 'admin1', 'admin2', 'admin3'])['grain_yield'].transform(max) == results['grain_yield']]

    # Rename pd and grain_yield columns to each crop
    crop = crop.strip().lower().replace(' ', '_')
    ref_pd.rename(columns={'pd': '%s_pd' %(crop), 'grain_yield': '%s_grain_yield' %(crop)}, inplace=True)

    # Add planting dates and yields to cropland data
    df = cropland_df.merge(ref_pd, how = 'inner', on = ['country', 'admin1', 'admin2', 'admin3'])

    # Write to output file
    df = df.reset_index(level=['country', 'admin1', 'admin2', 'admin3'])
    df.replace('NaN', '', regex=True, inplace=True)

    df.to_csv(OUTPUT_FILE, index=False)
