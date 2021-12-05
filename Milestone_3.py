# -*- coding: utf-8 -*-
# CIS 9440 - Milestone #3 
# Team 2
# Taylor Thompson + Jack Hazan 
import pandas as pd 
from google.cloud import bigquery
from google.oauth2 import service_account
import pyarrow
import geopy

class ETL_pipeline:

    def __init__(self, name):
        self.name = name

    def establish_bigquery_connection(key_path:str,
                                      scopes_url = r'https://www.googleapis.com/auth/cloud-platform'):
        # created bigquery credentials from service account key json file
        credentials = service_account.Credentials.from_service_account_file(key_path)

        # initiate the bigquery client with the credentials
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        # return the bigquery client
        return client

    def extract_bigquery_data(client,
                              sql_query:str):
        print("-----\n extracting data from BigQuery")
        # gather data from bigquery to dataframe
        try:
            df = client.query(sql_query).to_dataframe()

            # validate that >0 rows have been extracted and return dataframe
            if len(df) > 0:
                print(f"{len(df)} rows extracted \n-----")
                return df

            # if data extraction fails print failure
            else:
                print(f"{sql_query} has 0 rows of data")

        # if data extraction fails print failure
        except:
            print(f"{sql_query} extraction failed")


    def create_dimension(df,
                         dimension_columns:list, # [first attribute, second, third]
                         surrogate_key_name:str,
                         surrogate_key_integer_start:int):

        print(f"-----\n creating dimension: {dimension_columns[0]}_dim")
        # copy full dataframe to create dimension from subset
        dim = df.copy()
        dim = dim[dimension_columns]

        # drop unneeded rows in hierarchy
        dim = dim.drop_duplicates(subset=[dimension_columns[0]], keep = "first")

        # add surrogate key
        dim.insert(0, surrogate_key_name, range(surrogate_key_integer_start,
                                                surrogate_key_integer_start+len(dim)))

        print(f"dimension {dimension_columns[0]}_dim created with {len(dim)} rows \n-----")
        # return the dimension as a dataframe
        return dim

    def load_table_to_bigquery(bq_client,
                               table,
                               dataset_name:str,
                               table_name:str):

        print(f"-----\n loading {table_name} to BigQuery dataset {dataset_name}")
        # define bigquery client
        client = bq_client

        # define location you will upload table to in bigquery
        table_ref = client.dataset(dataset_name).table(table_name)

        # configure load job settings
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.write_disposition = "WRITE_TRUNCATE"

        # initiate load job
        load_job = client.load_table_from_dataframe(table, table_ref,
                                                    job_config=job_config)
        load_job.result()

        # Make a BigQuery API request to check if new table was loaded successfully
        validate_table = client.get_table(table_ref)  # Make an API request.
        print(f"Loaded {validate_table.num_rows} rows and {len(validate_table.schema)} columns to {table_ref} \n-----")
        
    

# ---- DATA PREPARATION --- 
''' The following section is the initial data preparation. This includes
loading the datasets from csv files, selecting the columns we want, removing
any malformed data. Another important action covered in this section is 
collecting the counts that will be used in the final fact table.
''' 
# 1) Air Quality 
air_quality_df = pd.read_csv('Air_Quality.csv')
air_quality_df = air_quality_df[air_quality_df['Geo Type Name'] != 'Borough']
air_quality_df.drop(['Indicator ID', 'Measure', 'Measure Info', 'Geo Join ID', 'Time Period', 'Message'],axis=1,inplace=True)
air_quality_df.dropna()
air_quality_df['Start_Date'] = pd.to_datetime(air_quality_df['Start_Date'], format='%m/%d/%Y')
air_quality_df['Year'] = pd.DatetimeIndex(air_quality_df['Start_Date']).year
air_quality_df = air_quality_df[air_quality_df['Geo Type Name'] != 'Borough']
air_quality_grouped_df = air_quality_df.groupby(['Year', 'Name', 'Geo Place Name'], as_index=False)['Data Value'].sum()
air_quality_descriptions = air_quality_grouped_df['Name'].drop_duplicates()
air_quality_descriptions_df = pd.DataFrame({'ID':air_quality_descriptions.index, 'air_quality_name':air_quality_descriptions.values})
#2) Trees
tree_census_2015_df = pd.read_csv('2015_Street_Tree_Census_-_Tree_Data.csv')
tree_census_2015_df.drop(['block_id', 
                     'tree_dbh', 'stump_diam', 'curb_loc', 
                     'spc_latin','spc_common', 'steward', 'guards', 
                     'sidewalk', 'user_type', 'problems', 
                     'root_stone', 'root_grate', 'root_other', 
                     'trunk_wire', 'trnk_light', 'trnk_other', 
                     'brch_light', 'brch_shoe', 'brch_other', 
                     'community board', 'cncldist', 'st_assem', 
                     'st_senate', 'nta','nta_name', 'boro_ct', 
                     'state', 'latitude', 'longitude', 'x_sp', 
                     'y_sp', 'council district',
                     'census tract', 'bin', 'bbl'],  
                    axis=1, 
                    inplace=True)
tree_census_2015_df['created_at'] = pd.to_datetime(tree_census_2015_df['created_at'], format='%m/%d/%Y')
tree_census_2015_df['Year'] = pd.DatetimeIndex(tree_census_2015_df['created_at']).year
tree_census_2015_df.dropna()
tree_census_2015_grouped = tree_census_2015_df.groupby(['Year','zipcode'])['tree_id'].count().reset_index()
tree_census_2015_grouped.rename({'tree_id': 'Tree_Count'}, axis=1, inplace=True)

tree_census_2005_df = pd.read_csv('2005_Street_Tree_Census.csv')
tree_census_2005_df.drop(['tree_dbh', 'address', 'tree_loc', 
                          'pit_type', 'soil_lvl','spc_latin', 
                          'spc_common', 'vert_other', 'vert_pgrd', 
                          'vert_tgrd', 'vert_wall', 'horz_blck', 
                          'horz_grate', 'horz_plant', 'horz_other', 
                          'sidw_crack', 'sidw_raise', 'wire_htap', 
                          'wire_prime', 'wire_2nd', 'wire_other', 
                          'inf_canopy', 'inf_guard', 'inf_wires', 
                          'inf_paving', 'inf_outlet', 'inf_shoes', 
                          'inf_lights', 'inf_other', 'trunk_dmg',
                          'cb_num','cncldist', 'st_assem', 'st_senate',
                          'nta', 'nta_name', 'boro_ct', 'state', 'latitude', 
                          'longitude', 'x_sp', 'y_sp', 'objectid_1', 
                          'census tract', 'bin', 'bbl', 'Location 1'],  
                    axis=1, 
                    inplace=True)
tree_census_2005_df.dropna()
tree_census_2005_filtered_df = tree_census_2005_df.loc[~(tree_census_2005_df['cen_year'] == 0) & ~(tree_census_2005_df['zipcode'] == 0)]
tree_census_2005_grouped = tree_census_2005_filtered_df.groupby(['cen_year','zipcode'])['OBJECTID'].count().reset_index()
tree_census_2005_grouped.rename({'OBJECTID': 'Tree_Count'}, axis=1, inplace=True)

# Applying 2015 data to 2016->2017->2018
tree_census_2016 = tree_census_2015_grouped.copy()
tree_census_2016['Year'] = tree_census_2016['Year'].replace(2015, 2016)
tree_census_2017 = tree_census_2015_grouped.copy()
tree_census_2017['Year'] = tree_census_2017['Year'].replace(2015, 2017)
tree_census_2018 = tree_census_2015_grouped.copy()
tree_census_2018['Year'] = tree_census_2018['Year'].replace(2015, 2018)

# Combining df's for 2015-2018 into 1 dataframe
trees_2015_2016 = pd.concat([tree_census_2015_grouped, tree_census_2016], ignore_index=True)
trees_2015_2017 = pd.concat([trees_2015_2016, tree_census_2017], ignore_index=True)
trees_2015_2018 = pd.concat([trees_2015_2017, tree_census_2018], ignore_index=True)

# Combining df's for 2005-2014 (Note: 2006 data is already included in the tree_census_2005_df)
tree_census_to_copy = tree_census_2005_grouped.loc[(tree_census_2005_grouped['cen_year'] == 2006)]
tree_census_2007 = tree_census_to_copy.copy()
tree_census_2007['cen_year'] = tree_census_2007['cen_year'].replace(2005, 2007)
tree_census_2008 = tree_census_to_copy.copy()
tree_census_2008['cen_year'] = tree_census_2008['cen_year'].replace(2005, 2008)
tree_census_2009 = tree_census_to_copy.copy()
tree_census_2009['cen_year'] = tree_census_2009['cen_year'].replace(2005, 2009)
tree_census_2010 = tree_census_to_copy.copy()
tree_census_2010['cen_year'] = tree_census_2010['cen_year'].replace(2005, 2010)
tree_census_2011 = tree_census_to_copy.copy()
tree_census_2011['cen_year'] = tree_census_2011['cen_year'].replace(2005, 2011)
tree_census_2012 = tree_census_to_copy.copy()
tree_census_2012['cen_year'] = tree_census_2012['cen_year'].replace(2005, 2012)
tree_census_2013 = tree_census_to_copy.copy()
tree_census_2013['cen_year'] = tree_census_2013['cen_year'].replace(2005, 2013)
tree_census_2014 = tree_census_to_copy.copy()
tree_census_2014['cen_year'] = tree_census_2014['cen_year'].replace(2005, 2014)
trees_2005_2007 = pd.concat([tree_census_2005_grouped, tree_census_2007], ignore_index=True)
trees_2005_2008 = pd.concat([trees_2005_2007, tree_census_2008], ignore_index=True)
trees_2005_2009 = pd.concat([trees_2005_2008, tree_census_2009], ignore_index=True)
trees_2005_2010 = pd.concat([trees_2005_2009, tree_census_2010], ignore_index=True)
trees_2005_2011 = pd.concat([trees_2005_2010, tree_census_2011], ignore_index=True)
trees_2005_2012 = pd.concat([trees_2005_2011, tree_census_2012], ignore_index=True)
trees_2005_2013 = pd.concat([trees_2005_2012, tree_census_2013], ignore_index=True)
trees_2005_2014 = pd.concat([trees_2005_2013, tree_census_2014], ignore_index=True)
trees_2005_2014.rename({'cen_year': 'Year'}, axis=1, inplace=True)

# Final Combination of Tree Census Data 
tree_census_2005_2018 = pd.concat([trees_2005_2014, trees_2015_2018])

#3) Population
population_df = pd.read_csv('pop-by-zip-code.csv')
population_df.drop(['aggregate', 'Zip'], axis=1, inplace=True)
population_2010 = population_df[['zip_code', 'y-2010']]
population_2011 = population_df[['zip_code', 'y-2011']]
population_2012 = population_df[['zip_code', 'y-2012']]
population_2013 = population_df[['zip_code', 'y-2013']]
population_2014 = population_df[['zip_code', 'y-2014']]
population_2015 = population_df[['zip_code', 'y-2015']]
population_2016 = population_df[['zip_code', 'y-2016']]
population_2017 = population_df[['zip_code', 'y-2017']]
population_2018 = population_df[['zip_code', 'y-2018']]
population_df.dropna()

# 4) NYC Neighborhoods By Zip 
neighborhoods_df = pd.read_csv('nyc_zip_borough_neighborhoods_pop.csv')
neighborhoods_df.drop(['density', 'population', 'post_office'], axis=1, inplace=True)
neighborhoods_df.dropna()

# ---- ETL Pipeline ----- 
# ---- Connecting to BigQuery ----
bigquery_client = ETL_pipeline.establish_bigquery_connection(key_path = "cis9440-324622-457f9c526b49.json")

# ---- Load Subway Dataset From BigQuery ---- 
subway_stations_df = ETL_pipeline.extract_bigquery_data(bigquery_client,"""SELECT station_id,
station_name,
borough_name,
line, station_lat,
station_lon FROM `bigquery-public-data.new_york_subway.stations`;""")

# Geolocator API used to find zipcode from latitude + longitude
geolocator = geopy.Nominatim(user_agent='ttaythompson@gmail.com')
def findzipcode(df):
    try:
        location = geolocator.reverse((df['station_lat'], df['station_lon']))
        zipcode = location.raw['address']['postcode']
        return zipcode
    except:
        return 0
    
subway_stations_df['zipcode'] = subway_stations_df.apply(lambda row: findzipcode(row), axis=1)
malformed_zipcodes = ['10001-2062','10011-6832','10012-3332','10023-7503','10025-4403','10035-3501', '10307:10312','10309:10312','10451:10455','10457-2919','11201-1832', '11224-4003','11374-2756','10037:10454', '11104:11377']
subway_stations_df = subway_stations_df[~subway_stations_df['zipcode'].isin(malformed_zipcodes)]
# Grouping the subway station dataset by zipcode and calculating the count
subway_stations_grouped = subway_stations_df.groupby(['zipcode'])['station_id'].count().reset_index()
subway_stations_grouped['zipcode'] = subway_stations_df['zipcode'].astype(int)
# Changing the count column name to Train Count
subway_stations_grouped.rename({'station_id': 'Train_Count'}, axis=1, inplace=True)

# Creating the location dimension
location_dim = ETL_pipeline.create_dimension(df = neighborhoods_df,
                                           dimension_columns = ["zip", "borough", "neighborhood"],
                                           surrogate_key_name = "location_id",
                                           surrogate_key_integer_start = 1000)

# Creating the air quality dimension
air_quality_dim = ETL_pipeline.create_dimension(df = air_quality_descriptions_df,
                                           dimension_columns = ["air_quality_name"],
                                           surrogate_key_name = "air_quality_id",
                                           surrogate_key_integer_start = 50)

# Creating the date dimension
years = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
decades = []

# For each year find the matching decade
for year in years: 
    decade = year - (year%10)
    decades.append(decade)

# Convert the years array and decades array into a dict
years_decades = {'Year': years, 'Decade':decades}

# Convert the dictionary into a dataframe
date_dim = pd.DataFrame(years_decades)

# Change the key of the dataframe to be the Year column
date_dim.set_index("Year", inplace = True)



# Creating the fact table

# Merging the dimensions with the aggregated tables to create the fact table
merge_location_air_quality =  pd.merge(location_dim,air_quality_grouped_df,how='left',left_on=['neighborhood'],
                  right_on=['Geo Place Name']).dropna()

merge_location_trees = pd.merge(tree_census_2005_2018,location_dim,how='left',left_on=['zipcode'],
                  right_on=['zip']).dropna()

merge_location_trees_subway = pd.merge(merge_location_trees,subway_stations_grouped,how='left',left_on=['zipcode'],
                  right_on=['zipcode']).dropna()
merge_location_trees_subway['zip'] = merge_location_trees_subway['zip'].astype(int)
merge_location_air_quality_id = pd.merge(merge_location_air_quality, air_quality_dim, how='right', left_on='Name', right_on='air_quality_name')
merge_location_air_quality_id['Year'] = merge_location_air_quality_id['Year'].astype(int)
air_quality_fact_table = pd.merge(merge_location_air_quality_id, merge_location_trees_subway[['zip','Tree_Count','Train_Count','Year']], on = ['zip', 'Year'], how = 'left').dropna()
# Added this method to easily access the population data by zip code
def find_population(row):
    zipcode = row['zip']
    if row['Year'] == 2010:
        return population_2010.loc[population_2010['zip_code'] == zipcode, 'y-2010'].iloc[0]
    if row['Year'] == 2011:
        return population_2011.loc[population_2011['zip_code'] == zipcode, 'y-2011'].iloc[0]
    if row['Year'] == 2012:
        return population_2012.loc[population_2012['zip_code'] == zipcode, 'y-2012'].iloc[0]
    if row['Year'] == 2013:
        return population_2013.loc[population_2013['zip_code'] == zipcode, 'y-2013'].iloc[0]
    if row['Year'] == 2014:
        return population_2014.loc[population_2014['zip_code'] == zipcode, 'y-2014'].iloc[0]
    if row['Year'] == 2015:
        return population_2015.loc[population_2015['zip_code'] == zipcode, 'y-2015'].iloc[0]
    if row['Year'] == 2016:
        return population_2016.loc[population_2016['zip_code'] == zipcode, 'y-2016'].iloc[0]
    if row['Year'] == 2017:
        return population_2017.loc[population_2017['zip_code'] == zipcode, 'y-2017'].iloc[0]
    if row['Year'] == 2018:
        return population_2018.loc[population_2018['zip_code'] == zipcode, 'y-2018'].iloc[0]

# Add a population count column 
air_quality_fact_table['Population_Count'] = air_quality_fact_table.apply(lambda row: find_population(row), axis=1)
air_quality_fact_table.dropna(inplace=True)
air_quality_fact_table.rename({'Data Value': 'Pollution_Level'}, axis=1, inplace=True)
air_quality_fact_table.drop(['Name', 'Geo Place Name', 'air_quality_name', 'zip', 'borough', 'neighborhood'],axis=1,inplace=True)    
air_quality_fact_table.to_csv('Air_Quality_Fact_Table_Final.csv')
# Upload Dimensions + Fact Table To Big Query
air_quality_fact_table.index.name = 'ID'
ETL_pipeline.load_table_to_bigquery(bigquery_client,
                           air_quality_fact_table,
                           dataset_name="milestone3",
                           table_name="air_quality_fact")

