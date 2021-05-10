import requests
import os
import datetime
from contextlib import closing
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import sqlite3



def select_country(df, country):
    df_country = df.loc[df['Country_Region'] == country]
    df_country.reset_index(drop=True, inplace=True)
    return df_country

# Check the data before load into database
def check_df(df):
    # check if dataframe is empty
    if df.empty:
        print('No data for this country on the selected day. Finishing execution')
        return False
    # primary key check
    if not pd.Series(df['Combined_Key']).is_unique:
        raise Exception('Primary Key check is violated.')
    # check for Null, remove the row with null value
    if df.isnull().values.any():
        raise Exception('Null values found.')
    return True

def covid_etl(country, starting_date): # starting_date format: "01-22-2020"
    database_location = "sqlite:///covid19.sqlite"
    # download data from 2 days ago
    today = datetime.date.today()
    lastday = (today - datetime.timedelta(days=1)).strftime('%m-%d-%Y')
    lastday = datetime.datetime.strptime(lastday, "%m-%d-%Y")
    startday = datetime.datetime.strptime(starting_date, "%m-%d-%Y")
    for i in range((lastday - startday).days + 1):
        day = startday + datetime.timedelta(days=i)
        day_str = "%02d-%02d-%d" % (day.month, day.day, day.year)
        print(f'Country: {country}, date: {day_str}')
        data_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{}.csv".format(day_str)
        file_path = os.path.join('./data', day_str) + '.csv'
        with closing(requests.get(data_url, stream=True)) as response:
            chunk_size = 1024  
            if response.status_code == 200:
                print('downloading file(s)...')
                df = pd.read_csv(data_url)
                df_new = df.drop(['FIPS', 'Admin2', 'Lat', 'Long_'], axis = 1)
                df_new = df_new.dropna(how='any',axis=0) # drop rows with NaN in them
                print('Finished.')
                df_country = select_country(df_new, country)
                # print(df_country)
                
                # Validate
                if check_df(df_country):
                    print('"Data valid, proceed to Load stage"')

                # load
                engine = sqlalchemy.create_engine(database_location)
                conn = sqlite3.connect('covid19.sqlite')
                cursor = conn.cursor()
                sql_query = """
                CREATE TABLE IF NOT EXISTS covid19(
                Province_State VARCHAR(200),
                Country_Region VARCHAR(200),
                Last_Update DATETIME,
                Confirmed INT,
                Deaths INT,
                Recovered INT,
                Active INT,
                Combined_Key VARCHAR(200),
                Incident_Rate FLOAT,
                Case_Fatality_Ratio FLOAT,
                CONSTRAINT primary_key_constraint PRIMARY KEY (Combined_Key, Last_Update))
                """
                cursor.execute(sql_query)
                print("Opened database successfully")
                try:
                    df_country.to_sql("covid19", engine, index=False, if_exists='append')
                except:
                    print("Data already exists in the database")
                    
                conn.close()
                print("Close database successfully")
            
            else:
                print('Error, cannot download data.')
    
if __name__ == '__main__':
    covid_etl('India')