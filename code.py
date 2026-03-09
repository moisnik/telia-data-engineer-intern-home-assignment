import requests
import pandas as pd
import datetime as dt
import sqlite3

def fetch_capitals():
 '''Fetch all European countries and their capital coordinates from RestCountries.
    For each country fetch country name, capital city, capital coordinates, population and area.
    Return a list containing a dictionary for each country.'''
 info = []
 url = 'https://restcountries.com/v3.1/region/europe'
 response = requests.get(url, timeout=10)
 response.raise_for_status()
 data = response.json()
 for country in data:
   # If there is no data about the capital, skip the country
   if 'capital' not in country or 'capitalInfo' not in country or 'latlng' not in country['capitalInfo']:
    continue
   country_info = {}
   country_info['name'] = country['name']['common'] #Use the common name for each country
   country_info['capital'] = country['capital'][0]
   country_info['coordinates'] = country['capitalInfo']['latlng']
   country_info['population'] = country['population']
   country_info['area'] = country['area']
   info.append(country_info)

 return info

def fetch_weather_data(coordinates, start, end):
 '''Use coordinates to fetch 30 days of weather data from Open-Meteo for each capital.
    For each capital fetch max/min temperature, precipitation sum, max windspeed (km/h) and sunshine duration.
    Return a dictionary containing daily weather fields (if exists).'''
 url = 'https://archive-api.open-meteo.com/v1/archive'
 fields = ['temperature_2m_max', 'temperature_2m_min', 'precipitation_sum', 'wind_speed_10m_max', 'sunshine_duration']
 params = {
  'latitude': coordinates[0],
  'longitude': coordinates[1],
  'start_date': start,
  'end_date': end,
  'daily': fields,
  'timezone': 'auto'
 }
 response = requests.get(url, params=params, timeout=10)
 response.raise_for_status()
 data = response.json()

 if 'daily' not in data:
    raise ValueError("Weather API response missing 'daily' data")

 return data['daily']

def combine_country_weather(country, weather_data):
 '''Combine country data with weather data.
    Create a row for each day, containing weather parameters and information about the location.
    Return a list of combined rows.'''
 data = []
 date = weather_data['time']
 max_temp = weather_data['temperature_2m_max']
 min_temp = weather_data['temperature_2m_min']
 precip_sum = weather_data['precipitation_sum']
 max_wind_speed = weather_data['wind_speed_10m_max']
 sun_duration = weather_data['sunshine_duration']

 for i in range(len(date)):
  row = {
   'country': country['name'],
   'capital': country['capital'],
   'latitude': country['coordinates'][0],
   'longitude': country['coordinates'][1],
   'population': country['population'],
   'area': country['area'],
   'date': date[i],
   'max_temperature': max_temp[i],
   'min_temperature': min_temp[i],
   'precipitation_sum': precip_sum[i],
   'max_wind_speed': max_wind_speed[i],
   'sunshine_duration': sun_duration[i]
  }
  data.append(row)

 return data

def extract():
 '''Extract data: fetch information about capitals and 30 days of weather data using predefined functions.
    Return a dataframe with 30 days of weather data for each country's capital'''
 countries_data = fetch_capitals()
 data = []

 # find dates to get weather data for last 30 days
 end = dt.date.today()
 start = end - dt.timedelta(days=29) 

 for country in countries_data:
  weather_data = fetch_weather_data(country['coordinates'], str(start), str(end))
  # If there was no weather data for given capital in the range of dates, skip it
  if weather_data is None:
   continue
  data.extend(combine_country_weather(country, weather_data))

 df = pd.DataFrame(data)
 return df

def transform(df):
 '''Clean and prepare the data before loading.
    Consider rows with missing values, format of sunshine duration, data types and the order of rows
    Return a cleaned dataframe.'''
 df = df.copy()
 
 if df.isnull().values.any():
  df = df.dropna()
  
 # Convert the duration of sunshine from seconds to full minutes of sunshine
 df['sunshine_duration_minutes'] = df['sunshine_duration'] // 60
 df = df.drop(columns=['sunshine_duration'])
 df.sunshine_duration_minutes = df.sunshine_duration_minutes.astype(int)

 df['date'] = pd.to_datetime(df['date'])

 # Sort the rows by capitals and chronological order
 df = df.sort_values(['capital', 'date'])

 return df

def load(raw_data, cleaned_data):
 '''Save the data into a database using SQLite.
    Create separate tables for raw and cleaned data.'''
 database_connection = sqlite3.connect('weather_data.db')

 raw_data.to_sql('raw_weather', database_connection, if_exists='replace', index=False)
 cleaned_data.to_sql('cleaned_weather', database_connection, if_exists='replace', index=False)

 database_connection.close()

def create_views():
 '''Build analytical views on top of the cleaned tables.'''
 database_connection = sqlite3.connect('weather_data.db')

 cursor = database_connection.cursor()

 # View 1: capitals ranked by average temperature
 cursor.execute('DROP VIEW IF EXISTS v_capitals_ranked_by_avg_temperature')
 cursor.execute('''
                CREATE VIEW v_capitals_ranked_by_avg_temperature AS 
                SELECT capital, AVG((min_temperature+max_temperature)/2) AS average_temperature
                FROM cleaned_weather
                GROUP BY capital
                ORDER BY average_temperature DESC;
                ''')

 # View 2: countries with the most rainfall
 cursor.execute('DROP VIEW IF EXISTS v_countries_with_the_most_rainfall')
 cursor.execute('''
                CREATE VIEW v_countries_with_the_most_rainfall AS
                SELECT country, SUM(precipitation_sum) AS total_rainfall
                FROM cleaned_weather
                GROUP BY country
                ORDER BY total_rainfall DESC;
                ''')
 
 # View 3: 30-day summary per country
 cursor.execute('DROP VIEW IF EXISTS v_30_day_summary')
 cursor.execute('''
                CREATE VIEW v_30_day_summary AS
                SELECT country, 
                        MIN(date) AS start_date,
                        MAX(date) AS end_date,
                        COUNT(DISTINCT date) AS days_covered,
                        MAX(max_temperature) AS max_temperature,
                        MIN(min_temperature) AS min_temperature,
                        AVG((max_temperature+min_temperature)/2) AS average_temperature,
                        SUM(precipitation_sum) AS total_rainfall,
                        AVG(max_wind_speed) AS average_max_wind_speed,
                        SUM(sunshine_duration_minutes) AS total_sunshine_minutes,
                        AVG(sunshine_duration_minutes) AS average_sunshine_minutes
                FROM cleaned_weather
                GROUP BY country;
                ''')

 database_connection.commit()
 database_connection.close()

def pipeline():
 '''Create an ETL pipeline and build views.'''
 raw_df = extract()
 cleaned_df = transform(raw_df)
 load(raw_df, cleaned_df)
 create_views()

if __name__ == "__main__":
    pipeline()