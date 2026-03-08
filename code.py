import requests
import pandas as pd
import datetime as dt
import sqlite3

def fetch_capitals():
 #Fetch all European countries and their capital coordinates from RestCountries.
 #For each country fetch country name, capital city, capital coordinates (latitude & longitude), population and area (optional).
 info = []
 url = 'https://restcountries.com/v3.1/region/europe'
 data = requests.get(url).json()
 for country in data:
   country_info = {}
   country_info['name'] = country['name']['common'] #Use the common name for each country
   country_info['capital'] = country['capital'][0]
   country_info['coordinates'] = country['capitalInfo']['latlng']
   country_info['population'] = country['population']
   country_info['area'] = country['area']
   info.append(country_info)

 return info

def fetch_weather_data(coordinates, start, end):
 #Use coordinates to fetch 30 days of weather data from Open-Meteo for each capital.
 #For each capital fetch max/min temperature, precipitation sum, max windspeed (km/h) and sunshine duration.
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
 
 data = requests.get(url, params=params).json()

 print(data)

 return data['daily']

def combine_country_weather(country, weather_data):
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
   'precipation_sum': precip_sum[i],
   'max_wind_speed': max_wind_speed[i],
   'sunshine_duration': sun_duration[i]
  }
  data.append(row)

 return data

def extract():
 countries_data = fetch_capitals()
 data = []

 # find dates to get weather data for last 30 days
 end = dt.date.today()
 start = end - dt.timedelta(days=29) 


 for country in countries_data:
  weather_data = fetch_weather_data(country['coordinates'], str(start), str(end))
  data.extend(combine_country_weather(country, weather_data))

 
 dataFrame = pd.DataFrame(data)
 return dataFrame

def transform(df):
 # Detect rows with missing values and drop them
 if df.isnull().values.any():
  df.dropna()
  print('Found and dropped rows with missing values.')
  
 # Convert the duration of sunshine from seconds to full minutes of sunshine
 df['sunshine_duration_minutes'] = df['sunshine_duration'] // 60
 df = df.drop(columns=['sunshine_duration'])
 df.sunshine_duration_minutes = df.sunshine_duration_minutes.astype(int)

 # Convert the data type of dates from string to date
 df['date'] = pd.to_datetime(df['date'])

 # Sort the capitals in chronological order (should be by default)
 df = df.sort_values(['capital', 'date'])

 return df


def load(raw_data, cleaned_data):
 database_connection = sqlite3.connect('weather_data.db')

 raw_data.to_sql('raw_weather', database_connection, if_exists='replace', index=False)
 cleaned_data.to_sql('cleaned_weather', database_connection, if_exists='replace', index=False)

 database_connection.close()

def create_views():
 database_connection = sqlite3.connect('weather_data.db')

 cursor = database_connection.cursor()

 cursor.execute('DROP VIEW IF EXISTS v_capitals_ranked_by_avg_temperature')
 cursor.execute('''
                CREATE VIEW v_capitals_ranked_by_avg_temperature AS' 
                SELECT capital, AVG((min_temperature+max_temperature)/2) AS average_temperature
                FROM cleaned_weather
                GROUP BY capital
                ORDER BY average_temperature DESC;
 ''')

 database_connection.commit()
 database_connection.close()



def pipeline():
 raw_df = extract()
 cleaned_df = transform(raw_df)
 load(raw_df, cleaned_df)
 create_views()


pipeline()

conn = sqlite3.connect("weather_data.db")

c = conn.cursor()
c.execute('SELECT * FROM v_capitals_ranked_by_avg_temperature LIMIT 10;')

