import requests
import pandas as pd

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

 for country in countries_data:
  weather_data = fetch_weather_data(country['coordinates'], '2024-01-01', '2024-01-31')
  data.extend(combine_country_weather(country, weather_data))

 return data

def transform(rows):
 df = pd.DataFrame(rows)
 print(df.info())
 print(df.shape)


def pipeline():
 rows = extract()
 transform(rows)

pipeline()

