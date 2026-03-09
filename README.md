# Telia data engineer internship - home assignment
The aim of this home assignment is to build a simple ETL(Extract, Transform, Load) pipeline in Python.<br>
The pipeline collects the last 30 days of weather data for European capital cities, cleans and transforms the data, and stores it in a local SQLite database. Analytical SQL views are then created on top of the cleaned dataset.

### Data sources
The pipeline gets data from two public APIs:
* **RestCountries API** - fetch European countries, their capitals, and capital coordinates.
* **Open-Meteo Archive API** - retrieve historical weather data for the past 30 days (max/min
temperature, precipitation sum, max windspeed (km/h) and sunshine duration).

### Pipeline structure
**Extract**
* Fetch European countries and their capital coordinates from the RestCountries API.
* Retrieve the last 30 days of weather data for each capital from Open-Meteo.
* Combine country and weather data.

**Transform**
* Handle missing values.
* Convert sunshine duration from seconds to minutes.
* Convert date strings to datetime format.
* Sort the dataset by capital and date.

**Load**
* Store the data in a SQLite database (`weather_data.db`)
* Create separate tables for raw and cleaned data.

### Analytical Views
Three SQL views are created on top of the cleaned dataset:

**v_capitals_ranked_by_avg_temperature**<br>
Ranks capitals by their average temperature over the last 30 days.

**v_countries_with_the_most_rainfall**<br>
Shows countries ranked by total rainfall.

**v_30_day_summary**<br>
Provides a 30-day summary per country including temperature statistics, total rainfall, wind speed averages and sunshine duration.

### How to run
Run the pipeline:<br>
`python code.py`<br>
This will create the SQLite database `weather_data.db` containing the raw and cleaned tables and the analytical views.
