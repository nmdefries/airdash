# -*- coding: utf-8 -*-
import os

# Get user settings set as environment variables. All read in as str. Set environment variables in dokku according to http://dokku.viewdocs.io/dokku/configuration/environment-variables/
databaseUrl = os.environ.get('DATABASE_URL')

# Keys. If named item doesn't exist, var = None.
header_key = os.environ.get('HEADER_KEY')
openWeatherApiKey = os.environ.get('OPENWEATHER_API_KEY')

# OpenWeather API settings
timezone = os.environ.get('APP_TIMEZONE')
latitude = os.environ.get('LAT')
longitude = os.environ.get('LONG')
lang = os.environ.get('LANG')

# Display settings
defaultTimeRange = os.environ.get('DEFAULT_TIME_RANGE')
showDailyForecast = os.environ.get('SHOW_DAILY_FORECAST')
showHourlyForecast = os.environ.get('SHOW_HOURLY_FORECAST')

# Other
loadHistoricalData = os.environ.get('LOAD_HISTORICAL_DATA')


# Validate settings.
if not openWeatherApiKey:
    print('no OpenWeather API key provided. Official outside weather info will not be displayed')
if not header_key:
    print("no PurpleAir POST header key provided. Database will have increased vulnerability to insertion attacks from unverified POST sources. Add header key on the PurpleAir 'Modify registration' form at https://www.purpleair.com/register according to https://www.keycdn.com/support/custom-http-headers")


if not timezone:
    raise Exception(
        "Timezone not provided. Please set environment variable APP_TIMEZONE according to the sensor's location and https://pvlib-python.readthedocs.io/en/stable/timetimezones.htm")
if not latitude or not longitude:
    raise Exception(
        "Latitude or longitude not provided. Please set environment variables LAT and LONG to the sensor's location.")
if not lang:
    print('defaulting to weather info in English. See other language options at https://openweathermap.org/api/one-call-api#data')
    lang = 'en'


if not defaultTimeRange:
    print('defaulting to showing 3 days of data')
    defaultTimeRange = '3 days'

if showDailyForecast == 'True':
    showDailyForecast = True
elif showDailyForecast == 'False':
    showDailyForecast = False
else:
    print('defaulting to showing daily forecast')
    showDailyForecast = True

if showHourlyForecast == 'True':
    showHourlyForecast = True
elif showHourlyForecast == 'False':
    showHourlyForecast = False
else:
    print('defaulting to not showing hourly forecast')
    showHourlyForecast = False

if loadHistoricalData == 'True':
    print('loading historical sensor data. Not recommended to leave this setting permanently on due to process intensity. Run during first setup or when you know data has not saved to your database (WiFi down, etc)')
    loadHistoricalData = True
else:
    loadHistoricalData = False
