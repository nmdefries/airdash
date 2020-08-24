# -*- coding: utf-8 -*-


"""
TODO:
    - Feature to add past data from https://www.purpleair.com/sensorlist or ThingSpeak API
    - Display hourly forecast
"""

# Running app and building webpage.
import dash
import dash_core_components as dcc
import dash_html_components as html
from flask import Flask
from flask import request

# Making plots and handling data.
import plotly.graph_objects as go  # More complex plotly graphs
import pandas as pd
from requests import get  # Make get requests
import json  # Decode jsons
import page_helper as ph  # Functions to fetch data and build plots

# Managing database.
import psycopg2
import psycopg2.extras
import database_management as dm

import user_settings as us  # JSON header verification, API key, etc.


# Initializing the app and webpage.
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = 'PurpleAir Monitoring'

server = app.server


# Get read-only DB connection for fetching data.
conn = psycopg2.connect(
    us.databaseUrl, cursor_factory=psycopg2.extras.DictCursor)
conn.set_session(readonly=True)


# Get read and write DB connection for managing database. Initialize DB object.
writeConn = psycopg2.connect(us.databaseUrl)
db = dm.AirDatabase(writeConn)


# Add incoming data to DB.
@server.route('/sensordata', methods=['POST'])
def insert_data():
    if not db:
        raise Exception('db object not defined')

    if us.header_key and request.headers.get('X-Purpleair') == us.header_key:
        db.insert_sensor_row(request.json)
    elif not us.header_key:
        db.insert_sensor_row(request.json)

    if us.loadHistoricalData:
        # Add all historical data to DB.
        db.load_historal_data()

    if us.openWeatherApiKey:
        print('querying weather API')
        # Make get request to OpenWeather API.
        weatherResponse = get("https://api.openweathermap.org/data/2.5/onecall?lat={}&lon={}&appid={}&units=imperial&lang={}".format(
            us.latitude, us.longitude, us.openWeatherApiKey, us.lang))
        print('got weather API response')

        weatherData = json.loads(weatherResponse.content.decode('utf-8'))
        db.insert_weather_row_and_forecasts(weatherData)

    return 'done'



# Laying out the webpage.
forecastDisplaySettings = []

if us.showDailyForecast:
    forecastDisplaySettings.append('daily')
if us.showHourlyForecast:
    forecastDisplaySettings.append('hourly')


app.layout = html.Div(children=[

    html.Div([
        html.Div([
            html.Label('Select a date range to display:'
                       )], className="three columns"),
        html.Div([
            dcc.Dropdown(
                id='standard-date-picker',
                options=[
                    {'label': '1 day', 'value': '1 day'},
                    {'label': '3 days', 'value': '3 days'},
                    {'label': '1 week', 'value': '1 week'},
                    {'label': '2 weeks', 'value': '2 weeks'},
                    {'label': '1 month', 'value': '1 month'},
                    {'label': '6 months', 'value': '6 months'},
                    {'label': '1 year', 'value': '1 year'},
                    {'label': 'All time', 'value': 'all'},
                    {'label': 'Custom date range', 'value': 'custom'}
                ], value=us.defaultTimeRange
            )], className="three columns"),
        html.Div([
            dcc.DatePickerRange(
                id='custom-date-range-picker',
                start_date_placeholder_text='Select a date',
                end_date_placeholder_text='Select a date',
                disabled=True
            ),
            dcc.Interval(
                id='fetch-interval',
                interval=(2 * 60) * 1000,  # 2 minutes in milliseconds
                n_intervals=0
            )
        ], className="six columns")

    ], className="row"),


    html.Div([
        html.Div('Select forecast to display:', className="three columns"),
        html.Div([
            dcc.Checklist(
                options=[
                    {'label': 'Hourly forecast', 'value': 'hourly'},
                    {'label': 'Daily forecast', 'value': 'daily'}
                ],
                value=forecastDisplaySettings,
                id='forecast-picker'
            )], className="three columns"),
    ], className="row"),

    html.Div(
        html.H3('Forecast', id='forecast-heading'),
        className="row"),

    html.Div([
        html.Div(
            id='daily-forecast-boxes')
    ], className="row"),

    html.Div([
        html.Div(
            id='hourly-forecast-display')
    ], className="row"),

    html.Div([
        html.H3('Temperature')
    ], className="row"),

    # Plot of temperature. Dropdown to toggle between °F and °C.
    html.Div([
        html.Div([
            dcc.Graph(
                id='temp-vs-time',
            )], className="eight columns"),
        html.Div([
            html.Div(
                dcc.Dropdown(
                    id='temp-unit-picker',
                    options=[
                        {'label': '°F', 'value': 'temp_f'},
                        {'label': '°C', 'value': 'temp_c'}
                    ], value='temp_f'
                ), className="row"),
            html.Blockquote(
                id='curr-sensor-temp',
                className="row"),
            html.Blockquote(
                id='curr-outside-temp',
                className="row")
        ], className="three columns", style={'position': 'relative'}),
    ], className="row"),

    html.Div([
        html.H3('Humidity')
    ], className="row"),

    # Plot of humidity.
    html.Div([
        html.Div([
            dcc.Graph(
                id='humid-vs-time',
            )], className="eight columns"),
        html.Div([], className="four columns")
    ], className="row"),

    html.Div([
        html.H3('Air Quality Index')
    ], className="row"),

    # Plot of AQI (both PM 2.5 and 10.0). Multi-select dropdown to toggle between displaying one or both. Text display + color of associated warning message.
    html.Div([
        html.Div([
            dcc.Graph(
                id='aqi-vs-time',
            )], className="eight columns"),
        html.Div([
            html.Div([
                dcc.Dropdown(
                    id='aqi-picker',
                    options=[
                        {'label': 'PM 2.5', 'value': 'pm_2_5_aqi'},
                        {'label': 'PM 10.0', 'value': 'pm_10_0_aqi'}
                    ], value=['pm_2_5_aqi', 'pm_10_0_aqi'], multi=True
                )], className="row"),
            html.Blockquote(id='aqi-warning', className="row")
        ], className="three columns")
    ], className="row"),

])


# Webpage callbacks
# Toggle custom date range picker display setting only when date dropdown menu is set to custom.
@ app.callback(
    dash.dependencies.Output('custom-date-range-picker', 'disabled'),
    [dash.dependencies.Input('standard-date-picker', 'value')])
def displayCustomDateRangePicker(standardDate):
    if standardDate == 'custom':
        return False

    return True


# Regenerate temp vs time graph when inputs are changed.
@ app.callback(
    [dash.dependencies.Output('temp-vs-time', 'figure'),
     dash.dependencies.Output('curr-sensor-temp', 'children'),
     dash.dependencies.Output('curr-outside-temp', 'children')],
    [dash.dependencies.Input('standard-date-picker', 'value'),
     dash.dependencies.Input('custom-date-range-picker', 'start_date'),
     dash.dependencies.Input('custom-date-range-picker', 'end_date'),
     dash.dependencies.Input('temp-unit-picker', 'value'),
     dash.dependencies.Input('fetch-interval', 'n_intervals')])
def updateTempPlot(standardDate, customStart, customEnd, tempUnit, n):
    records = ph.fetchSensorData(conn, tempUnit, standardDate, [
        customStart, customEnd])
    weather = ph.fetchWeatherDataNewTimeRange(conn, tempUnit, standardDate, [
        customStart, customEnd])

    records = ph.correctTemp(records, tempUnit)

    fig = ph.temp_vs_time(records, tempUnit)
    fig.add_trace(go.Scatter(x=weather.ts, y=weather[tempUnit],
                             mode='markers+lines', line={"color": "rgb(175,175,175)"},
                             hovertemplate='%{y:.1f}',
                             name='Official outside'))

    currentRecords = ph.fetchSensorData(conn, tempUnit, '1 day')
    currentWeather = ph.fetchWeatherDataNewTimeRange(conn, tempUnit, '1 day')

    currentRecords = ph.correctTemp(currentRecords, tempUnit)

    try:
        currSensorStatement = 'Current sensor temperature: {:.0f}°'.format(
            currentRecords.iloc[0][tempUnit])
        currWeatherStatement = 'Current outside temperature: {:.1f}°'.format(
            currentWeather.iloc[0][tempUnit])
    except IndexError as e:
        print(e)
        currSensorStatement = 'Current sensor temperature: Unknown'
        currWeatherStatement = 'Current outside temperature: Unknown'

    return fig, currSensorStatement, currWeatherStatement


# Regenerate humidity vs time graph when inputs are changed.
@ app.callback(
    dash.dependencies.Output('humid-vs-time', 'figure'),
    [dash.dependencies.Input('standard-date-picker', 'value'),
     dash.dependencies.Input('custom-date-range-picker', 'start_date'),
     dash.dependencies.Input('custom-date-range-picker', 'end_date'),
     dash.dependencies.Input('fetch-interval', 'n_intervals')])
def updateHumidPlot(standardDate, customStart, customEnd, n):
    records = ph.fetchSensorData(conn, "humidity", standardDate, [
        customStart, customEnd])
    weather = ph.fetchWeatherDataNewTimeRange(conn, "humidity", standardDate, [
        customStart, customEnd])

    records = ph.correctHumid(records)

    fig = ph.humid_vs_time(records)
    fig.add_trace(go.Scatter(x=weather.ts, y=weather.humidity,
                             mode='markers+lines', line={"color": "rgb(175,175,175)"},
                             hovertemplate='%{y}',
                             name='Official outside'))

    return fig


# Regenerate AQI vs time graph when inputs are changed.
@ app.callback(
    [dash.dependencies.Output('aqi-vs-time', 'figure'), dash.dependencies.Output(
        'aqi-warning', 'children'), dash.dependencies.Output('aqi-warning', 'style')],
    [dash.dependencies.Input('standard-date-picker', 'value'),
     dash.dependencies.Input('custom-date-range-picker', 'start_date'),
     dash.dependencies.Input('custom-date-range-picker', 'end_date'),
     dash.dependencies.Input('aqi-picker', 'value'),
     dash.dependencies.Input('fetch-interval', 'n_intervals')])
def updateAqiPlot(standardDate, customStart, customEnd, aqiSpecies, n):
    if len(aqiSpecies) == 0:
        # Default to showing PM 2.5.
        aqiSpecies = ["pm_2_5_aqi"]

    records = ph.fetchSensorData(conn, aqiSpecies, standardDate, [
        customStart, customEnd])

    warningMessage, style = ph.fetchAqiWarningInfo(
        conn,
        aqiSpecies,
        standardDate,
        [customStart, customEnd])

    return ph.aqi_vs_time(records, aqiSpecies), warningMessage, style


# Generate daily forecast display with most recent data.
@ app.callback(
    [dash.dependencies.Output('forecast-heading', 'children'),
     dash.dependencies.Output('daily-forecast-boxes', 'children')],
    [dash.dependencies.Input('forecast-picker', 'value'),
     dash.dependencies.Input('temp-unit-picker', 'value'),
     dash.dependencies.Input('fetch-interval', 'n_intervals')])
def updateDailyForecast(forecastsToDisplay, tempUnit, n):
    if 'daily' not in forecastsToDisplay:
        if 'hourly' not in forecastsToDisplay:
            return [], []
        return 'Forecast', None

    tempSelector = {'temp_f': ['min_f', 'max_f'], 'temp_c': ['min_c', 'max_c']}
    degreeUnit = {'temp_f': '°F', 'temp_c': '°C'}
    columns = ['weather_type_id', 'short_weather_descrip', 'detail_weather_descrip',
               'weather_icon', 'precip_chance', 'uvi'] + tempSelector[tempUnit]

    records = ph.fetchDailyForecastData(conn, columns)

    blockStyle = {
        'backgroundColor': 'rgba(223,231,244,1.0)',
        "width": "15%",
        "margin-left": '0.83333333333%',
        "margin-right": '0.83333333333%',
        "border-radius": 10}
    lineStyle = {
        "margin-left": 15,
        "margin-top": 0,
        "margin-bottom": 0}

    forecastBoxes = []

    # TODO: Not recommended to use iterrows(), though this dataframe is quite small.
    for index, row in records.iterrows():
        if index < 6:

            # Customize weather description by weather type. Weather type codes here: https://openweathermap.org/weather-conditions#Weather-Condition-Codes-2
            if round(row["weather_type_id"], -2) in (300, 700) or row["weather_type_id"] == 800:
                weatherDescription = row["short_weather_descrip"]
            elif round(row["weather_type_id"], -2) == 200 or (round(row["weather_type_id"], -2) == 800 and row["weather_type_id"] != 800):
                weatherDescription = row["detail_weather_descrip"]
            if round(row["weather_type_id"], -2) in (500, 600):
                weatherDescription = row["detail_weather_descrip"]

                # Swap "shower" and following word.
                weatherDescription = weatherDescription.split(' ')

                if 'shower' in weatherDescription:
                    swapIndex = weatherDescription.index('shower')
                    weatherDescription[swapIndex], weatherDescription[swapIndex +
                                                                      1] = weatherDescription[swapIndex + 1], weatherDescription[swapIndex]

                if round(row["weather_type_id"], -2) == 500:
                    # Drop any instances of "intensity"
                    weatherDescription = [
                        item for item in weatherDescription if item != "intensity"]

                weatherDescription = ' '.join(weatherDescription)

            weatherDescription = weatherDescription.capitalize()

            forecastBoxes.append(
                html.Div([
                    html.B([row['ts'].strftime('%B '), row['ts'].day,
                            html.Img(
                                src='http://openweathermap.org/img/wn/{}@2x.png'.format(
                                    row['weather_icon']),
                                style={'height': '25%',
                                       'width': '25%',
                                       'verticalAlign': 'middle'})],
                           style={"margin-left": 5}),
                    html.P([weatherDescription],
                           style=lineStyle),
                    html.P(["Min: ",
                            round(row[tempSelector[tempUnit][0]]),
                            degreeUnit[tempUnit]],
                           style=lineStyle),
                    html.P(["Max: ",
                            round(row[tempSelector[tempUnit][1]]),
                            degreeUnit[tempUnit]],
                           style=lineStyle),
                    html.P(["Chance of rain: ",
                            round(row['precip_chance'] * 100), '%'],
                           style=lineStyle),
                    html.P(["UV Index: ",
                            round(row['uvi'], 0)],
                           style=lineStyle)
                ], style=blockStyle,
                    className="two columns"))

    return 'Forecast', forecastBoxes


# TODO: Generate hourly forecast display.
@ app.callback(
    dash.dependencies.Output('hourly-forecast-display', 'children'),
    [dash.dependencies.Input('forecast-picker', 'value'),
     dash.dependencies.Input('temp-unit-picker', 'value'),
     dash.dependencies.Input('fetch-interval', 'n_intervals')])
def updateHourlyForecast(forecastsToDisplay, tempUnit, n):
    if 'hourly' not in forecastsToDisplay:
        return []

    return 'Hourly forecast display not yet implemented'

    tempSelector = {'temp_f': ['min_f', 'max_f'], 'temp_c': ['min_c', 'max_c']}
    degreeUnit = {'temp_f': '°F', 'temp_c': '°C'}


if __name__ == '__main__':
    app.run_server(debug=True)
