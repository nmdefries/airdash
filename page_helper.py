# -*- coding: utf-8 -*-

import plotly.graph_objects as go  # More complex plotly graphs
import pandas as pd
import psycopg2

import user_settings as us


def fetchSensorData(pool, varName, standardDate=us.defaultTimeRange, customDate=None, queryFields=None, timezone=us.timezone):
    """
    Fetch updated data for a single variable or a list of variables when date range is changed.

    Args:
        varName: str or list of str corresponding to fields in the sensor_data table
        standardDate: str

    Returns:
        pandas dataframe of data fetched
    """
    conn = pool.getconn()
    conn.set_session(readonly=True)
    cur = conn.cursor()

    if isinstance(varName, str):
        varName = [varName]

    names = ['measurement_ts'] + varName

    if not queryFields:
        queryFields = ', '.join(names)
    else:
        if isinstance(queryFields, str):
            queryFields = [queryFields]

        queryFields = ', '.join(['measurement_ts'] + queryFields)

    records = None

    print("getting sensor data from database...")

    # Get data from database within desired time frame.
    if standardDate != 'custom':
        if standardDate == 'all':
            cur.execute(
                "SELECT {} FROM sensor_data ORDER BY measurement_ts DESC ".format(queryFields))
        else:
            cur.execute(
                "SELECT {} FROM sensor_data WHERE measurement_ts >= NOW() - INTERVAL '{}' ORDER BY measurement_ts DESC ".format(queryFields, standardDate))

    else:
        if customDate[0] and customDate[1]:
            cur.execute("SELECT {} FROM sensor_data WHERE measurement_ts >= '{}' and measurement_ts <= '{}' ORDER BY measurement_ts DESC ".format(
                queryFields, customDate[0], customDate[1]))
        else:
            records = pd.DataFrame(columns=names)

    # Format data.
    if not records:
        try:
            records = pd.DataFrame([{name: row[name] for name in names}
                                    for row in cur.fetchall()], columns=names)

            records.measurement_ts = records.measurement_ts.apply(
                lambda ts: ts.tz_convert(timezone))

        except psycopg2.ProgrammingError:
            print('no data in selected timeframe, creating empty dataframe')
            records = pd.DataFrame(columns=names)

    print("got data")

    cur.close()
    pool.putconn(conn)
    return records


def fetchAqiWarningInfo(pool, aqiSpecies=['pm_2_5_aqi', 'pm_10_0_aqi'], standardDate=us.defaultTimeRange, customDate=None):
    varNames = ['rgb', 'description', 'message']

    # AQI warning text and color.
    if "pm_2_5_aqi" in aqiSpecies and "pm_10_0_aqi" not in aqiSpecies:
        warningVars = ['pm_2_5_aqi_rgb as rgb',
                       'pm_2_5_aqi_description as description',
                       'pm_2_5_aqi_message as message']

    elif "pm_2_5_aqi" not in aqiSpecies and "pm_10_0_aqi" in aqiSpecies:
        warningVars = ['pm_10_0_aqi_rgb as rgb',
                       'pm_10_0_aqi_description as description',
                       'pm_10_0_aqi_message as message']

    elif "pm_2_5_aqi" in aqiSpecies and "pm_10_0_aqi" in aqiSpecies:
        warningVars = [
            'CASE WHEN pm_2_5_aqi >= pm_10_0_aqi THEN pm_2_5_aqi_rgb ELSE pm_10_0_aqi_rgb END AS rgb',
            'CASE WHEN pm_2_5_aqi >= pm_10_0_aqi THEN pm_2_5_aqi_description ELSE pm_10_0_aqi_description END AS description',
            'CASE WHEN pm_2_5_aqi >= pm_10_0_aqi THEN pm_2_5_aqi_message ELSE pm_10_0_aqi_message END AS message']

    else:
        warningVars = []
        varNames = []

    try:
        # First (most recent) row of warning info.
        warnings = fetchSensorData(pool,
                                   varNames, standardDate, customDate, warningVars).iloc[0]

        warningMessage = [warnings['description'], '.\r', warnings['message']]
        style = {
            'backgroundColor': warnings['rgb']
        }
    except IndexError:
        warningMessage = ''
        style = {}

    return warningMessage, style


def fetchWeatherDataNewTimeRange(pool, varName, standardDate=us.defaultTimeRange, customDate=None, timezone=us.timezone):
    """
    Fetch updated data for a single variable or a list of variables when date range is changed.

    Args:
        varName: str or list of str corresponding to fields in the weather_data table

    Returns:
        pandas dataframe of data fetched
    """
    conn = pool.getconn()
    conn.set_session(readonly=True)
    cur = conn.cursor()

    if isinstance(varName, str):
        varName = [varName]

    names = ['ts'] + varName
    queryFields = ', '.join(names)

    records = None

    print("getting weather data from database...")

    # Get data from database.
    if standardDate != 'custom':
        if standardDate == 'all':
            cur.execute(
                "SELECT {} FROM weather_data ORDER BY ts DESC ".format(queryFields))
        else:
            cur.execute(
                "SELECT {} FROM weather_data WHERE ts >= NOW() - INTERVAL '{}' ORDER BY ts DESC ".format(queryFields, standardDate))

    else:
        if customDate[0] and customDate[1]:
            cur.execute("SELECT {} FROM weather_data WHERE ts >= '{}' and ts <= '{}' ORDER BY ts DESC ".format(
                queryFields, customDate[0], customDate[1]))
        else:
            records = pd.DataFrame(columns=names)

    # Format data
    if not records:
        try:
            records = pd.DataFrame([{name: row[name] for name in names}
                                    for row in cur.fetchall()], columns=names)
            records.ts = records.ts.apply(
                lambda ts: ts.tz_convert(timezone))

        except psycopg2.ProgrammingError:
            print('no data in selected timeframe, creating empty dataframe')
            records = pd.DataFrame(columns=names)

    print("got data")

    cur.close()
    pool.putconn(conn)
    return records


def fetchForecastData(pool, varName, tableName, timezone=us.timezone):
    """
    Fetch all daily forecast data.

    Args:
        timezone:

    Returns:
        pandas dataframe of data fetched
    """
    conn = pool.getconn()
    conn.set_session(readonly=True)
    cur = conn.cursor()

    if isinstance(varName, str):
        varName = [varName]

    names = ['ts'] + varName
    queryFields = ', '.join(names)

    print("getting weather forecast from database...")

    # Get data from database.
    cur.execute(
        "SELECT {} FROM {} ORDER BY ts ASC ".format(queryFields, tableName))

    # Format data.
    try:
        records = pd.DataFrame([{name: row[name] for name in names}
                                for row in cur.fetchall()], columns=names)
        records.ts = records.ts.apply(
            lambda ts: ts.tz_convert(timezone))

    except psycopg2.ProgrammingError:
        print('no data in selected timeframe, creating empty dataframe')
        records = pd.DataFrame(columns=names)

    print('got data')

    cur.close()
    pool.putconn(conn)
    return records


def fetchDailyForecastData(pool, varName, timezone=us.timezone):
    return fetchForecastData(pool, varName, "daily_weather_forecast", timezone)


def fetchHourlyForecastData(pool, varName, timezone=us.timezone):
    return fetchForecastData(pool, varName, "hourly_weather_forecast", timezone)


def correctTemp(records, tempUnit):
    # Temp correction: https://de-de.facebook.com/groups/purpleair/permalink/722201454903597/?comment_id=722403448216731
    if tempUnit == "temp_c":
        records[tempUnit] = (
            (((records[tempUnit] * 9 / 5) + 32) - 8) - 32) * 5 / 9
    elif tempUnit == "temp_f":
        records[tempUnit] = records[tempUnit] - 8

    return records


def correctHumid(records):
    # Humidity correction: https://de-de.facebook.com/groups/purpleair/permalink/722201454903597/?comment_id=722403448216731
    records["humidity"] = records["humidity"] + 4

    return records



# Figures to insert.
defaultMargin = dict(b=100, t=0, r=0)


def temp_vs_time(records, species="temp_f", margin=defaultMargin):
    newTempLabel = {
        "temp_c": "Temperature [°C]", "temp_f": "Temperature [°F]"}[species]

    if records.empty:
        # Make empty/blank plot.
        records = pd.DataFrame(columns=["measurement_ts", "value"])
        species = "value"

    fig = go.Figure()

    fig.add_trace(go.Scattergl(x=records["measurement_ts"],
                               y=records[species],
                               mode='markers+lines',
                               hovertemplate='%{y:.0f}',
                               name='Sensor'))

    fig.update_layout(margin=margin,
                      hovermode="x",
                      legend=dict(
                          yanchor="top",
                          y=0.99,
                          xanchor="left",
                          x=0.01
                      ))
    fig.update_yaxes(title_text=newTempLabel)

    if not records.empty:
        xBounds = [min(records.measurement_ts),
                   max(records.measurement_ts)]
        fig.update_layout(xaxis_range=xBounds)

    return fig


def humid_vs_time(records, margin=defaultMargin):
    if records.empty:
        # Make empty/blank plot.
        records = pd.DataFrame(columns=["measurement_ts", "humidity"])

    fig = go.Figure()

    fig.add_trace(go.Scattergl(x=records["measurement_ts"],
                               y=records["humidity"],
                               mode='markers+lines',
                               hovertemplate='%{y}',
                               name='Sensor'))

    fig.update_layout(margin=margin,
                      hovermode="x",
                      legend=dict(
                          yanchor="top",
                          y=0.99,
                          xanchor="left",
                          x=0.01
                      ))
    fig.update_yaxes(title_text="Relative humidity [%]")

    if not records.empty:
        xBounds = [min(records.measurement_ts),
                   max(records.measurement_ts)]
        fig.update_layout(xaxis_range=xBounds)

    return fig


def aqi_vs_time(records, species=["pm_2_5_aqi", "pm_10_0_aqi"], margin=defaultMargin):
    if isinstance(species, str):
        species = [species]

    # Initialize figure
    fig = go.Figure()

    if not species or records.empty:
        # Make empty records df with correct column names.
        records = pd.DataFrame(columns=["measurement_ts"] + species)

    else:
        xBounds = [min(records.measurement_ts),
                   max(records.measurement_ts)]
        yBound = max(max(item for item in records[aqiType] if item is not None)
                     for aqiType in species)

        # EPA color bands by AQI risk.
        # TODO: pull from csv instead of hard-coding.
        colorCutoffs = [
            [50, 'rgba(0,228,0,0.3)'], [100, 'rgba(255,255,0,0.3)'],
            [150, 'rgba(255,126,0,0.3)'], [200, 'rgba(255,0,0,0.3)'],
            [300, 'rgba(143,63,151,0.3)'], [10000, 'rgba(126,0,35,0.3)']]
        colorList = list((item[1] for item in colorCutoffs))

        # Put AQI color band info into dataframe. Data should span min ts to max ts to get full coloring of plot area.
        colorCutoffs = [
            [bound] + cutoff for bound in xBounds for cutoff in colorCutoffs]
        colorCutoffs = pd.DataFrame(colorCutoffs, columns=[
                                    "measurement_ts", "aqi", "color"])

        # Add color stripe one at a time. Stop at the last AQI color band that includes the max AQI value seen in measured data.
        for index, color in enumerate(colorList):
            x = colorCutoffs.loc[colorCutoffs["color"]
                                 == color]["measurement_ts"]
            y = colorCutoffs.loc[colorCutoffs["color"] == color]["aqi"]

            fig.add_trace(go.Scatter(
                x=x, y=y,
                mode='lines',
                line=dict(width=0),
                fillcolor=color,
                fill='tozeroy' if index == 0 else 'tonexty',
                showlegend=False,
                hovertemplate=None,
                hoverinfo='skip'
            ))

            # Max AQI value within most recently added color band.
            if int(yBound) < y.iloc[0]:
                break

        # Set plot axes ranges.
        if index == len(colorCutoffs) - 1:
            # Cap y range at nearest hundred greater than max measured AQI value.
            fig.update_layout(
                yaxis_range=(0, round(yBound + 100, -2)),
                xaxis_range=xBounds
            )
        else:
            fig.update_layout(
                yaxis_range=(0, y.iloc[0]),
                xaxis_range=xBounds
            )

    # Add measured AQI values.
    aqiLabel = {"pm_2_5_aqi": "PM 2.5", "pm_10_0_aqi": "PM 10.0"}
    aqiColor = {"pm_2_5_aqi": "#636EFA", "pm_10_0_aqi": "#EF553B"}

    # Add measured series one by one.
    for aqiType in species:
        fig.add_trace(go.Scattergl(
            x=records["measurement_ts"], y=records[aqiType],
            mode="markers+lines",
            hovertemplate='%{y}',
            name=aqiLabel[aqiType],
            marker=dict(color=aqiColor[aqiType])
        ))

    fig.update_layout(
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ),
        margin=margin,
        hovermode="x"
    )

    fig.update_yaxes(title_text="AQI")

    return fig
