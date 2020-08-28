
"""
Tools for managing PostgreSQL database containing air sensor data.
"""


import psycopg2  # Manipulating PostgreSQL.
import aqi  # Calculating AQI.
from datetime import datetime as dt


def getCarefullyFromDict(d, key):
    if d.__contains__(key):
        return d[key]
    else:
        return None


class AirDatabase(object):
    """
    Initializes and manipulates PostgreSQL database.
    """

    def __init__(self, connection):
        """
        Initialize empty database or establish connection to database of the same name.
        """
        self.conn = connection
        self.cur = self.conn.cursor()

        print('got cursor')

        try:
            self.cur.execute("CREATE TABLE sensor_data ( "
                             # Metadata
                             "id numeric "
                             ", sensor_id text "
                             ", place text "
                             ", version text "
                             ", hardware_version text "
                             ", uptime_s numeric CHECK (uptime_s >= 0) "
                             ", rssi_dbm numeric "
                             # Implied UNIQUE and NOT NULL constraint
                             ", measurement_ts timestamptz PRIMARY KEY "

                             # Environment data
                             ", temp_f numeric "
                             ", temp_c numeric "
                             ", humidity numeric CHECK (humidity >= 0 AND humidity <= 100) "
                             ", dewpoint_f numeric CHECK (dewpoint_f <= temp_f) "
                             ", pressure_mbar numeric "

                             # Air data
                             ", pm_2_5_aqi numeric "
                             ", pm_2_5_aqi_rgb text "
                             ", pm_2_5_aqi_description text "
                             ", pm_2_5_aqi_message text "

                             ", pm_10_0_aqi numeric "
                             ", pm_10_0_aqi_rgb text "
                             ", pm_10_0_aqi_description text "
                             ", pm_10_0_aqi_message text "

                             ", pm_1_0_um_m3 numeric "
                             ", pm_2_5_um_m3 numeric "
                             ", pm_10_0_um_m3 numeric "

                             ", p_0_3_count_dl numeric "
                             ", p_0_5_count_dl numeric "
                             ", p_1_0_count_dl numeric "
                             ", p_2_5_count_dl numeric "
                             ", p_5_0_count_dl numeric "
                             ", p_10_0_count_dl numeric "
                             ") ")

        except psycopg2.ProgrammingError as e:
            # Table already exists. Roll back command.
            print(e)
            self.conn.rollback()
        else:
            print('created sensor_data table')

        # Create table of outside weather data.
        try:
            self.cur.execute("CREATE TABLE weather_data ("
                             "id SERIAL " # Auto-incrementing
                             ", ts timestamptz PRIMARY KEY " # Implied UNIQUE and NOT NULL constraint
                             ", timezone text "
                             ", ts_offset numeric "

                             # Environment data
                             ", temp_f numeric "
                             ", temp_c numeric "
                             ", temp_feels_like_f numeric "
                             ", temp_feels_like_c numeric "
                             ", humidity numeric CHECK (humidity >= 0 AND humidity <= 100) "
                             ", dewpoint_f numeric CHECK (dewpoint_f <= temp_f) "
                             ", pressure_mbar numeric "

                             ")")

        except psycopg2.ProgrammingError as e:
            # Table already exists. Roll back command.
            print(e)
            self.conn.rollback()
        else:
            print('created weather_data table')

        # Create table of daily weather forecast.
        try:
            self.cur.execute("CREATE TABLE daily_weather_forecast ("
                             "id SERIAL " # Auto-incrementing
                             ", ts timestamptz PRIMARY KEY " # Implied UNIQUE and NOT NULL constraint
                             ", timezone text "
                             ", ts_offset numeric "

                             # Environment data
                             ", min_f numeric "
                             ", min_c numeric "
                             ", max_f numeric "
                             ", max_c numeric "
                             ", weather_type_id numeric "
                             ", short_weather_descrip text "
                             ", detail_weather_descrip text "
                             ", weather_icon text "
                             ", precip_chance numeric "
                             ", uvi numeric "

                             ")")

        except psycopg2.ProgrammingError as e:
            # Table already exists. Roll back command.
            print(e)
            self.conn.rollback()
        else:
            print('created daily_weather_forecast table')

        # Create table of hourly weather forecast.
        try:
            self.cur.execute("CREATE TABLE hourly_weather_forecast ("
                             "id SERIAL " # Auto-incrementing
                             ", ts timestamptz PRIMARY KEY " # Implied UNIQUE and NOT NULL constraint
                             ", timezone text "
                             ", ts_offset numeric "

                             # Environment data
                             ", temp_f numeric "
                             ", temp_c numeric "
                             ", humidity numeric CHECK (humidity >= 0 AND humidity <= 100) "
                             ", dewpoint_f numeric CHECK (dewpoint_f <= temp_f) "
                             ", weather_type_id numeric "
                             ", short_weather_descrip text "
                             ", detail_weather_descrip text "
                             ", weather_icon text "
                             ", precip_chance numeric "

                             ")")

        except psycopg2.ProgrammingError as e:
            # Table already exists. Roll back command.
            print(e)
            self.conn.rollback()
        else:
            print('created hourly_weather_forecast table')

    def insert_sensor_row(self, data):
        """
        Add a row of sensor data to the air database.

        Args:
            data: sensor data in json/dictionary format.

        Returns:
            NULL
        """
        data["temp_c"] = (data["current_temp_f"] - 32) * (5 / 9)

        breakpoints = aqi.loadAqiBreakpoints()
        descriptions = aqi.loadAqiDescriptiveInfo()

        data["pm_10_0_aqi"] = aqi.getAqi(
            data['pm10_0_cf_1'], breakpoints)
        data["pm_10_0_aqi_rgb"] = aqi.aqiColor(
            data['pm_10_0_aqi'], descriptions)
        data['pm_10_0_aqi_description'] = aqi.aqiDescription(
            data['pm_10_0_aqi'], descriptions)
        data['pm_10_0_aqi_message'] = aqi.aqiMessage(
            data['pm_10_0_aqi'], descriptions)

        data['p25aqic'] = aqi.aqiColor(data['pm2.5_aqi'], descriptions)
        data['pm_2_5_aqi_description'] = aqi.aqiDescription(
            data['pm2.5_aqi'], descriptions)
        data['pm_2_5_aqi_message'] = aqi.aqiMessage(
            data['pm2.5_aqi'], descriptions)

        try:
            print('inserting new obs into sensor_data table...')

            self.cur.execute("INSERT INTO sensor_data (id, sensor_id, place"
                             ", version, hardware_version, uptime_s, rssi_dbm"
                             ", measurement_ts, temp_f, temp_c, humidity"
                             ", dewpoint_f, pressure_mbar, pm_2_5_aqi"
                             ", pm_2_5_aqi_rgb, pm_2_5_aqi_description"
                             ", pm_2_5_aqi_message, pm_10_0_aqi"
                             ", pm_10_0_aqi_rgb, pm_10_0_aqi_description "
                             ", pm_10_0_aqi_message"
                             ", pm_1_0_um_m3, pm_2_5_um_m3, pm_10_0_um_m3"
                             ", p_0_3_count_dl, p_0_5_count_dl, p_1_0_count_dl"
                             ", p_2_5_count_dl, p_5_0_count_dl, p_10_0_count_dl"
                             ") "
                             "VALUES (%(Id)s, %(SensorId)s, %(place)s"
                             ", %(version)s, %(hardwareversion)s, %(uptime)s, %(rssi)s"
                             ", %(DateTime)s, %(current_temp_f)s, %(temp_c)s, %(current_humidity)s"
                             ", %(current_dewpoint_f)s, %(pressure)s, %(pm2.5_aqi)s"
                             ", %(p25aqic)s, %(pm_2_5_aqi_description)s, %(pm_2_5_aqi_message)s"
                             ", %(pm_10_0_aqi)s, %(pm_10_0_aqi_rgb)s"
                             ", %(pm_10_0_aqi_description)s, %(pm_10_0_aqi_message)s"
                             ", %(pm1_0_cf_1)s, %(pm2_5_cf_1)s, %(pm10_0_cf_1)s"
                             ", %(p_0_3_um)s, %(p_0_5_um)s, %(p_1_0_um)s"
                             ", %(p_2_5_um)s, %(p_5_0_um)s, %(p_10_0_um)s)",
                             data)
        except (psycopg2.ProgrammingError, psycopg2.errors.UniqueViolation, KeyError) as e:
            print('failed: ', e)
            self.conn.rollback()
        else:
            self.conn.commit()  # Make database changes persistent.

    def table_exists(self, table_name):
        """
        Check if the named table exists in the database.

        Args:
            table_name: str

        Returns:
            NULL
        """
        try:
            self.cur.execute(
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = {} ".format(table_name))
        except psycopg2.ProgrammingError as e:
            print(e)
            self.conn.rollback()
        else:
            print('checked if {} exists'.format(table_name))
            return self.cur.rowcount != 0

    def del_row(self, data):
        """
        Remove an observation from the air database.

        Args:
            data: sensor data in json/dictionary format.

        Returns:
            NULL
        """
        try:
            self.cur.execute("DELETE FROM sensor_data "
                             "WHERE id = %(Id)s and measurement_ts = %(DateTime)s",
                             data)
        except psycopg2.ProgrammingError as e:
            print(e)
            self.conn.rollback()
        else:
            print('deleted row from sensor_data table')

    def del_all(self, table_name):
        """
        Remove all data from the named table.

        Args:
            table_name: str

        Returns:
            NULL
        """
        try:
            self.cur.execute(
                "IF (EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = {} ) ) THEN DELETE FROM {} ; END IF; ".format(table_name, table_name))
        except psycopg2.ProgrammingError as e:
            print(e)
            self.conn.rollback()
        else:
            print('removed all rows from table {}'.format(table_name))

    def del_table(self, table_name):
        """
        Delete the named table.

        Args:
            table_name: str

        Returns:
            NULL
        """
        try:
            self.cur.execute("DROP TABLE IF EXISTS {} ".format(
                table_name))
        except psycopg2.ProgrammingError as e:
            print(e)
            self.conn.rollback()
        else:
            print('deleted table {}'.format(table_name))

    def close_comms(self):
        """
        Close the DB connection and cursor.

        Args:
            None

        Returns:
            NULL
        """
        self.cur.close()
        self.conn.close()
        print('connection and cursor closed')

    def insert_weather_row_and_forecasts(self, data):
        """
        Add a row of sensor data to the air database.

        Args:
            data: sensor data in json/dictionary format.

        Returns:
            NULL
        """
        cleanData = dict()

        cleanData["time"] = dt.fromtimestamp(data["current"]["dt"])

        cleanData["timezone_offset"] = getCarefullyFromDict(
            data, "timezone_offset")
        cleanData["timezone"] = getCarefullyFromDict(data, "timezone")

        cleanData["temp_f"] = data["current"]["temp"]
        cleanData["temp_c"] = (data["current"]["temp"] - 32) * (5 / 9)
        cleanData["temp_feels_like_f"] = data["current"]["feels_like"]
        cleanData["temp_feels_like_c"] = (
            data["current"]["feels_like"] - 32) * (5 / 9)

        cleanData["humidity"] = data["current"]["humidity"]
        cleanData["dewpoint_f"] = data["current"]["dew_point"]
        cleanData["pressure_mbar"] = data["current"]["pressure"]

        try:
            print('inserting new obs into weather_data table...')
            self.cur.execute("INSERT INTO weather_data ( "
                             "ts, timezone, ts_offset "
                             ", temp_f, temp_c, temp_feels_like_f "
                             ", temp_feels_like_c, humidity "
                             ", dewpoint_f, pressure_mbar "
                             ") "
                             "VALUES ( "
                             "%(time)s, %(timezone)s, %(timezone_offset)s "
                             ", %(temp_f)s, %(temp_c)s, %(temp_feels_like_f)s "
                             ", %(temp_feels_like_c)s, %(humidity)s "
                             ", %(dewpoint_f)s, %(pressure_mbar)s "
                             ") ",
                             cleanData)

            # Add forecast data.
            self.insert_daily_forecast_row(data)
            self.insert_hourly_forecast_row(data)

        except (psycopg2.ProgrammingError, psycopg2.errors.UniqueViolation, KeyError) as e:
            print('failed: ', e)
            self.conn.rollback()
        else:
            self.conn.commit()

    def insert_daily_forecast_row(self, data):
        """
        Add a row of forecast data to the daily forecast table.

        Args:
            data: forecast data in json/dictionary format.

        Returns:
            NULL
        """
        # Remove any existing data in table.
        try:
            self.cur.execute("DELETE FROM daily_weather_forecast ")
        except (psycopg2.ProgrammingError, psycopg2.errors.UniqueViolation) as e:
            print(e)
            self.conn.rollback()
        else:
            print('deleted all existing rows in daily_weather_forecast table')

        cleanData = dict()

        cleanData["timezone_offset"] = getCarefullyFromDict(
            data, "timezone_offset")
        cleanData["timezone"] = getCarefullyFromDict(data, "timezone")

        # List of dicts, one per day.
        dataList = getCarefullyFromDict(data, "daily")

        try:
            print('inserting new obs into daily_weather_forecast table...')

            # Add new data row by row.
            for data in dataList:
                cleanData["time"] = dt.fromtimestamp(data["dt"])

                cleanData["min_f"] = round(data["temp"]["min"], 2)
                cleanData["min_c"] = round(
                    (data["temp"]["min"] - 32) * (5 / 9), 2)
                cleanData["max_f"] = round(data["temp"]["max"], 2)
                cleanData["max_c"] = round(
                    (data["temp"]["max"] - 32) * (5 / 9), 2)

                cleanData["weather_type_id"] = data["weather"][0]["id"]
                cleanData["short_weather_descrip"] = data["weather"][0]["main"]
                cleanData["detail_weather_descrip"] = data["weather"][0]["description"]
                cleanData["weather_icon"] = data["weather"][0]["icon"]

                cleanData["precip_chance"] = getCarefullyFromDict(data, "pop")
                cleanData["uvi"] = getCarefullyFromDict(data, "uvi")

                self.cur.execute("INSERT INTO daily_weather_forecast ( "
                                 "ts, timezone, ts_offset "
                                 ", min_f, min_c, max_f "
                                 ", max_c, short_weather_descrip "
                                 ", detail_weather_descrip, weather_icon "
                                 ", precip_chance, uvi, weather_type_id "
                                 ") "
                                 "VALUES ( "
                                 "%(time)s, %(timezone)s, %(timezone_offset)s "
                                 ", %(min_f)s, %(min_c)s, %(max_f)s "
                                 ", %(max_c)s, %(short_weather_descrip)s "
                                 ", %(detail_weather_descrip)s, %(weather_icon)s "
                                 ",  %(precip_chance)s, %(uvi)s, %(weather_type_id)s "
                                 ") ",
                                 cleanData)

        except (psycopg2.ProgrammingError, psycopg2.errors.UniqueViolation, KeyError) as e:
            print('failed: ', e)
            self.conn.rollback()
        else:
            self.conn.commit()

    def insert_hourly_forecast_row(self, data):
        """
        Add a row of forecast data to the hourly forecast table.

        Args:
            data: forecast data in json/dictionary format.

        Returns:
            NULL
        """
        # Remove any existing data in table.
        try:
            self.cur.execute("DELETE FROM hourly_weather_forecast ")
        except (psycopg2.ProgrammingError, psycopg2.errors.UniqueViolation) as e:
            print(e)
            self.conn.rollback()
        else:
            print('deleted all existing rows in hourly_weather_forecast table')

        cleanData = dict()

        cleanData["timezone_offset"] = getCarefullyFromDict(
            data, "timezone_offset")
        cleanData["timezone"] = getCarefullyFromDict(data, "timezone")

        # List of dicts, one per hour of the next several days.
        dataList = getCarefullyFromDict(data, "hourly")

        try:
            print('inserting new obs into hourly_weather_forecast table...')

            # Add new data row by row.
            for data in dataList:
                cleanData["time"] = dt.fromtimestamp(data["dt"])

                cleanData["temp_f"] = round(data["temp"], 2)
                cleanData["temp_c"] = round((data["temp"] - 32) * (5 / 9), 2)
                cleanData["humidity"] = getCarefullyFromDict(data, "humidity")
                cleanData["dewpoint_f"] = getCarefullyFromDict(
                    data, "dew_point")

                cleanData["weather_type_id"] = data["weather"][0]["id"]
                cleanData["short_weather_descrip"] = data["weather"][0]["main"]
                cleanData["detail_weather_descrip"] = data["weather"][0]["description"]
                cleanData["weather_icon"] = data["weather"][0]["icon"]

                cleanData["precip_chance"] = getCarefullyFromDict(data, "pop")

                self.cur.execute("INSERT INTO hourly_weather_forecast ( "
                                 "ts, timezone, ts_offset "
                                 ", temp_f, temp_c, humidity "
                                 ", dewpoint_f, short_weather_descrip "
                                 ", detail_weather_descrip, weather_icon "
                                 ", precip_chance, weather_type_id "
                                 ") "
                                 "VALUES ( "
                                 "%(time)s, %(timezone)s, %(timezone_offset)s "
                                 ", %(temp_f)s, %(temp_c)s, %(humidity)s "
                                 ", %(dewpoint_f)s, %(short_weather_descrip)s "
                                 ", %(detail_weather_descrip)s, %(weather_icon)s "
                                 ",  %(precip_chance)s, %(weather_type_id)s "
                                 ") ",
                                 cleanData)

        except (psycopg2.ProgrammingError, psycopg2.errors.UniqueViolation, KeyError) as e:
            print('failed: ', e)
            self.conn.rollback()
        else:
            self.conn.commit()

    def load_historal_data(self):
        # TODO: Add all historical data to DB.
        print('historical data load not yet implemented')
        # self.conn.commit()
