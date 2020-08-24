
"""
    Calculates AQI for various air pollutants (default PM 10.0) using the EPA equation and EPA AQI breakpoints.

    AQI equation and technical documentation: https://www.airnow.gov/sites/default/files/2020-05/aqi-technical-assistance-document-sept2018.pdf
    AQI breakpoints: https://aqs.epa.gov/aqsweb/documents/codetables/aqi_breakpoints.html

    Data provided by the US EPA used under public domain (https://edg.epa.gov/EPA_Data_License.html). 
"""

import pandas as pd
from numbers import Number


def loadAqiBreakpoints(file_name='aqi_breakpoints.csv', pollutant='PM10 Total 0-10um STP', duration_code='7'):
    """
        Gets AQI breakpoint data for calculating AQI of a specific pollutant.

    Args:
        file_name: path to file; str
        pollutant: str; 'PM10 Total 0-10um STP' (default) or 'PM2.5 - Local Conditions'
        duration_code: str; helps uniquely identify a set of breakpoints; indicates length of averaging

    Returns:
        Pandas dataframe
    """

    breakpoints = pd.read_csv(file_name)

    return breakpoints[(breakpoints['Parameter'] == pollutant) & (breakpoints['Duration Code'] == duration_code)]


def loadAqiDescriptiveInfo(file_name='aqi_colors_messages.csv'):
    """
        Gets descriptive data for each AQI level from file. File created using color info from the EPA here: https://www3.epa.gov/airnow/aqi-technical-assistance-document-sept2018.pdf and messages and cutoff info here: https://www.airnow.gov/aqi/aqi-basics/

    Args:
        file_name: path to file; str

    Returns:
        Pandas dataframe
    """
    return pd.read_csv(file_name)


def getAqiDescriptiveFeature(descriptions, feature, aqi):
    """
        Gets specified feature from AQI level descriptive info for specified AQI value.

    Args:
        descriptions: pandas dataframe, as read in from aqi_colors_messages.csv
        feature: str; column name of interest in descriptions
        aqi: numeric; AQI value to choose hazard level with

    Returns:
        Pandas dataframe
    """
    if not aqi:
        return None

    return descriptions[(descriptions['aqi_lo'] <= aqi) & (descriptions['aqi_hi'] > aqi)].iloc[0][feature]


def aqiMessage(aqi, descriptions):
    return getAqiDescriptiveFeature(descriptions, 'message', aqi)


def aqiDescription(aqi, descriptions):
    return getAqiDescriptiveFeature(descriptions, 'description', aqi)


def aqiColor(aqi, descriptions):
    return getAqiDescriptiveFeature(descriptions, 'color', aqi)


def getAqi(pollutantConcentration, breakpoints):
    """
    Calculate AQI for species of interest.

    Args:
        pollutantConcentration: concentration [µg/m3] of pollutant of interest; numeric

    Returns:
        Rounded AQI (None if pollutantConcentration value is invalid)
    """
    if not pollutantConcentration:
        print("pollutantConcentration doesn't exist")
        return None
    if not isinstance(pollutantConcentration, Number):
        print("pollutantConcentration not a number")
        return None
    if pollutantConcentration < 0:
        print("pollutantConcentration < 0")
        return None

    # Round to nearest integer to make compatible with EPA breakpoints.
    pollutantConcentration = round(pollutantConcentration)

    aqiRow = breakpoints[(breakpoints['Low Breakpoint'] <= pollutantConcentration) & (
        breakpoints['High Breakpoint'] >= pollutantConcentration)]

    if not aqiRow.empty:
        return calculateAqiFromConcentration(pollutantConcentration, aqiRow.iloc[0]['High Breakpoint'], aqiRow.iloc[0]['Low Breakpoint'], aqiRow.iloc[0]['High AQI'], aqiRow.iloc[0]['Low AQI'])
    else:
        return None


def calculateAqiFromConcentration(pollutantConcentration, breakpointHi, breakpointLo, aqiHi, aqiLo):
    """
    Calculate AQI using EPA AQI equation.

    Equation from the EPA AirNow AQI documentation: https://www.airnow.gov/sites/default/files/2020-05/aqi-technical-assistance-document-sept2018.pdf

    Args:
        pollutantConcentration: concentration [µg/m3] of pollutant p
        breakpointHi: concentration breakpoint >= Cp
        breakpointLo: concentration breakpoint <= Cp
        aqiHi: AQI value corresponding to BPhi
        aqiLo: AQI value corresponding to BPlo

    Returns:
        Rounded AQI
    """
    return round(((aqiHi - aqiLo) * (pollutantConcentration - breakpointLo) / (breakpointHi - breakpointLo)) + aqiLo)
