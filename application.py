# Comment structure:

#A======Comment applies to all code between this and the next #A======. A = one or more alphabetic characters
#-------Comment applies to the following block of code
#N Comment applies to the following N lines of code (N lines do not include comments)
# Comment applies to the following line of code
#TODO - task yet to be completed

from flask import Flask, request, render_template, session
import pandas as pd
import requests
import datetime
import re
import pvlib
import pvlib.forecast
from pvlib.forecast import NAM
import ctypes
import pout

# Used for UTCI, but poor performance with mean radiant temp approximation from the globe temp returned by liljegren
# Must cite if utilized
# import pythermalcomfort

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':

        #------get longitude, latitude, and location from geocode.xyz------
        location = request.form.get('location')
        response = requests.get('https://geocode.xyz', params={'locate': location, 'json': 1, 'region': 'US'})
        data = response.json()
        latitude = data['latt']
        longitude = data['longt']
        if 'city' in data['standard']:
            location = data['standard']['city']
            if 'region' in data['standard']:
                location = location + ', ' + data['standard']['region']
        elif 'zip' in data['standard']:
            location = data['standard']['zip']
            if 'region' in data['standard']:
                location = location + ', ' + data['standard']['region']
        # status = response.status_code
        # print(f'Geocode API status: {status}')

        #------ get data from National Weather Service ------
        response = requests.get(f'https://api.weather.gov/points/{latitude},{longitude}')
        data = response.json()
        timezone = data['properties']['timeZone']
        weather_data = data['properties']['forecastGridData']
        response = requests.get(weather_data)
        data = response.json()

        #------ change latitude and longitude to floats for future use ------
        latitude = float(latitude)
        longitude = float(longitude)

        #a======Build dataframe with weather data and calculated approximations======
        # empty dataframe for accumulating data and calculations
        forecast = pd.DataFrame()

        properties_to_fetch = ['temperature', 'relativeHumidity', 'windSpeed', 'pressure']
        for property in properties_to_fetch:

            #------ create dataframe from json for a property
            df = pd.json_normalize(
                data=data,
                record_path=['properties', property, 'values']
            )
            df.rename(columns={'value': property}, inplace=True)
            df[['time', 'period']] = df.apply(lambda row: row['validTime'].split(r'/'), axis=1, result_type='expand')
            df.drop('validTime', axis=1, inplace=True)
            df['time'] = pd.to_datetime(df['time'])

            #b====== make sure there is a row for the final hour
            #------ calc period
            last_period_hrs = int(re.search(r'\d+(?=H)', df.iloc[-1]['period']).group())
            last_period_days = re.search(r'\d+(?=D)', df.iloc[-1]['period'])
            #2 must check for a match before accessing match, otherwise error occurs
            if last_period_days:
                last_period_days = int(last_period_days.group())
                last_period = last_period_hrs + 24 * last_period_days
            else: last_period = last_period_hrs

            #------ add final_hr row if necessary
            if last_period > 1:
                last_hr = df.iloc[-1]['time']
                final_hr = last_hr + pd.Timedelta(hours=last_period - 1)
                df = df.append({'time':final_hr, property:df.iloc[-1][property]}, ignore_index=True)

            # remove period column
            df.drop('period', axis=1, inplace=True)
            #b=========================================================

            #------ set datetimes as index and resample to hourly
            df.set_index('time', inplace=True)
            df = df.resample('H').pad()

            # merge with forecast dataframe
            forecast = pd.merge(forecast, df, left_index=True, right_index=True, how='outer')

        #------Correct units------
        # change windSpeed from kph to mps
        forecast['windSpeed'] = forecast['windSpeed'] / 3.6
        #2 make minimum windSpeed .5 mps due to effects of body movement
        filt = (forecast['windSpeed'] < .5)
        forecast.loc[filt, 'windSpeed'] = .5
        # change pressure from inHg to mbar
        forecast['pressure'] = forecast['pressure'] * 33.863886667

        # erase rows with missing values
        forecast.dropna(inplace=True)

        #------Get irradiance data------
        # get irradiance dataframe
        irradiance_df = pvlib.location.Location(latitude, longitude).get_clearsky(forecast.index, model='haurwitz')
        # merge irradiance df to forecast df
        forecast = pd.concat([forecast, irradiance_df], axis=1)

        #------Get cloudy irradiance data------
        model = NAM()
        start = forecast.index[1]
        end = forecast.index[-1]
        raw_data = model.get_data(latitude, longitude, start, end)
        cloud_data = model.rename(raw_data)
        # Resample because model resolutions changes to 3hr after some time
        cloud_data = cloud_data.resample('H').interpolate()
        irrads_w_clouds = model.cloud_cover_to_irradiance(cloud_data['total_clouds'], how='clearsky_scaling')
        irrads_w_clouds = irrads_w_clouds.add_prefix('cloudy_')
        # Merge with forecast
        forecast = pd.concat([forecast, irrads_w_clouds], axis=1)

        #c======Set up to use liljegren's C program, feed rows to the program=============
        libc = ctypes.CDLL('./liljegren_c.so')

        liljegren_c = libc.calc_wbgt
        liljegren_c.argtypes =[ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
            ctypes.c_double, ctypes.c_double, ctypes.c_double, ctypes.c_double, ctypes.c_double, ctypes.c_double, ctypes.c_double,
            ctypes.POINTER(ctypes.c_double), ctypes.POINTER(ctypes.c_double), ctypes.POINTER(ctypes.c_double), ctypes.POINTER(ctypes.c_double)]

        #------Function that organizes the needed data before feeding to the liljegren_c function and returning the desired approximations
        def liljegren(row):
            year = row.name.year
            month = row.name.month
            day = row.name.dayofweek
            #2 calc gmt offset
            gmt = pd.Timestamp.now('America/New_York').utcoffset()
            gmt = int(gmt.days * 24 + gmt.seconds / 3600)
            hour = row.name.hour + gmt
            minute = 0
            avg = 60
            lat = latitude
            lon = longitude
            solar = row['ghi']
            solar_cloudy = row['cloudy_ghi']
            pres = row['pressure']
            Tair = row['temperature']
            relhum = row['relativeHumidity']
            speed = row['windSpeed']
            Tg = ctypes.c_double()
            Tg_cloudy = ctypes.c_double()
            Tnwb = ctypes.c_double()
            Tpsy = ctypes.c_double()
            Twbg = ctypes.c_double()
            Twbg_cloudy = ctypes.c_double()
            liljegren_c(year, month, day, hour, minute, gmt, avg, lat, lon, solar, pres, Tair, relhum, speed, Tg, Tnwb, Tpsy, Twbg)
            liljegren_c(year, month, day, hour, minute, gmt, avg, lat, lon, solar_cloudy, pres, Tair, relhum, speed, Tg_cloudy, Tnwb, Tpsy, Twbg_cloudy)
            answer = [Twbg.value, Tg.value, Twbg_cloudy.value, Tg_cloudy.value]
            return answer

        # apply function to each row
        forecast[['Twbg', 'Tg', 'Twbg_cloudy', 'Tg_cloudy']] = forecast.apply(liljegren, axis=1, result_type='expand')

        #------Build any remaining necessary final columns for output------
        # Create column with nicely formated times
        forecast['Time'] = forecast.index.tz_convert('America/New_York').strftime('%a %m/%d %I:%M %p')
        # Add final column of rounded values
        forecast['Wet Bulb Globe Temperature (F)'] = (forecast['Twbg'] * 1.8 + 32).round().astype(int)
        # WBGT with cloud cover
        forecast['WBGT (F) Considering Clouds'] = (forecast['Twbg_cloudy'] * 1.8 + 32).round().astype(int)
        # add column of farenheight temp
        forecast['Temperature (F)'] = (forecast['temperature'] * 1.8 + 32).round().astype(int)
        #3 Add cloud cover column
        forecast['Cloud Cover (%)'] = cloud_data['total_clouds'].round()
        # Remove rows with missing values (common when adding cloud cover column)
        forecast.dropna(inplace=True)
        # change cloud cover to int to remove decimal
        forecast['Cloud Cover (%)'] = forecast['Cloud Cover (%)'].astype(int)

        # Appears to raise exception because difference between tdb and tr is too great.
        #   Seems that the mrt approximation is not great, probably due to the different d argument (default = .015).
        #   Returns UTCI values that are obviously too high
        #
        # #------calc UTCI------
        # forecast['mrt'] = forecast.apply(lambda row: pythermalcomfort.psychrometrics.t_mrt(row['Tg'], row['temperature'], row['windSpeed'], d=0.0508, emissivity=.95), axis=1)
        # def utci(row):
        #     try:
        #         return pythermalcomfort.models.utci(tdb=row['temperature'], tr=row['mrt'], v=1, rh=row['relativeHumidity'], units='SI')
        #     except:
        #         return None
        # forecast['UTCI'] = forecast.apply(utci, axis=1)
        # forecast['UTCI_F'] = (forecast['UTCI'] * 1.8 + 32)

        #c=================================================================================
        #a=================================================================================

        #------create html table for insertion into webpage-----
        table = forecast.to_html(columns=['Time', 'Wet Bulb Globe Temperature (F)', 'WBGT (F) Considering Clouds', 'Cloud Cover (%)', 'Temperature (F)'], classes='table table-striped', index=False, justify='left')

        return render_template('forecast.html', location=location, table=table)

    else:
        return render_template('index.html')

# @app.route('/<location>')
# def location():
#     pass