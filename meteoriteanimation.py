# Project Deliverable 2
# Meteorite Landings Interactive Globe Animation
# Kristina Pettersson

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, lit
import numpy as np
import pandas as pd
import csv
import plotly.graph_objects as go

# Create spark session
ss = SparkSession.builder.appName("MeteoriteAnimation").config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow").config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow").getOrCreate()

# Processing data
df = ss.read.csv("./Meteorite_Landings.csv",header=True,inferSchema=True).toPandas()
dfFiltered = df[ (df['year'].notnull())
                & (df['mass (g)'].notnull())]
dfFiltered = dfFiltered[dfFiltered['year']<=2013] # Remove invalid years (eg. 2101)
dfSorted = dfFiltered.sort_values(by=['year'])
dfSorted = dfSorted.drop(["id","nametype","GeoLocation","States","Counties"], axis=1)
dfSorted['label'] = (
    df['name'] + ": fell " + df['year'].astype(str) +
    "; Mass (g): " + df['mass (g)'].astype(str) +
    "; Class: " + df['recclass'])

yearList = dfSorted.get('year').to_numpy()
minYear = int(yearList[0])
maxYear = int(yearList[-1])

frames = [] # List of animation frames (all points up to year i)
steps = [] # Steps in year slider
frameYears = [] # Track years with data
for i in range(minYear, maxYear + 1):
    if i > minYear:
        prevLat = list(frames[-1].data[0]['lat'])
        prevLon = list(frames[-1].data[0]['lon'])
        prevLabels = list(frames[-1].data[0]['text'])
    else:
        prevLat = []
        prevLon = []
        prevLabels = []

    yearData = dfSorted[dfSorted['year'] == i]
    newLat = yearData.get('reclat').tolist()
    newLon = yearData.get('reclong').tolist()
    newLabels = yearData.get('label').tolist()

    if newLat or newLon:
        frame = go.Frame(
            data=[go.Scattergeo(
                lat=prevLat + newLat,
                lon=prevLon + newLon,
                text=prevLabels + newLabels,
                mode='markers',
                marker=dict(size=5, color='red'))],
            name=str(i)
        )
        frames.append(frame)
        frameYears.append(i)

        step = {
            'method': 'animate',
            'label': i,
            'value': i,
            'args': [[i], {'frame': {'duration': 10, 'redraw': True},
                'mode': 'immediate'}
            ],
        }
        steps.append(step)

fig = go.Figure(frames=frames)

# Initial point
initial_year = frameYears[0]
fig.add_trace(go.Scattergeo(
    lat=dfSorted[dfSorted['year'] <= initial_year]['reclat'],
    lon=dfSorted[dfSorted['year'] <= initial_year]['reclong'],
    mode='markers',
    marker=dict(size=5, color='red'))
)

# Set figure layout
fig.update_layout(
    geo=dict(
        projection=dict(
            type='orthographic',
            scale=1
        ),
        fitbounds="locations",
        lonaxis=dict(range=[-180, 180]),
        lataxis=dict(range=[-90, 90]),
        showocean=True,
        framecolor="#000000",
        landcolor="#61a644",
        lakecolor="#1f7e94",
        oceancolor="#154869",
        showframe=True,
        showcountries=True,
        center=dict(lat=0, lon=0),
        domain=dict(x=[0,1],y=[0,1])
    ),
    
    # Year slider
    sliders = [{
        'active': 0,
        'yanchor': 'top',
        'xanchor': 'left',
        'currentvalue': {
            'font': {'size': 20},
            'prefix': 'Year: ',
            'visible': True,
            'xanchor': 'right'
        },
        'transition': {'duration': 300, 'easing': 'cubic-in-out'},
        'pad': {'b': 10, 't': 50},
        'len': 0.9,
        'x': 0.1,
        'y': 0,
        'steps': steps
    }],
    
    margin = {"b":0, "l":0, "r":0, "t":0},
    autosize = True
)

# Save interactive globe animation
fig.write_html("animation.html")
