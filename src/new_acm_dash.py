#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 23 16:09:30 2021

@author: dbalas
"""
import pandas as pd
import plotly.express as px  # (version 4.7.0)
#import plotly.graph_objects as go

import dash  # (version 1.12.0) pip install dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

app = dash.Dash(__name__)
# ------------------------------------------------------------------------------
# Import and clean data
community_metrics = pd.read_csv('../data/network/affiliation_community_metrics.csv')
community_metrics.head()
# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([

    html.H1("Communities of Doctors Based on Affliation Network demo", style={'text-align': 'center'}),
    
    html.Div(["Select Minimum Total Doctors per Community: ",
        dcc.Input(id='doctor_min',
                type='number',
                value=0,
                debounce=False,
                min=0)]),

    html.Div(id='output_container', children=[]),
    html.Br(),


    
    # html.Div(id='doctor_min_container', children=[]),
    # html.Br(),

    dcc.Graph(id='acm_map', figure={})

])

# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
@app.callback(
    [Output(component_id='output_container', component_property='children'),
     Output(component_id='acm_map', component_property='figure')],
    [Input(component_id='doctor_min', component_property='value')]
)

def update_graph(doc_min):
    print(doc_min)
    print(type(doc_min))

    container = "Minimum total doctors per community: {}".format(doc_min)

    #### Copy dataframe for filtering
    filtered = community_metrics.copy()
    filtered = filtered[filtered['total_doctors_in_community']>=doc_min]
    
    # Plotly Express
    fig = px.scatter_geo(filtered, lon="long", lat='lat',
                     size="total_orgs_in_community",
                     color = 'total_shared_doctors_in_community',
                     hover_name='community_id',
                     hover_data={'total_orgs_in_community':True,
                                 'total_doctors_in_community':True},
                     size_max=15,
                     labels=dict(trelegy_share_decile="total_shared_doctors_in_community")
                     )
    fig.update_layout(
            title = 'Community visualization',
            geo_scope='usa',
        )
    return container, fig

###### Run app on specified port
if __name__ == '__main__':
    app.run_server(debug=True, port=8559)