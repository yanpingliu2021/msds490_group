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

import dash_cytoscape as cyto #network visualization

app = dash.Dash(__name__)
# ------------------------------------------------------------------------------
# Import and clean data

#### Community Metrics ####
community_metrics = pd.read_csv('../data/network/affiliation_community_metrics.csv')
community_metrics.head()

#### Network Topology ####
# Load data
network_data = pd.read_csv('../data/network/affiliation_network_topology.csv')\
    .query('community_id == 1')\
    .filter(items = ['source_org_pac_id', 'target_org_pac_id'])


# We select the first 750 edges and associated nodes for an easier visualization
edges = network_data
nodes = set()

cy_edges = []
cy_nodes = []

for idx, network_edge in edges.iterrows():
    source = str(network_edge['source_org_pac_id'])
    target = str(network_edge['target_org_pac_id'])

    if source not in nodes:
        nodes.add(source)
        cy_nodes.append({"data": {"id": source, "label": "Org #" + source[-5:]}})
    if target not in nodes:
        nodes.add(target)
        cy_nodes.append({"data": {"id": target, "label": "Org #" + target[-5:]}})

    cy_edges.append({
        'data': {
            'source': source,
            'target': target
        }
    })

default_stylesheet = [
    {
        "selector": 'node',
        'style': {
            "opacity": 0.65,
        }
    },
    {
        "selector": 'edge',
        'style': {
            "curve-style": "bezier",
            "opacity": 0.65
        }
    },
]

styles = {
    'json-output': {
        'overflow-y': 'scroll',
        'height': 'calc(50% - 25px)',
        'border': 'thin lightgrey solid'
    },
    'tab': {
        'height': 'calc(98vh - 105px)'
    }
}
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

    dcc.Graph(id='acm_map', figure={}),
    html.Br(),

    html.Div(cyto.Cytoscape(
            id='cytoscape',
            layout={'name': 'circle'},
            elements=cy_edges + cy_nodes,
            style={
                'height': '95vh',
                'width': '100%'
            }
        )
    )

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
