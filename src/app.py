#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 23 16:09:30 2021

@author: dbalas
updated by: yanping on May 30
"""
import pandas as pd
import plotly.express as px  # (version 4.7.0)
import dash  # (version 1.12.0) pip install dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
from dash.dependencies import Input, Output, State
import dash_cytoscape as cyto #network visualization

app = dash.Dash(__name__)

stylez = {
    'pre': {
        'border': 'thin lightgrey solid',
        'overflowX': 'scroll'
    }
}


# ------------------------------------------------------------------------------
# Import and clean data
#### Community Metrics ####
community_metrics = pd.read_csv('../data/network/affiliation_community_metrics.csv')
# community_metrics.head()
# community_metrics.dtypes


community_eval_metrics = community_metrics\
    .filter(items=['community_id',
                   'leader_org_pagerank_score',
                   'embeddedness',
                   'average_internal_degree',
                   'conductance',
                   'transitivity',
                   'cut_ratio',
                   'expansion',
                   'edges_inside',
                   'fraction_over_median_degree',
                   'hub_dominance',
                   'internal_edge_density',
                   'max_odf',
                   'avg_odf',
                   'flake_odf',
                   'triangle_participation_ratio'])
float_cols = community_eval_metrics.select_dtypes('float').columns
community_eval_metrics[float_cols] = community_eval_metrics[float_cols].round(2)

community_eval_metrics_tb = community_eval_metrics\
    .query('community_id ==  1')\
    .T\
    .reset_index()
community_eval_metrics_tb.columns = ['metrics', 'value']

#### Network Topology ####
# Load data
network_data = pd.read_csv('../data/network/affiliation_network_topology.csv')\
    .filter(items = ['community_id',
                     'source_org_pac_id',
                     'source_org_nm',
                     'target_org_pac_id',
                     'target_org_nm',
                     'shared_doctors'])

#### Top Doctors ####
top_docs = pd.read_csv('../data/network/affiliation_community_topdoctors.csv')
# top_docs.head()
# top_docs.dtypes
init_top_docs = top_docs\
    .query('community_id == 1')\
    .filter(items=['lst_nm','frst_nm','pri_spec','overall_mips_score'])

# We select the first 750 edges and associated nodes for an easier visualization
def prepare_cy_input(network_edges):

    nodes = set()
    cy_edges = []
    cy_nodes = []

    for _, network_edge in network_edges.iterrows():
        source = str(network_edge['source_org_pac_id'])
        target = str(network_edge['target_org_pac_id'])
        source_label = network_edge['source_org_nm']
        target_label = network_edge['target_org_nm']
        weight = network_edge['shared_doctors']

        if source not in nodes:
            nodes.add(source)
            cy_nodes.append({"data": {"id": source, "label": source_label}})
        if target not in nodes:
            nodes.add(target)
            cy_nodes.append({"data": {"id": target, "label": target_label}})

        cy_edges.append({
            'data': {
                'source': source,
                'target': target,
                'weight': weight
            }
        })

    return (cy_nodes, cy_edges)

cy_nodes, cy_edges = prepare_cy_input(network_data.query('community_id == 1'))


def community_map(community_metrics):
    fig = px.scatter_geo(community_metrics,
                         lon="long", lat='lat',
                         size="total_orgs_in_community",
                         color = 'total_shared_doctors_in_community',
                         custom_data=['community_id'],
                         hover_name='community_id',
                         hover_data={'total_orgs_in_community':True,
                                     'total_doctors_in_community':True,
                                     'total_shared_doctors_in_community':True,
                                     'zip':True,
                                     'cty':True,
                                     'lat':False,
                                     'long': False},
                         size_max=15,
                         scope='usa',
                         basemap_visible=True,
                         labels={'total_shared_doctors_in_community':"doctors shared",
                                 'total_orgs_in_community':'total orgs',
                                 'total_doctors_in_community':'total doctors',
                                 'zip':'zip',
                                 'cty':'city name'
                                 }
                     )

    # fig.update_layout(clickmode='event+select')

    fig.update_layout(
        title = {
            'text':'<b>Community US Map Overview</b>',
            'font':{'size':12},
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor="white"
    )

    return fig


# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([

    html.H2("Healthcare Organization Community Visualization Using Shared Doctor Affliation Network", style={'text-align': 'center'}),

    html.Div(["Select Minimum Total Doctors per Community to Display: ",
        dcc.Input(id='doctor_min',
                type='number',
                value=1,
                debounce=False,
                min=1)]),

    html.Div(id='output_container', children=[]),
    html.Br(),

    html.Div([
        dcc.Graph(
            id='acm_map', figure=community_map(community_metrics))],
            style={'width': '70%', 'display': 'inline-block'}),
    html.Div([
        html.P('Community Fitness Metrics',
               style={'font-size':'12px', 'text-align':'center', 'font-weight':'bold'}),
        dt.DataTable(
        id='com_eval_table',
        columns=[{"name": i, "id": i}
                 for i in community_eval_metrics_tb.columns],
        data=community_eval_metrics_tb.to_dict('records'),
        style_cell=dict(textAlign='left'),
        style_header=dict(backgroundColor="paleturquoise"),
        style_data=dict(backgroundColor="lavender"),
        style_table={'height': '350px', 'overflowY': 'auto'}
        )], style={'display': 'inline-block', 'width': '25%', 'float': 'right', 'margin': '30px'}),
    html.Br(),

    html.Div([
            dcc.Markdown("""
                **Community Networks**

                Click the community in the US map to view the relationships between organizations in each community.
                Connections indicate that two organizations share at least one doctor in common.

                Current community displayed:
            """),
            html.Pre(id='hover-data', style=stylez['pre'])
        ], className='three columns'),

    html.Br(),

    html.Div([cyto.Cytoscape(
            id='cytoscape',
            layout={'name': 'circle'},
            elements=cy_edges + cy_nodes,
            style={
                'height': '95vh',
                'width': '100%'}),
        ],
        style={'display': 'inline-block', 'width': '70%'}
        ),

    html.Div([
        html.P('Top Doctors in the Community',
               style={'font-size':'12px', 'text-align':'center', 'font-weight':'bold'}),
        dt.DataTable(
        id='top_doctors_table',
        columns=[{"name": i, "id": i}
                 for i in init_top_docs.columns],
        data=init_top_docs.to_dict('records'),
        style_cell=dict(textAlign='left'),
        style_header=dict(backgroundColor="paleturquoise"),
        style_data=dict(backgroundColor="lavender"),
        style_table={'height': '95vh', 'overflowY': 'auto'}
        )], style={'display': 'inline-block', 'width': '25%', 'float': 'right', 'margin': '30px'}),

])

# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
@app.callback(
    [Output(component_id='output_container', component_property='children'),
     Output(component_id='acm_map', component_property='figure')],
    [Input(component_id='doctor_min', component_property='value')]
)

def update_graph(doc_min):

    # print(doc_min)
    # print(type(doc_min))

    container = "Minimum total doctors per community: {}".format(doc_min)

    #### Copy dataframe for filtering
    filtered = community_metrics.copy()
    filtered = filtered[filtered['total_doctors_in_community']>=doc_min]

    # Plotly Express
    fig = community_map(filtered)
    return container, fig


@app.callback(
    Output(component_id='com_eval_table', component_property='data'),
    # Input(component_id='acm_map', component_property='hoverData')
    Input(component_id='acm_map', component_property='clickData')
)
def display_top_docs(clickData):
    try:
        comm_id = clickData['points'][0]['customdata'][0]

        # filter doctors for selected community
        tmp_eval_metrics_tb = community_eval_metrics\
            .query(f'community_id ==  {comm_id}')\
            .T\
            .reset_index()
        tmp_eval_metrics_tb.columns = ['metrics', 'value']
        selected_data = tmp_eval_metrics_tb.to_dict('records')
    except:
        selected_data = community_eval_metrics_tb.to_dict('records')
    return selected_data

@app.callback(
    [Output(component_id='hover-data', component_property='children'),
    Output(component_id='cytoscape',component_property='elements')],
    # [Input(component_id='acm_map', component_property='hoverData')],
    [Input(component_id='acm_map', component_property='clickData')],
    State(component_id='cytoscape', component_property='elements')
)
def update_cytoscape(clickData, elements):
    try:
        # get community ID from map
        selected_comm_id = clickData['points'][0]['customdata'][0]
        # selected_comm_id = hoverData['points'][0]['pointIndex']+1

        # update cytoscape web data to display selected community
        query_string = 'community_id == ' + str(selected_comm_id)

        new_cy_nodes, new_cy_edges = prepare_cy_input(network_data.query(query_string))

        elements = new_cy_edges + new_cy_nodes
    except:
        selected_comm_id = 1
        elements = cy_edges + cy_nodes

    return selected_comm_id, elements

@app.callback(
    Output(component_id='top_doctors_table', component_property='data'),
    # Input(component_id='acm_map', component_property='hoverData')
    Input(component_id='acm_map', component_property='clickData')
)
def display_top_docs(clickData):
    try:
        # comm_id = hoverData['points'][0]['pointIndex']+1
        comm_id = clickData['points'][0]['customdata'][0]

        # filter doctors for selected community
        top_comm_docs = top_docs.copy()
        top_comm_docs =top_comm_docs[top_comm_docs['community_id']==comm_id]
        top_comm_docs = top_comm_docs[['lst_nm','frst_nm','pri_spec','overall_mips_score']]

        selected_data = top_comm_docs.to_dict('records')
    except:
        selected_data = init_top_docs.to_dict('records')
    return selected_data

###### Run app on specified port
if __name__ == '__main__':
    app.run_server(debug=True, port=8559)
