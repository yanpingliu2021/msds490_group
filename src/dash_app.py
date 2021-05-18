"""
Original Demo: http://js.cytoscape.org/demos/cose-layout/
Note: This implementation looks different from the original implementation,
although the input paramters are exactly the same.
"""
# https://github.com/plotly/dash-cytoscape
import pandas as pd
import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import dash_cytoscape as cyto

app = dash.Dash(__name__)
server = app.server

# ###################### DATA PREPROCESSING ######################
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

app.layout = html.Div(className='eight columns', children=[
        cyto.Cytoscape(
            id='cytoscape',
            elements=cy_edges + cy_nodes,
            style={
                'height': '95vh',
                'width': '100%'
            }
        )
    ])


if __name__ == '__main__':
    app.run_server(debug=True)