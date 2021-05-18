import pandas as pd
import numpy as np
from affiliation_network import affiliationnetwork
import plotly.graph_objects as go
import plotly.express as px
import networkx as nx

network_cls = affiliationnetwork()
network_cls.construct_network()
network_cls.community_detection()
network_topology, network_metrics, community_metrics = network_cls.writer()
"""
Name:
Type: DiGraph
Number of nodes: 36543
Number of edges: 189012
Average in degree:   5.1723
Average out degree:   5.1723

unique communities:3307
"""

affiliation_fact = pd.read_parquet('../data/network/affiliation_fact.parquet')
affiliation_fact.head()

fig = px.scatter_geo(community_metrics, lon="long", lat='lat',
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
fig.show()

fig.write_html(f"../data/network/community_us_map.html")