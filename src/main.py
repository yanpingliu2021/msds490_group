import pandas as pd
import numpy as np
from affiliation_network import affiliationnetwork
import plotly.graph_objects as go


network_cls = affiliationnetwork()
network_cls.construct_network()
network_topology, network_metrics, community_metrics = network_cls.community_detection()
"""
Name: 
Type: DiGraph
Number of nodes: 36543
Number of edges: 189012
Average in degree:   5.1723
Average out degree:   5.1723

unique communities:3307
"""




import plotly.express as px
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

fig.write_html(f"data/network/community_us_map.html")



# fig = px.scatter(viz_data_df, x="market_rxs", y="trelegy_share",
#                  size="size", color="trelegy_share_decile",
#                  hover_name="louvain_community_id", size_max=30,
#                  labels=dict(market_rxs="Market Rxs Volume", trelegy_share="Trelegy Share",
#                              trelegy_share_decile="Trelegy Share Decile")
#                  )
# fig.update_yaxes(tick0=0, dtick=20)
# fig.show()


viz_data_df[viz_data_df['trelegy_share'] >= 0.11]

hcp_viz_sdf = hcp_mcm_trim_sdf\
    .where('louvain_community_id = 347')\
    .select('npi', sf.expr('concat("Dr. ", last_nm) as name'), 'lat_nbr', 'lon_nbr',
            'prscore_decile')
hcp_viz_sdf.show()

network_feature_sdf = spark.read.parquet(config['network']['sharedpats_network_metrics_pqt'])\
    .orderBy(sf.col('sharedpats_prscore').desc())\
    .limit(1000)
network_feature_sdf.count()
network_feature_sdf.toPandas()['sharedpats_indegree'].describe()
"""
count    1000.00000
mean     5515.21191
std      3462.24048
min       352.00000
25%      2946.75000
50%      4879.00000
75%      7293.50000
max     21144.00000
"""

sharedpats_edges_df = get_edges(SharedPatsNetworksDirect(spark, config).sharedpats_network_gpickle)
sharedpats_edges_sdf = pandas_to_spark(spark, sharedpats_edges_df)
edges_sdf = network_feature_sdf\
    .select(sf.col('npi').alias('to_id'))\
    .join(sharedpats_edges_sdf, 'to_id', 'inner')
edges_sdf.cache()
edges_sdf.select(sf.countDistinct('to_id'), sf.count('*'), sf.countDistinct('from_id')).show()
"""
+---------------------+--------+-----------------------+
|count(DISTINCT to_id)|count(1)|count(DISTINCT from_id)|
+---------------------+--------+-----------------------+
|                 1000|  167695|                  90892|
+---------------------+--------+-----------------------+
"""

comm_edges_sdf = edges_sdf\
    .join(hcp_viz_sdf, [edges_sdf.from_id == hcp_viz_sdf.npi], 'inner')\
    .select('from_id',
            'to_id',
            sf.col('name').alias('from_name'),
            sf.col('lat_nbr').alias('from_lat_nbr'),
            sf.col('lon_nbr').alias('from_lon_nbr'),
            'weight')\
    .join(hcp_viz_sdf, [edges_sdf.to_id == hcp_viz_sdf.npi], 'inner')\
    .select('from_id',
            'to_id',
            'from_name',
            'from_lat_nbr',
            'from_lon_nbr',
            sf.col('name').alias('to_name'),
            sf.col('lat_nbr').alias('to_lat_nbr'),
            sf.col('lon_nbr').alias('to_lon_nbr'),
            'weight',
            decile('weight'))
comm_edges_sdf.count()
# 22

comm_edges_df = comm_edges_sdf.toPandas()
# G = nx.from_pandas_edgelist(comm_edges_df,
#                             create_using=nx.Graph(),
#                             source='from_name',
#                             target='to_name',
#                             edge_attr=['weight'])

G = nx.Graph()
# Add node for each character
for idx, row in hcp_viz_sdf.select('name', 'prscore_decile').toPandas().iterrows():
    G.add_node(row['name'], size = row['prscore_decile'])

# For each co-appearance between two characters, add an edge
for idx, row in comm_edges_df.iterrows():
    G.add_edge(row['from_name'], row['to_name'], weight = row['weight'])

print(nx.info(G))
print(len(G))




# method 1 static chart
# Get positions for the nodes in G
pos_ = nx.spring_layout(G)
# Custom function to create an edge between node x and node y, with a given text and width
def make_edge(x, y, text, width):

    '''Creates a scatter trace for the edge between x's and y's with given width

    Parameters
    ----------
    x    : a tuple of the endpoints' x-coordinates in the form, tuple([x0, x1, None])

    y    : a tuple of the endpoints' y-coordinates in the form, tuple([y0, y1, None])

    width: the width of the line

    Returns
    -------
    An edge trace that goes between x0 and x1 with specified width.
    '''
    return  go.Scatter(x         = x,
                       y         = y,
                       line      = dict(width = width,
                                   color = 'cornflowerblue'),
                       hoverinfo = 'text',
                       text      = ([text]),
                       mode      = 'lines')

# For each edge, make an edge_trace, append to list
edge_trace = []
for idx, edge in comm_edges_df.iterrows():
    char_1 = edge['from_name']
    char_2 = edge['to_name']

    x0, y0 = pos_[char_1]
    x1, y1 = pos_[char_2]

    text   = char_1 + '--' + char_2 + ': ' + str(edge['weight'])

    trace  = make_edge([x0, x1, None], [y0, y1, None], text,
                        edge['weight_decile'])
    edge_trace.append(trace)

# Make a node trace
node_trace = go.Scatter(x         = [],
                        y         = [],
                        text      = [],
                        textposition = "top center",
                        textfont_size = 10,
                        mode      = 'markers+text',
                        hoverinfo = 'none',
                        marker    = dict(color = [],
                                         size  = [],
                                         line  = None))
# For each node in midsummer, get the position and size and add to the node_trace
for node in G.nodes():
    x, y = pos_[node]
    node_trace['x'] += tuple([x])
    node_trace['y'] += tuple([y])
    node_trace['marker']['color'] += tuple(['cornflowerblue'])
    node_trace['marker']['size'] += tuple([5*G.nodes()[node]['size']])
    node_trace['text'] += tuple(['<b>' + node + '</b>'])


layout = go.Layout(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)'
)


fig = go.Figure(layout = layout)

for trace in edge_trace:
    fig.add_trace(trace)

fig.add_trace(node_trace)

fig.update_layout(showlegend = False)

fig.update_xaxes(showticklabels = False)

fig.update_yaxes(showticklabels = False)

fig.show()

fig.write_html(f"{config['main']['LOCAL_DATA_PATH']}/05_reporting/mcm_community_347.html")





# method 2 map network chart
import plotly.express as px
fig = px.scatter_geo(hcp_viz_sdf.toPandas(), lon="lon_nbr", lat='lat_nbr',
                     size="prscore_decile",
                     hover_name='name',
                     color = 'prscore_decile',
                     size_max=15
                     )

df = hcp_viz_sdf.toPandas()

fig.add_trace(go.Scattergeo(
    lon=df['lon_nbr'][[18,19,17,13,15,22,14,12]],
    lat=df['lat_nbr'][[18,19,17,13,15,22,14,12]],
    text=df['name'][[18,19,17,13,15,22,14,12]],
    mode='text',
    textfont={
        "size": 7,
        "color": "MidnightBlue"
    }
))
fig.update_traces(textposition='bottom center')


# https://plotly.com/python/lines-on-maps/
for i in range(len(comm_edges_df)):
    fig.add_trace(
        go.Scattergeo(
            lon = [comm_edges_df['from_lon_nbr'][i], comm_edges_df['to_lon_nbr'][i]],
            lat = [comm_edges_df['from_lat_nbr'][i], comm_edges_df['to_lat_nbr'][i]],
            mode = 'lines',
            line = dict(width = 1,color = 'red'),
            opacity = float(comm_edges_df['weight_decile'][i])/max(comm_edges_df['weight_decile']),
        )
    )


fig.update_layout(
    title_text = 'Community:347',
    showlegend = False,
)

fig.update_geos(fitbounds="locations")
fig.show()

fig.write_html(f"{config['main']['LOCAL_DATA_PATH']}/05_reporting/mcm_community_347_map.html")