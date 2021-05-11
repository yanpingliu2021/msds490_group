import pandas as pd
import plotly.express as px


community_metrics = pd.read_csv('../data/network/affiliation_community_metrics.csv')
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