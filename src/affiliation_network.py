import networkx as nx
import numpy as np
import pandas as pd
import re
from pandasql import sqldf
from community import community_louvain
from sklearn.preprocessing import MinMaxScaler
#from types import SimpleNamespace
#self = SimpleNamespace()

class affiliationnetwork(object):
  def __init__(self) -> None:

      self.affiliation_fact = f'data/network/affiliation_fact.parquet'
      self.affiliation_network_gpickle = f'data/network/affiliation_network.gpickle'
      self.affiliation_network_topology = f'data/network/affiliation_network_topology.csv'
      self.affiliation_network_metrics = f'data/network/affiliation_network_metrics.csv'
      self.affiliation_community_metrics = f'data/network/affiliation_community_metrics.csv'
      
  def construct_network(self):

      prov_affl = pd.read_parquet('data/processed/provider_affiliation.parquet')
      prov_affl.shape
      prov_affl.head()

      q = """SELECT
              a.org_pac_id as source_org_pac_id,
              b.org_pac_id as target_org_pac_id,
              a.npi
          FROM
              prov_affl a
          INNER JOIN
              prov_affl b
          ON a.npi = b.npi
          AND a.org_pac_id <> b.org_pac_id
          GROUP BY a.org_pac_id, b.org_pac_id, a.npi;"""
      
      affl_fact = sqldf(q, locals())
      
      network_edges = affl_fact\
        .groupby(['source_org_pac_id', 'target_org_pac_id'])\
        .agg(shared_doctors = ('npi', 'size'))\
        .reset_index()
      
      # network_edges.head()
      # network_edges.shape

      G = nx.from_pandas_edgelist(network_edges,
                                  create_using=nx.DiGraph(),
                                  source='source_org_pac_id',
                                  target='target_org_pac_id',
                                  edge_attr=['shared_doctors']
                                  )
      print(nx.info(G))

      nx.write_gpickle(G, self.affiliation_network_gpickle)
      affl_fact.to_parquet(self.affiliation_fact)

  def network_metrics(self):
      G = nx.read_gpickle(self.affiliation_network_gpickle)
      pr = nx.pagerank(G)

      network_metrics = pd.DataFrame(pr.items())
      scaler = MinMaxScaler()
      network_metrics.columns = ['org_pac_id', 'pagerank']
      network_metrics[['pagerank']] = scaler.fit_transform(network_metrics[['pagerank']])

      return network_metrics

  def community_detection(self):
    network_metrics = self.network_metrics()
    G = nx.read_gpickle(self.affiliation_network_gpickle)
    coms = community_louvain.best_partition(G.to_undirected(), weight='shared_doctors', 
                                            randomize=False, resolution=0.05, random_state=42)
    louvain_com_df = pd.DataFrame(coms.items(), columns = ['org_pac_id', 'community_id'])
    print(f"unique communities:{louvain_com_df['community_id'].nunique()}")
    # louvain_com_df.groupby('community_id').size().sort_values(ascending=False)
    louvain_com_df['community_id'] = louvain_com_df['community_id'] + 1
    edges_df = pd.DataFrame(G.edges.data('shared_doctors', default=1), 
                            columns = ['source_org_pac_id', 'target_org_pac_id', 'shared_doctors'])

    network_topology = edges_df\
        .merge(louvain_com_df, left_on = ['source_org_pac_id'], right_on = ['org_pac_id'],  how = 'left')\
        .rename(columns = {'community_id': 'source_community_id'})\
        .drop(columns=['org_pac_id'])\
        .merge(louvain_com_df, left_on = ['target_org_pac_id'], right_on = ['org_pac_id'],  how = 'left')\
        .rename(columns = {'community_id': 'target_community_id'})\
        .drop(columns=['org_pac_id'])
  
    network_topology['community_id'] = np.where(
        network_topology['target_community_id'] == network_topology['source_community_id'],
        network_topology['target_community_id'],
        None
    )
    network_topology.drop(columns=['source_community_id', 'target_community_id'], inplace=True)

    prov_affl = pd.read_parquet('data/processed/provider_affiliation.parquet')
    affl_fact = pd.read_parquet(self.affiliation_fact)
    org_demo = pd.read_parquet('data/processed/organization_demographics.parquet')


    org_prov_cnt = prov_affl\
        .merge(louvain_com_df, on = ['org_pac_id'], how = 'inner')\
        .filter(items = ['org_pac_id', 'npi'])\
        .drop_duplicates()\
        .groupby('org_pac_id')\
        .agg(self_doctors = ('npi', 'size'))\
        .reset_index()

    com_prov_cnt = prov_affl\
        .merge(louvain_com_df, on = ['org_pac_id'], how = 'inner')\
        .filter(items=['community_id', 'npi'])\
        .drop_duplicates()\
        .groupby('community_id')\
        .agg(total_doctors_in_community = ('npi', 'size'))\
        .reset_index()

    com_size = louvain_com_df\
        .groupby('community_id')\
        .agg(total_orgs_in_community = ('org_pac_id', 'size'))\
        .reset_index()

    com_shared_prov = network_topology\
        .merge(affl_fact, on = ['source_org_pac_id', 'target_org_pac_id'], how = 'inner')\
        .filter(items = ['community_id', 'npi'])\
        .drop_duplicates()\
        .groupby('community_id')\
        .agg(total_shared_doctors_in_community = ('npi', 'size'))\
        .reset_index()
      

    network_metrics_final = network_metrics\
        .merge(louvain_com_df, on = ['org_pac_id'], how = 'left')\
        .merge(org_prov_cnt, on = ['org_pac_id'], how = 'left')\
        .merge(com_prov_cnt, on = ['community_id'], how = 'left')\
        .merge(com_size, on = ['community_id'], how = 'left')\
        .merge(com_shared_prov, on = ['community_id'], how = 'left')\
        .merge(org_demo\
                .filter(items=['org_pac_id', 'org_nm', 'zip', 'cty', 'st', 'lat', 'long'])\
                .drop_duplicates()\
                .reset_index(drop = True),
               on = ['org_pac_id'],
               how = 'inner')\
        .sort_values(by=['community_id', 'org_pac_id'], ascending=True)\
        .reset_index(drop = True)

    network_metrics_final['leader_org'] = network_metrics_final\
      .groupby("community_id")["pagerank"]\
      .rank(method="first", ascending=False, na_option='keep')
  
    network_topology.to_csv(self.affiliation_network_topology, index=False)
    network_metrics_final.to_csv(self.affiliation_network_metrics, index=False)
    community_metrics = network_metrics_final\
      .query('leader_org == 1')\
      .filter(items = ['community_id',
                'total_doctors_in_community',
                'total_orgs_in_community',
                'total_shared_doctors_in_community',
                'zip', 'cty', 'st', 'lat', 'long'])\
      .reset_index(drop = True)

    community_metrics.to_csv(self.affiliation_community_metrics, index=False)
    
    return (network_topology, network_metrics_final, community_metrics)

if __name__=='__main__':
    network_cls = affiliationnetwork()
    network_cls.construct_network()
    network_topology, network_metrics, community_metrics = network_cls.community_detection()