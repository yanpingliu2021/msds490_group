import networkx as nx
import numpy as np
import pandas as pd
import re
from pandasql import sqldf
from community import community_louvain
from types import SimpleNamespace
self = SimpleNamespace()

class affiliationnetwork(object):
  def __init__(self) -> None:

      self.affiliation_network_gpickle = f'../data/network/affiliation_network.gpickle'
      self.affiliation_network_topology = f'../data/network/affiliation_network_topology.csv'
      self.affiliation_network_metrics = f'../data/network/affiliation_network_metrics.csv'

  def construct_network(self):

      prov_affl = pd.read_parquet('../data/processed/provider_affiliation.parquet')
      prov_affl_jon = prov_affl.merge(prov_affl, on=['afl_org_nm'], how='inner')


      q = """SELECT
              a.npi as source_npi,
              b.npi as target_npi,
              count(distinct a.afl_org_nm) as num_affiliations
          FROM
              prov_affl a
          INNER JOIN
              prov_affl b
          ON a.afl_org_nm = b.afl_org_nm
          AND a.npi <> b.npi
          GROUP BY a.npi, b.npi;"""
      network_edges = sqldf(q, locals())
      network_edges = sqldf(q, globals())

      # network_edges.head()
      # network_edges.shape

      G = nx.from_pandas_edgelist(network_edges,
                                  create_using=nx.DiGraph(),
                                  source='source_npi',
                                  target='target_npi',
                                  edge_attr=['affiliation']
                                  )
      print(nx.info(G))

      nx.write_gpickle(G, self.affiliation_network_gpickle)

      self.pat_fact_table = disease_pat_npi[['PATIENT_ID', 'NPI']].drop_duplicates().reset_index(drop = True)

  def network_metrics(self):
      G = nx.read_gpickle(self.affiliation_network_gpickle)
      pr = nx.pagerank(G)
      indegree = dict(G.in_degree(weight = 'weight'))
      outdegree = dict(G.out_degree(weight = 'weight'))

      tmp_dict = {key: (pr[key], indegree[key], outdegree[key]) for key in pr}
      network_metrics = pd.DataFrame(tmp_dict).T.reset_index()
      network_metrics.columns = ['npi', 'pagerank', 'indegree', 'outdegree']

      return network_metrics

  def community_detection(self):
    network_metrics = self.network_metrics()
    G = nx.read_gpickle(self.affiliation_network_gpickle)
    coms = community_louvain.best_partition(G.to_undirected(), weight='affiliation', randomize=False, resolution=0.1, random_state=42)
    louvain_com_df = pd.DataFrame(coms.items(), columns = ['npi', 'community_id'])
    print(f"unique communities:{louvain_com_df['community_id'].nunique()}")
    # louvain_com_df.groupby('community_id').size().sort_values(ascending=False)
    louvain_com_df['community_id'] = louvain_com_df['community_id'] + 1
    edges_df = pd.DataFrame(G.edges.data('affiliation', default=1), columns = ['source_npi', 'target_npi', 'affiliation'])

    network_topology = edges_df\
        .merge(louvain_com_df, left_on = ['source_npi'], right_on = ['npi'],  how = 'left')\
        .rename(columns = {'community_id': 'source_community_id'})\
        .drop(columns=['npi'])\
        .merge(louvain_com_df, left_on = ['target_npi'], right_on = ['npi'],  how = 'left')\
        .rename(columns = {'community_id': 'target_community_id'})\
        .drop(columns=['npi'])
    network_topology['community_id'] = np.where(
        network_topology['target_community_id'] == network_topology['source_community_id'],
        network_topology['target_community_id'],
        None
    )
    network_topology.drop(columns=['source_community_id', 'target_community_id'], inplace=True)


    npi_pat = self.pat_fact_table\
        .merge(louvain_com_df, left_on = ['NPI'], right_on = ['npi'], how = 'inner')\
        .groupby('npi')\
        .agg(doctor_self_pats = ('PATIENT_ID', 'size'))\
        .reset_index()

    com_pats = self.pat_fact_table\
        .merge(louvain_com_df, left_on = ['NPI'], right_on = ['npi'], how = 'inner')\
        .filter(items=['community_id', 'PATIENT_ID'])\
        .drop_duplicates()\
        .groupby('community_id')\
        .agg(total_pats_in_community = ('PATIENT_ID', 'size'))\
        .reset_index()

    com_size = louvain_com_df\
        .groupby('community_id')\
        .agg(total_doctors_in_community = ('npi', 'size'))\
        .reset_index()

    com_links = network_topology\
        .groupby('community_id')\
        .agg(total_links_in_community = ('affiliation', 'size'))\
        .reset_index()

    network_metrics_final = network_metrics\
        .merge(louvain_com_df, on = ['npi'], how = 'left')\
        .merge(npi_pat, on = ['npi'], how = 'left')\
        .merge(com_pats, on = ['community_id'], how = 'left')\
        .merge(com_size, on = ['community_id'], how = 'left')\
        .merge(com_links, on = ['community_id'], how = 'left')\

    network_topology.to_csv(self.affiliation_network_topology, index=False)
    network_metrics_final.to_csv(self.affiliation_network_metrics, index=False)

    return (network_topology, network_metrics_final)

if __name__=='__main__':
    network_cls = affiliationnetwork(disease='type 2 diabetes')
    network_cls.construct_network()
    network_topology, network_metrics = network_cls.community_detection()