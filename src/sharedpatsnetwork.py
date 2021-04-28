import networkx as nx
import numpy as np
import pandas as pd
import re
from pandasql import sqldf
from community import community_louvain

class sharedpatsnetwork(object):
  def __init__(self, disease='type 2 diabetes') -> None:
      """
        disease: the target disease name for building shared patients HCP network, such as type II Diabetes
        list of disease are supported
          peptic ulcer disease
          cerebrovascular disease
          chronic pulmonary disease
          peripheral vascular disease
          renal disease
          hemiplegia or paraplegia
          myocardial infarction
          metastatic solid tumor
          congestive heart failure
          mild liver disease
          type 1 diabetes
          type 2 diabetes
          dementia
          rheumatic disease
          moderate or severe liver disease
          aids/hiv
      """
      disease_def = pd.read_parquet('../data/input/disease_def.parquet')
      self.disease_def = disease_def.loc[disease_def['disease'] == disease].reset_index(drop=True)
      assert self.disease_def.shape[0] != 0, f'the disease {disease} is not supported in this tool'
      self.disease = disease
      fileprefix = re.sub('[\s+/|\\\]', '_', disease)
      self.sharedpats_network_gpickle = f'../data/network/{fileprefix}_sharedpats.gpickle'
      self.sharedpats_network_topology = f'../data/network/{fileprefix}_sharedpats_network_topology.csv'
      self.sharedpats_network_metrics = f'../data/network/{fileprefix}_sharedpats_network_metrics.csv'

  def construct_network(self, threshold=2):
      """
      threshold: how many shared patients to establish a edge
      """
      pat_diag_fact = pd.read_parquet('../data/fact/pat_diag_fact.parquet')
      pat_npi_fact = pd.read_parquet('../data/fact/pat_npi_fact.parquet')
      # pat_diag_fact.head()
      disease_pat = pat_diag_fact\
          .merge(self.disease_def, on = 'DIAG_CD', how = 'inner')\
          .filter(items = ['PATIENT_ID', 'CLAIM_ID'])\
          .drop_duplicates()

      disease_pat_npi = pat_npi_fact\
          .merge(disease_pat, on = ['PATIENT_ID', 'CLAIM_ID'], how = 'inner')


      q = """SELECT
              a.npi as source_npi,
              b.npi as target_npi,
              count(distinct a.patient_id) as sharedpats
          FROM
              disease_pat_npi a
          INNER JOIN
              disease_pat_npi b
          ON a.patient_id = b.patient_id
          AND a.npi <> b.npi
          AND b.SVC_DT > a.SVC_DT
          GROUP BY a.npi, b.npi;"""
      network_edges = sqldf(q, locals())

      # network_edges.head()
      # network_edges.shape
      network_edges = network_edges\
        .query(f"sharedpats >= {threshold}")\
          .reset_index(drop=True)

      G = nx.from_pandas_edgelist(network_edges,
                                  create_using=nx.DiGraph(),
                                  source='source_npi',
                                  target='target_npi',
                                  edge_attr=['sharedpats']
                                  )
      print(nx.info(G))

      nx.write_gpickle(G, self.sharedpats_network_gpickle)

      self.pat_fact_table = disease_pat_npi[['PATIENT_ID', 'NPI']].drop_duplicates().reset_index(drop = True)

  def network_metrics(self):
      G = nx.read_gpickle(self.sharedpats_network_gpickle)
      pr = nx.pagerank(G)
      indegree = dict(G.in_degree(weight = 'weight'))
      outdegree = dict(G.out_degree(weight = 'weight'))

      tmp_dict = {key: (pr[key], indegree[key], outdegree[key]) for key in pr}
      network_metrics = pd.DataFrame(tmp_dict).T.reset_index()
      network_metrics.columns = ['npi', 'pagerank', 'indegree', 'outdegree']

      return network_metrics

  def community_detection(self):
    network_metrics = self.network_metrics()
    G = nx.read_gpickle(self.sharedpats_network_gpickle)
    coms = community_louvain.best_partition(G.to_undirected(), weight='sharedpats', randomize=False, resolution=0.1, random_state=42)
    louvain_com_df = pd.DataFrame(coms.items(), columns = ['npi', 'community_id'])
    print(f"unique communities:{louvain_com_df['community_id'].nunique()}")
    # louvain_com_df.groupby('community_id').size().sort_values(ascending=False)
    louvain_com_df['community_id'] = louvain_com_df['community_id'] + 1
    edges_df = pd.DataFrame(G.edges.data('sharedpats', default=1), columns = ['source_npi', 'target_npi', 'sharedpats'])

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
        .agg(total_links_in_community = ('sharedpats', 'size'))\
        .reset_index()

    network_metrics_final = network_metrics\
        .merge(louvain_com_df, on = ['npi'], how = 'left')\
        .merge(npi_pat, on = ['npi'], how = 'left')\
        .merge(com_pats, on = ['community_id'], how = 'left')\
        .merge(com_size, on = ['community_id'], how = 'left')\
        .merge(com_links, on = ['community_id'], how = 'left')\

    network_topology.to_csv(self.sharedpats_network_topology, index=False)
    network_metrics_final.to_csv(self.sharedpats_network_metrics, index=False)

    return (network_topology, network_metrics_final)

if __name__=='__main__':
    network_cls = sharedpatsnetwork(disease='type 2 diabetes')
    network_cls.construct_network()
    network_topology, network_metrics = network_cls.community_detection()