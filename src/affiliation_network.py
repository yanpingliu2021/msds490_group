import networkx as nx
import numpy as np
import pandas as pd
from pandasql import sqldf
from community import community_louvain
from sklearn.preprocessing import MinMaxScaler
from utils import community_evaluation_metrics, PickleUtils
from utils import louvain_to_cdlib_coms, cdlib_coms_to_pandas
# from types import SimpleNamespace
# self = SimpleNamespace()

class affiliationnetwork(object):
    def __init__(self) -> None:
        self.affiliation_fact = f'../data/network/affiliation_fact.parquet'
        self.affiliation_network_gpickle = f'../data/network/affiliation_network.gpickle'
        self.affiliation_network_topology = f'../data/network/affiliation_network_topology.csv'
        self.affiliation_network_metrics = f'../data/network/affiliation_network_metrics.csv'
        self.affiliation_community_metrics = f'../data/network/affiliation_community_metrics.csv'
        self.affiliation_community_topdoctors = f'../data/network/affiliation_community_topdoctors.csv'

    def construct_network(self):

        prov_affl = pd.read_parquet('../data/processed/provider_affiliation.parquet')

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
        G = nx.read_gpickle(self.affiliation_network_gpickle)
        coms = community_louvain.best_partition(G.to_undirected(), weight='shared_doctors',
                                                randomize=False, resolution=0.05, random_state=42)
        cdlib_coms = louvain_to_cdlib_coms(G, coms)
        print(f"unique communities:{len(np.unique(list(coms.values())))}")
        PickleUtils.saver(cdlib_coms, '../data/network/affiliation_community.pkl')

    def writer(self):
        network_metrics = self.network_metrics()
        cdlib_coms = PickleUtils.loader('../data/network/affiliation_community.pkl')
        G = nx.read_gpickle(self.affiliation_network_gpickle)
        com_eval_df = community_evaluation_metrics(cdlib_coms)
        louvain_com_df = cdlib_coms_to_pandas(cdlib_coms)

        edges_df = pd.DataFrame(G.edges.data('shared_doctors', default=1),
                                columns = ['source_org_pac_id', 'target_org_pac_id', 'shared_doctors'])


        prov_affl = pd.read_parquet('../data/processed/provider_affiliation.parquet')
        affl_fact = pd.read_parquet(self.affiliation_fact)
        org_demo = pd.read_parquet('../data/processed/organization_demographics.parquet')
        clinician_measurement = pd.read_parquet('../data/processed/clinician_measurement.parquet')


        # construct network topology file
        network_topology = edges_df\
            .merge(louvain_com_df, left_on = ['source_org_pac_id'], right_on = ['org_pac_id'],  how = 'left')\
            .rename(columns = {'community_id': 'source_community_id'})\
            .drop(columns=['org_pac_id'])\
            .merge(louvain_com_df, left_on = ['target_org_pac_id'], right_on = ['org_pac_id'],  how = 'left')\
            .rename(columns = {'community_id': 'target_community_id'})\
            .drop(columns=['org_pac_id'])\
            .merge(org_demo[['org_pac_id', 'org_nm']], left_on = ['source_org_pac_id'], right_on = ['org_pac_id'],  how = 'inner')\
            .rename(columns = {'org_nm': 'source_org_nm'})\
            .drop(columns=['org_pac_id'])\
            .merge(org_demo[['org_pac_id', 'org_nm']], left_on = ['target_org_pac_id'], right_on = ['org_pac_id'],  how = 'inner')\
            .rename(columns = {'org_nm': 'target_org_nm'})\
            .drop(columns=['org_pac_id'])

        network_topology['community_id'] = np.where(
            network_topology['target_community_id'] == network_topology['source_community_id'],
            network_topology['target_community_id'],
            None
        )
        network_topology.drop(columns=['source_community_id', 'target_community_id'], inplace=True)


        # construct network metrics file at organization level
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
            .merge(org_demo, on = ['org_pac_id'], how = 'inner')\
            .sort_values(by=['community_id', 'org_pac_id'], ascending=True)\
            .reset_index(drop = True)

        network_metrics_final['leader_org'] = network_metrics_final\
        .groupby("community_id")["pagerank"]\
        .rank(method="first", ascending=False, na_option='keep')

        network_topology.to_csv(self.affiliation_network_topology, index=False)
        network_metrics_final.to_csv(self.affiliation_network_metrics, index=False)

        # construct community metrics file
        community_measure = clinician_measurement\
            .merge(louvain_com_df, on = ['org_pac_id'], how = 'inner')\
            .drop(columns=['org_pac_id', 'org_nm'])\
            .drop_duplicates()\
            .groupby('community_id')\
            .agg(total_mips_rated_doctors = ('npi', 'size'),
                average_quality_score_per_doctor = ('quality_score', lambda x: x.mean(skipna=False)),
                average_promoting_interoperability_score_per_doctor = ('promoting_interoperability_score', lambda x: x.mean(skipna=False)),
                average_cost_score_per_doctor = ('cost_score', lambda x: x.mean(skipna=False)),
                average_improvement_activities_score_per_doctor = ('improvement_activities_score', lambda x: x.mean(skipna=False)),
                average_mips_score_per_doctor = ('overall_mips_score', 'mean')
                )\
            .reset_index()

        community_metrics = network_metrics_final\
            .query('leader_org == 1')\
            .filter(items = ['community_id', 'pagerank',
                            'org_pac_id', 'org_nm',
                            'total_doctors_in_community',
                            'total_orgs_in_community',
                            'total_shared_doctors_in_community',
                            'adr_ln_1', 'adr_ln_1', 'zip', 'cty', 'st',
                            'lat', 'long'])\
            .reset_index(drop = True)\
            .rename(columns = {'pagerank': 'leader_org_pagerank_score',
                            'org_pac_id': 'leader_org_pac_id',
                            'org_nm': 'leader_org_nm'}
                    )\
            .merge(com_eval_df, on = ['community_id'], how = 'left')\
            .merge(community_measure, on = ['community_id'], how = 'left')

        community_metrics.to_csv(self.affiliation_community_metrics, index=False)


        # identify top 20 rated doctors in each community in the MIPS program
        top20_doctors = clinician_measurement\
            .merge(louvain_com_df, on = ['org_pac_id'], how = 'inner')\
            .drop(columns=['org_pac_id', 'org_nm'])\
            .drop_duplicates()\
            .sort_values(by = ['overall_mips_score', 'improvement_activities_score', 'quality_score'],
                         ascending = False)\
            .groupby('community_id').head(20)

        top20_doctors.to_csv(self.affiliation_community_topdoctors, index=False)


        return (network_topology, network_metrics_final, community_metrics)


if __name__=='__main__':
    network_cls = affiliationnetwork()
    network_cls.construct_network()
    network_topology, network_metrics, community_metrics = network_cls.community_detection()