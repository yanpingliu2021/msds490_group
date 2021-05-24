import pickle
from cdlib import NodeClustering
from cdlib import evaluation
from collections import defaultdict
import pandas as pd

def louvain_to_cdlib_coms(G, coms):
    uG = G.to_undirected()
    coms_to_node = defaultdict(list)
    for n, c in coms.items():
        coms_to_node[c].append(n)

    coms_louvain = [list(c) for c in coms_to_node.values()]

    cdlib_coms = NodeClustering(
        coms_louvain, uG, 'louvain',
        method_parameters={'weight':'shared_doctors'}
    )
    return cdlib_coms

def cdlib_coms_to_pandas(cdlib_coms):
    com_df = pd.DataFrame(
        [(x, idx+1) for idx, x in enumerate(cdlib_coms.communities)],
        columns=['org_pac_id', 'community_id'])\
        .explode('org_pac_id')\
        .reset_index(drop = True)

    return com_df

class PickleUtils(object):
    """
    Pickle file loader/saver utility function
    """
    def __init__(self):
        pass

    @staticmethod
    def loader(directory):
        with open(directory, 'rb') as f:
            data = pickle.load(f)
            print(f"load pickle from {directory}")
        return data

    @staticmethod
    def saver(data, directory):
        with open(directory, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            print(f"save data to {directory}")



def community_evaluation_metrics(cdlib_coms):
    uG = cdlib_coms.graph

    # https://cdlib.readthedocs.io/en/latest/reference/evaluation.html
    embeddedness = evaluation.avg_embeddedness(uG, cdlib_coms, summary = False)
    average_internal_degree = evaluation.average_internal_degree(uG, cdlib_coms, summary = False)
    conductance = evaluation.conductance(uG, cdlib_coms, summary = False)
    transitivity = evaluation.avg_transitivity(uG, cdlib_coms, summary = False)
    cut_ratio = evaluation.cut_ratio(uG, cdlib_coms, summary = False)
    expansion = evaluation.expansion(uG, cdlib_coms, summary = False)
    edges_inside = evaluation.edges_inside(uG, cdlib_coms, summary = False)
    fraction_over_median_degree = evaluation.fraction_over_median_degree(uG, cdlib_coms, summary = False)
    hub_dominance = evaluation.hub_dominance(uG, cdlib_coms, summary = False)
    internal_edge_density = evaluation.internal_edge_density(uG, cdlib_coms, summary = False)
    max_odf = evaluation.max_odf(uG, cdlib_coms, summary = False)
    avg_odf = evaluation.avg_odf(uG, cdlib_coms, summary = False)
    flake_odf = evaluation.flake_odf(uG, cdlib_coms, summary = False)
    size = evaluation.size(uG, cdlib_coms, summary = False)
    triangle_participation_ratio = evaluation.triangle_participation_ratio(uG, cdlib_coms, summary = False)


    eval_dict = {
        'embeddedness':embeddedness,
        'average_internal_degree':average_internal_degree,
        'conductance':conductance,
        'transitivity':transitivity,
        'cut_ratio':cut_ratio,
        'expansion':expansion,
        'edges_inside':edges_inside,
        'fraction_over_median_degree':fraction_over_median_degree,
        'hub_dominance':hub_dominance,
        'internal_edge_density':internal_edge_density,
        'max_odf':max_odf,
        'avg_odf':avg_odf,
        'flake_odf':flake_odf,
        'size':size,
        'triangle_participation_ratio':triangle_participation_ratio
    }

    com_eval_df = pd.DataFrame(eval_dict)\
        .reset_index()\
        .rename({'index':'community_id'}, axis = 1)
    com_eval_df['community_id'] = com_eval_df['community_id'] + 1
    return com_eval_df