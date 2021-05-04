import pandas as pd
import numpy as np
from sharedpatsnetwork import sharedpatsnetwork


network_cls = sharedpatsnetwork(disease='type 2 diabetes')
network_cls.construct_network()
network_topology, network_metrics = network_cls.community_detection()

network_cls = sharedpatsnetwork(disease='type 1 diabetes')
network_cls.construct_network()
network_topology, network_metrics = network_cls.community_detection()

network_cls = sharedpatsnetwork(disease='renal disease')
network_cls.construct_network()
network_topology, network_metrics = network_cls.community_detection()


nppes_prov = pd.read_csv('../data/NPPES/npidata.csv', nrows=100000)
nppes_prov