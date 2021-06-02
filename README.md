# NU MSDS498 Capstone Group Project

Healthcare Organization Shared Doctor Affiliation Network Community Detection and Visualization

## Team Members

Danika Balas, Rikita Hirpara, Yanping Liu

## Introduction

In this project, we have built a Web App using Python Dash to display Healthcare Organization (Hospitals, Doctors' Clinics, Nursing Homes, etc.) Communities running in Google GCP App Runner. The visualizations include the communities' locations, demographics, network metrics, network topology, and top rated doctors. The Web App can be served as a guideline tool for users to find doctors and their affiliated organizations in a certain geographic area.

### Shared Doctor Network

We construct the organization affiliation network based on shared doctors. As long as two organizations shared one common doctor, they establish a link in the network. Below chart illustrates how the network is constructed:

![alt text](https://github.com/yanpingliu2021/msds490_group/blob/master/data/images/AffiliationNetwork.png?raw=true)

### Community Detection

In network science, a community in a graph is defined as a subset of nodes that are densely connected to each other and loosely connected to the nodes in the other communities in the same graph. In terms of the shared doctor organization network constructed in this project, a healthcare organization community can be viewed as a group of organizations contain a close set of common doctors.

We used the [louvain method](<https://github.com/taynaud/python-louvain>) to detect communities in the network constructed. The fitness metrics were calculated and displayed in the App. Please refer to the Helper section for the defintion of these fitness metrics.

### Data Source

The data used in this project were downloaded from Centers for Medicare & Medicaid Services (CMS). The data is part of the source data used to populate the [Medicare Care Compare Tool](<https://www.medicare.gov/care-compare/>) which was launched by CMS in 2020 to provide a web interface that patients and caregivers can use to make informed decisions about healthcare based on cost, quality of care, volume of services, and other data. The data contains various clinicians' performance ratings collected in the [CMS Quality Payment Program](<https://qpp.cms.gov/>)

The download link: <https://data.cms.gov/provider-data/topics/doctors-clinicians>

### Web Interface Screenshot

![alt text](https://github.com/yanpingliu2021/msds490_group/blob/master/data/images/WebInterface1.png?raw=true)

![alt text](https://github.com/yanpingliu2021/msds490_group/blob/master/data/images/WebInterface2.png?raw=true)

## How to

### Set up

 install dependencies

### Run the App

launch the dash web app

### Deploy to GCP App Runner

config

## Helper

Community fitness functions: <https://cdlib.readthedocs.io/en/latest/reference/evaluation.html></br>
Doctor performance metrics: <https://www.cms.gov/medicare/quality-initiatives-patient-assessment-instruments/compare-dac>
