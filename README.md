# NU MSDS498 Capstone Group Project

Healthcare Organization Shared Doctor Affiliation Network: Community Detection and Visualization

## Team Members

Danika Balas, Rikita Hirpara, Yanping Liu

## Introduction

In this project, we have built a web application to display healthcare organization (hospitals, private practice clinics, nursing homes, etc.) communities. The visualizations include the communities' locations, demographics, network metrics, network topology, and top rated doctors based on their [MIPS scores](<https://qpp.cms.gov/mips/quality-requirements>). 

The web app can serve as a guideline tool for users to find doctors and their affiliated organizations within specific geographic areas. It can also be used to discover relationships between healthcare organizations.
### Shared Doctor Network

We constructed the healthcare organization affiliation network based on shared doctors. When two organizations share at least one common doctor, they establish a link within the network. The figure below illustrates how the network is constructed:

![alt text](https://github.com/yanpingliu2021/msds490_group/blob/master/data/images/AffiliationNetwork.png?raw=true)

### Community Detection

In network science, a community in a graph is defined as a subset of nodes that are densely connected to each other and loosely connected to the nodes in the other communities in the same graph. In terms of the shared doctor organization network constructed in this project, a healthcare organization community can be viewed as a group of organizations that contain a closed set of common doctors.

We used the [Louvain method](<https://github.com/taynaud/python-louvain>) to detect communities in the network constructed. The fitness metrics for each community are calculated and displayed in the app when that community is selected. Please refer to the [Helper](#helper) section for the defintions of these fitness metrics.

![alt text](https://github.com/yanpingliu2021/msds490_group/blob/master/data/images/community_detection.png?raw=true)

### Data Source

The data used in this project were downloaded from the Centers for Medicare & Medicaid Services (CMS). The data is part of the source data used to populate the [Medicare Care Compare Tool](<https://www.medicare.gov/care-compare/>). The Compare Tool was launched by CMS in 2020 to provide a web interface that patients and caregivers can use to make informed decisions about healthcare based on cost, quality of care, volume of services, and other data. The data contains various clinicians' performance ratings collected in the [CMS Quality Payment Program](<https://qpp.cms.gov/>).

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

## Future Directions
### Application enhancements
* Add ability to filter by doctor specialties
* Add ability to look up community by doctor or organization
* Improve loading speed of visualizations
* Add a help page with data dictionary
### Long term enhancements
* Develop a doctor network based on shared patients (rather than shared affiliations)


## Helper

Community fitness functions: <https://cdlib.readthedocs.io/en/latest/reference/evaluation.html></br>
Doctor performance metrics: <https://www.cms.gov/medicare/quality-initiatives-patient-assessment-instruments/compare-dac>
