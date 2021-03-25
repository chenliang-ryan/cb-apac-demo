# Couchbase APAC Demo
This repository contains the scripts to build a Couchbase demo. It is built by Couchbase APAC SE team, hence we call it Couchbase APAC Demo.

The dataset used in the demo is provided by Google COVID-19 open data project. You can get more details from [Google COVID-19 Open-Data repo](https://github.com/GoogleCloudPlatform/covid-19-open-data).

You can run script ProvisionDemo.py to provision the demo. The script can
* Create Couchbase bucket, GSI indexes and analytics service datasets
* Download data files from Google COVID 19 Open-Data repo and transform it to a more effcienct format for Couchbase
* Load the transformed data files into Couchbase bucket.
* Control the data granularity level to be downloaded, which will be either country level or subregion level.

There are 3 configuration files:
| File | Purpose |
| ---- | ------- |
| <strong>config.json</strong> | This is the primary configuration file, includes the definition of the data files. Type attribute in the data file definition will be used as the document type in Couchbase. Key attribute defines the document key. |
| <strong>bucket-definition.json</strong> | This is the bucket definition file which will be used to provision the Couchbase bucket. |
| <strong>cbas-datasets-definition.json</strong> | This is the Couchbase Analytics Service datasets definition file, which will be used to create Analytics Service dataset. |

<br>

## Recommended citation

Please use the following when citing this project as a source of data:

```
@article{Wahltinez2020,
  author = "O. Wahltinez and others",
  year = 2020,
  title = "COVID-19 Open-Data: curating a fine-grained, global-scale data repository for SARS-CoV-2",
  note = "Work in progress",
  url = {https://goo.gle/covid-19-open-data},
}
```