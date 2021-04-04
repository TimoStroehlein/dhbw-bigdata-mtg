# Big Data - MTG API

Implementation of an ETL Workflow with Airflow, downloading all cards from the MTG API, exporting the cards to MongoDB and make the cards accessible through a Node.js React App.

> Exam of the lecture Big Data at the University DHBW Stuttgart

## Table of contents
- [Installation](#installation)
- [Links](#links)
- [ETL Workflow - Airflow](#etl-workflow-with-airflow)
    - [hdfs_mkdir_raw_cards](#hdfs_mkdir_raw_cards)
    - [hdfs_mkdir_final_cards](#hdfs_mkdir_final_cards)
    - [pyspark_download_cards](#pyspark_download_cards)
    - [pyspark_format_cards](#pyspark_format_cards)
    - [pyspark_export_cards_to_mongodb](#pyspark_export_cards_to_mongodb)
- [Backend](#backend)
- [Frontend](#frontend)

## Installation

Start all containers.

```bash
# Default
docker-compose up -d

# Development
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

When running the development script, you need to run npm install locally first. Also the local Node version must be Node.js 15. The development scripts allows Hot Reloading without recreating the container and Dockerfiles.

```bash
npm install
```

Wait until container startup finished.

```bash
docker-compose logs hadoop
```

```bash
[...]
Stopping nodemanagers
Stopping resourcemanager
Container Startup finished.
```

Start the hadoop cluster.

```bash
docker exec -it hadoop bash
sudo su hadoop
cd
start-all.sh
```

Navigate to Airflow (http://external-ip:8080/), start the DAG and then pray and wait :pray::hourglass_flowing_sand:.

## Links

The following ports should now be accessible (if added to the allowed ports within the firewall, when running in gcloud):

* Airflow: http://external-ip:8080/
* Hadoop: http://external-ip:8088/
* HDFS: http://external-ip:9870/
* Frontend: http://external-ip:3030/
* Mongo Express: http://external-ip:8081/

## Containers

The project consists of following containers:

* **Frontend** (Node.js + React)
* **Backend** (Node.js + Express)
* **Hadoop** (Hadoop FS, Yarn, PySpark, ...)
* **Airflow** (Airflow + PostgreSQL)
* **Mongo** (MongoDB)
* **Mongo Express** (MongoDB admin interface)

## ETL Workflow with Airflow

Airflow contains one DAG called MTG, that performs all steps. All files are stored within HDFS and operations on those files are being executed with PySpark.

The MTG DAG performs the following steps:

### hdfs_mkdir_raw_cards

Create the download directory for the raw cards within HDFS, located at:

```bash
/user/hadoop/mtg/raw/year/month/day/

# For example
/user/hadoop/mtg/raw/2021/04/03/
```

### hdfs_mkdir_final_cards

Create the final directory for the cards within HDFS, after being formatted with PySpark, located at:

```bash
/user/hadoop/mtg/final/year/month/day/

# For example
/user/hadoop/mtg/final/2021/04/03/
```

### pyspark_download_cards

Download all cards from the MTG API (https://docs.magicthegathering.io/) with PySpark (using the python `requests` library). The MTG API contains roughly 56.000 cards, but the API only delivers up to 100 cards at once, because of paging. That's why it's necessary to perform multiple requests to the API. The current progress will be printend to the logs within Airflow. After successfully downloading all the cards, they are being parallelized, converted to a Spark Dataframe and finally stored and splitted into multiple parts in HDFS within the `/user/hadoop/mtg/raw/year/month/day/` directory.

All the data is stored as json in the following, unformatted format:

```json
{
    "cards": [
        {
            "artist":"Pete Venters",
            "cmc":7.0,
            "colorIdentity":["W"],
            "colors":["White"],
            "..."
        }
    ]
}
```

### pyspark_format_cards

Format and reduce the json data stored in the `raw` directory within HDFS. Follwing changes are being made to the data:

1. Explode the array into single elements (rows)
2. Replace all `null` values with an empty string
3. Remove all unnecessary columns
4. Flatten the subtypes from an array to a comma seperated string
5. Write the data back to the HDFS to the `final` directory (`/user/hadoop/mtg/final/year/month/day/`)

The final format of the cards looks like this:

```json
{
    "name":"Drag Down",
    "subtypes":"",
    "text":"Domain — Target creature gets -1/-1 until end of turn for each basic land type among lands you control.",
    "flavor":"The barbarians of Jund believe the bottomless tar pits extend forever into other, darker worlds.",
    "artist":"Trevor Claxton",
    "multiverseid":"179239",
    "imageUrl":"http://gatherer.wizards.com/Handlers/Image.ashx?multiverseid=179239&type=card"
}
{
    "name":"Dreadwing",
    "subtypes":"Zombie",
    "text":"{1}{U}{R}: Dreadwing gets +3/+0 and gains flying until end of turn.",
    "flavor":"Dreadwings spring from lofty perches to surprise kathari in midflight. They smother their prey and then consume it as they glide gently toward the ground.",
    "artist":"Mark Hyzer",
    "multiverseid":"159629",
    "imageUrl":"http://gatherer.wizards.com/Handlers/Image.ashx?multiverseid=159629&type=card"
}
...
```

### pyspark_export_cards_to_mongodb

Get the final json data from HDFS as a dataframe, convert it to json and export it to MongoDB (using the python package `pymongo`, https://pypi.org/project/pymongo/). Each card will be stored as a single document within the database. The exported data can finally be seen within Mongo Express (http://external-ip:8081/) and searched through within the frontend (http://external-ip:3030/).

## Backend

The backend is written in TypeScript using Node.js and Express as server. In order to connect to MongoDB, the package `mongodb` (https://www.npmjs.com/package/mongodb) is being used.

## Frontend

The frontend is written in TypeScript using Node.js and React. For UI components, React Suite (`rsuite`, https://rsuitejs.com/) is being used, in order to perform requests against the backend, `axios` is being used.
