# real timER

The goal of my Insight Data Engineering project was to develop a platform for low-latency entity resolution built on a real dataset with a plethora of underlying inconsistencies within identification colemns, which makes deduplicating a nontrivial task.

## Table of contents
1. [Introduction](README.md#introduction)
3. [Building a pipeline for low-latency ER](README.md#data-pipeline)
    * [Data set of choice](README.md#data-set)
    * [Data production](README.md#data-production)
    * [Data ingestion](README.md#data-ingestion)
    * [Enitity Resolution algorithms](README.md#entity-resolution)
4. [Challenges](README.md#challenges)
    * [approxSimilarityJoin](README.md#computation)
    * [Spark Memory](README.md#storage)
5. [Technologies](README.md#technologies)

## Introduction

Entity resolution is the complex task of matching real world objects with all of their virtual identities. This is a task that needs to be done over different time scales and for a variety of industries. A few examples are 
    * Deduplication of web site results after using a search bar
    * Developing a greater understanding of a consumer by tracking them across all the different devices that they use to shop and browse (e.g. laptop, desktop, mobile device, etc.)
    * Providing companies with complex global supply-chains with a better understanding of their subsidiaries, by relating their global identity to their local identity through news articles. 

This project is an attempt to use two algorithms (minHashLSH and fuzzy string matching) at low latecny to maintain a list of all individual contributors that have donated to a political committee by comparing a stream of new transactions to a corpus of all the unique contributors. The data set being used is real, which allows me to attempt to build a proof of concept for non-trivial deduplication, even if the data being used does not demand low latency.

## Building a pipeline for low latency ER

### Data set of choice

The complete data of the Federal Election Commission can be found on the [FEC website](https://www.fec.gov/data/advanced/?tab=bulk-data) for the years 1980-2018. The dataset being used in this work is the Individual Contributions set. This is a list of each transaction between an individual and a political committee since 1980. The millions of transactions, with varying levels of information in the identification fields, serves as a good data set to build a proof-of-concept model about low-latency Entity Resolution.

### Data production

A kafka producer is created to read in data from the pipe-delimited files online, and create a pseudo-stream of events. The values within the columns are unchanged in this stage of the project. The kafka producer can be found in the kafka folder of this repo titled kafka_producer_fec2.py

### Data ingestion

The data is then read by a Spark consumer, which is the bulk of this project. Spark is used for its highly distributed capabilities, as well as the implementation of machine learning libraries that could be useful for building upon this platform. In this case Spark Structured Streaming is the context of choice, as it allows for mutable dataframes to be created for the incoming stream, and allows workload to change with event frequency.

As a side note, this choice fo Spark Structured Streaming required me to write my consumer in Scala, as pyspark does not yet support an API for sinking outputs to RDS like PostgreSQL.

### Entity Resolution Algorithms

## Challenges

### approxSimilarityJoin

### Spark Memory

## Technologies

Taking into account the unique challenges of my project and technical trade-offs that had to be made when working on it, I designed the following data pipeline for my application:

1. A Kafka producer which populates a Kafka-topic with transactions loaded from the FEC individual contributions dataset.
2. Consume the Kafka topic with Spark Structured Streaming, and identify the incoming non-trivial duplicates.
3. Write the unique contributors in the stream to a PostgreSQL database table and write the possible duplicates to another table.

