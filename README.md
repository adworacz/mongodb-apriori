MongoDB Apriori
===============

This code was written (hastily) for some work on finding significant genetic transcription factors in genes.

It has proven to be accurate (enough), and demonstrates the use of the Apriori algorithm within the NoSQL database, MongoDB.

Usage
-----
Load the data using transporter.py (simply edit the loadJSON call to select the right data source.)

`$ python transporter.py`

Run the map/reduce querys to determine significant items using the querier.py.

`$ python querier.py`

Written (hastily, don't judge me) by: Austin Dworaczyk Wiltshire
