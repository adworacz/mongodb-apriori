"""The Transporter

Reads in data from json and stores it into a mongodb instance."""
import json
from pymongo import MongoClient


# Load file for parsing.
def loadJSON(file_name):
    with open(file_name) as fd:
        jsonDoc = json.load(fd)
        return jsonDoc

documents = loadJSON("buckets_sorted.json")

# Get a connection to mongod.
with MongoClient() as client:
    # Lazy create a database in mongo called 'brovine-testdb'.
    db = client['brovine-testdb']

    for i in range(10):
        # Create a collection (aka. table) in the brovine db called 'genes'.
        for index, genebucket in enumerate(documents):
            # print "Inserting %s into 'genes' collection." % genebucket['geneid']
            # print index, i
            genebucket["_id"] = index + i * 1000
            # PyMongo does lazy collection creation, so genes collection is created on first insert.
            db.genes.insert(genebucket)
