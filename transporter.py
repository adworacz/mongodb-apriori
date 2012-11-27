"""The Transporter

Reads in data from json and stores it into a mongodb instance."""
import json
from pymongo import Connection


# Load file for parsing.
def loadJSON(file_name):
    with open(file_name) as fd:
        jsonDoc = json.load(fd)
        return jsonDoc

documents = loadJSON("buckets_sorted.json")

# Get a connection to mongod.
with Connection() as connection:
    # Lazy create a database in mongo called 'brovine-testdb'.
    db = connection['brovine-testdb']

    # Create a collection (aka. table) in the brovine db called 'genes'.
    for genebucket in documents:
        print "Inserting %s into 'genes' collection." % genebucket['geneid']

        # PyMongo does lazy collection creation, so genes collection is created on first insert.
        db.genes.insert(genebucket)
