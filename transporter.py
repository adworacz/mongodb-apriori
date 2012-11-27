"""The Transporter

Reads in data from json and stores it into a mongodb instance."""
import json
from pymongo import Connection


# Load file for parsing.
def loadJSON(file_name):
    with open(file_name) as fd:
        jsonDoc = json.load(fd)
        return jsonDoc


documents = loadJSON("buckets.json")
# Get a connection to mongod.
connection = Connection()

# Lazy create a database in mongo called 'brovine-testdb'.
db = connection['brovine-testdb']

# Create a collection (aka. table) in the brovine db called 'genes'.
genes = db.genes2

for genebucket in documents:
    print "Inserting %s into 'gene' collection." % genebucket['geneid']
    genes.insert(genebucket)

connection.close()
