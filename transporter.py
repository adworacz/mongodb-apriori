"""The Transporter

Reads in data from json and stores it into a mongodb instance."""
import json
from pymongo import Connection


# Load file for parsing.
def loadJSON(file_name):
    with open(file_name) as fd:
        jsonDoc = json.load(fd)
        return jsonDoc


documents = loadJSON("gene_data.json")
# Get a connection to mongod.
connection = Connection()

# Lazy create a database in mongo called 'brovine-testdb'.
db = connection['brovine-testdb']

# Create a collection (aka. table) in the brovine db called 'genes'.
genes = db.genes

for genebucket in documents:
    print "Inserting %s into 'gene' collection." % genebucket['genename']
    genes.insert(genebucket)

connection.close()
