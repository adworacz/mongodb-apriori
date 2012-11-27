from pymongo import Connection
from bson.code import Code
import itertools


minimum_bucket_occurences = 120  # sets the minimum number of buckets that a set of items must occur in.
maximum_bucked_occurences = 148  # sets the maximum number of buckets that a set of items must occur in.

mapFunc = Code("""
                function () {
                    var idx = 0;
                    for (var j = 0; j < candidates.length; j++) {
                        var candidate = candidates[j];
                        idx = 0;
                        for (var i = 0; i < this.basket.length; i++) {
                            var item = this.basket[i];
                            if (idx >= candidate.length) {
                                emit(candidate.toString(), 1);
                                break;
                            } else if (item === candidate[idx]) idx++;
                        }
                    }
                }
                """)

reduceFunc = Code("""
                function (key, values) {
                  total = 0
                  values.forEach(function(value){
                    total += value
                  });
                  return total;
                }
                """)


def joinSets(inSet, inList):
    for element in inList:
        if inSet[:-1] == element[:-1]:
            yield inSet + [element[-1]]


def getInitialCandidates(db):
    # Get all the documents.
    results = db.genes2.find()

    # Get all the buckets together.
    allBuckets = [result["basket"] for result in results]

    # Get all the unique items out of all of the buckets.
    allItems = [item for bucket in allBuckets for item in bucket]
    uniqueItems = set(allItems)

    # Get the first set of items that occur more than minimum_bucket_occurences.
    initialCandidates = []
    for item in uniqueItems:
        occurences = allItems.count(item)

        if occurences > minimum_bucket_occurences and occurences < maximum_bucked_occurences:
            initialCandidates.append(item)

    # Get initial combinations
    combinations = itertools.combinations(sorted(initialCandidates), 2)
    initialCandidates = [list(combo) for combo in combinations]

    return initialCandidates


def getNextCandidates(db, candidates):
    # Given the initial candidates, MapReduce to find counts for each occurence.
    db.tempCandidates.drop()
    db.genes2.map_reduce(mapFunc, reduceFunc, "tempCandidates", scope={'candidates': candidates})

    # Take the results and create the next candidate set.
    reducedResults = db.tempCandidates.find({"value": {"$gt": minimum_bucket_occurences, "$lt": maximum_bucked_occurences}})

    # cleanReduceResults = [[int(key) for key in result["_id"].split(",")] for result in reducedResults]
    cleanReduceResults = [result["_id"].split(",") for result in reducedResults]

    nextCandidates = []
    for index, rs in enumerate(cleanReduceResults):
        nextCandidates.extend(list(joinSets(rs, cleanReduceResults[index + 1:])))

    return nextCandidates


with Connection() as connection:
    db = connection['brovine-testdb']

    initialCandidates = getInitialCandidates(db)
    print "initial candidates", len(initialCandidates)

    nextCandidates = getNextCandidates(db, initialCandidates)
    while len(nextCandidates) != 1:
        print "Iteration."
        print len(nextCandidates)
        nextCandidates = getNextCandidates(db, nextCandidates)

    print nextCandidates
