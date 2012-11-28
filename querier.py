from pymongo import Connection
from bson.code import Code
import itertools


minimum_bucket_occurences = 130  # sets the minimum number of buckets that a set of items must occur in.
maximum_bucked_occurences = 148  # sets the maximum number of buckets that a set of items must occur in.

# It's important to note that this Map function relies on the basket data being sorted.
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
    """If the provided list matches the first n-1 elements of
    another element in the provided list of lists, yield their combination.

    Ex:
    inSet = [1, 2, 3]
    inList = [[1, 2, 5], [...], ...]

    Since the first 2 (note: n - 1) elements of [1, 2, 3] and [1, 2, 5] match,
    return the resulting joint list.

    result = [1, 2, 3, 5]
    """
    for element in inList:
        if inSet[:-1] == element[:-1]:
            yield inSet + [element[-1]]


def getInitialCandidates(db):
    # Get all the documents.
    results = db.genes.find()

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
    db.genes.map_reduce(mapFunc, reduceFunc, "tempCandidates", scope={'candidates': candidates})

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

    finalCandidates = []
    while len(nextCandidates) > 0:
        print "Running with %d possible candidates." % len(nextCandidates)
        nextCandidates = getNextCandidates(db, nextCandidates)

        if len(nextCandidates) > 0:
            finalCandidates = nextCandidates

    print finalCandidates
