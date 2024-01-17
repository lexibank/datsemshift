from pycldf import Dataset
from collections import defaultdict

ds = Dataset.from_metadata("../cldf/cldf-metadata.json")

table = [[
    "Source_Concept",
    "Target_Concept",
    "Derivation",
    "Polysemy",
    "Undirected"
    ]]

concepts = ds.objects("ParameterTable")
pairs = defaultdict(lambda : {"direction": {}, "undirected": {}})
for concept in concepts:
    if concept.data["Concepticon_ID"]:
        if concept.data["Target_Concepts"]:
            print(concept.data["Concepticon_Gloss"])
            for t in concept.data["Target_Concepts"]:
                target = concepts[t["ID"]]
                if target.data["Concepticon_Gloss"]:
                    print("  →", target.data["Concepticon_Gloss"])
                    pairs[concept.id, t["ID"]]["direction"] = t
            for t in concept.data["Linked_Concepts"]:
                target = concepts[t["ID"]]
                if target.data["Concepticon_Gloss"]:
                    pairs[concept.id, t["ID"]]["undirected"] = t

for cA, cB in pairs:

    rowA, rowB = [], []
    





