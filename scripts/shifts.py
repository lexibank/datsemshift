from pycldf import Dataset
from collections import defaultdict
from pathlib import Path
from csvw.dsv import UnicodeWriter
from warnings import warn

ds = Dataset.from_metadata(Path(__file__).parent.parent / "cldf/cldf-metadata.json")


concepts = ds.objects("ParameterTable")
pairs = defaultdict(lambda : {"directed": {}, "undirected": {}})
for concept in concepts:
    if concept.data["Concepticon_ID"]:
        if concept.data["Target_Concepts"]:
            print(concept.data["Concepticon_Gloss"])
            for t in concept.data["Target_Concepts"]:
                target = concepts[t["ID"]]
                if target.data["Concepticon_Gloss"]:
                    print("  →", target.data["Concepticon_Gloss"])
                    pairs[concept.id, t["ID"]]["directed"] = t
            for t in concept.data["Linked_Concepts"]:
                target = concepts[t["ID"]]
                if target.data["Concepticon_Gloss"]:
                    pairs[concept.id, t["ID"]]["undirected"] = t

# check if polysemy goes in both directions

table = [[
    "Source_ID",
    "Target_ID",
    "Source_CID",
    "Source_Gloss",
    "Target_CID",
    "Target_Gloss",
    "Polysemy",
    "Derivation",
    "Undirected_Polysemy",
    "Undirected_Derivation"
    ]]


visited = set()
for (cA, cB), d in pairs.items():
    # get values
    ab_polysemy, ab_derivation = (
            d["directed"].get("Polysemy", 0),
            d["directed"].get("Derivation", 0))
    polysemy, derivation = (
            d["undirected"].get("Polysemy", 0),
            d["undirected"].get("Derivation", 0))

    if (cB, cA) in pairs:
        ba_polysemy, ba_derivation = (
                pairs[cB, cA]["directed"].get("Polysemy", 0),
                pairs[cB, cA]["directed"].get("Derivation", 0))
        polysemy_, derivation_ = (
                pairs[cB, cA]["undirected"].get("Polysemy", 0),
                pairs[cB, cA]["undirected"].get("Derivation", 0))
        if polysemy_ != polysemy:
            warn("Polysemy not symmetric in {0} / {1} / {2} / {3}".format(
                cA, cB, polysemy, polysemy_))
    else:
        ba_polysemy, ba_derivation = 0, 0
    rowA = [
            cA, cB, 
            concepts[cA].data["Concepticon_ID"],
            concepts[cA].data["Concepticon_Gloss"],
            concepts[cB].data["Concepticon_ID"],
            concepts[cB].data["Concepticon_Gloss"],
            ab_polysemy, ab_derivation, polysemy, derivation
            ]
    rowB = [
            cB, cA, 
            concepts[cB].data["Concepticon_ID"],
            concepts[cB].data["Concepticon_Gloss"],
            concepts[cA].data["Concepticon_ID"],
            concepts[cA].data["Concepticon_Gloss"],
            ba_polysemy, ba_derivation, polysemy, derivation
            ]
    table += [rowA, rowB]
with UnicodeWriter(Path(__file__).parent / "dss.tsv", delimiter="\t") as writer:
    for row in table:
        writer.writerow(row)
    
    





