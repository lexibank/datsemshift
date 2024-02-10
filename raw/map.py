from pyconcepticon import Concepticon
from pysem import to_concepticon
from csvw.dsv import UnicodeDictReader, UnicodeWriter

with UnicodeDictReader("../etc/concepts.tsv", delimiter="\t") as reader:
    data = [row for row in reader]
concepticon = Concepticon()

dss2 = {c.english: (c.concepticon_id, c.concepticon_gloss) for c in
        concepticon.conceptlists["Zalizniak-2020-2590"].concepts.values()}


table = [[
    "NUMBER",
    "ENGLISH",
    "CONCEPTICON_ID",
    "CONCEPTICON_GLOSS",
    "CERTAINTY",
    "GLOSS_IN_SOURCE",
    "DEFINITION",
    "ALIAS",
    "DOMAIN",
    ]]

for row in data:
    if row["ENGLISH"] in dss2:
        cid, cgl = dss2[row["ENGLISH"]]
        val = "100"
    else:
        mappings = to_concepticon([{"gloss": row["ENGLISH"]}])[row["ENGLISH"]]
        if mappings:
            cid, cgl = mappings[0][0], mappings[0][1]
            val = str(mappings[0][3])

        else:
            cid, cgl = "", ""
            val = ""
    table += [[
        row["NUMBER"],
        row["ENGLISH"],
        cid,
        cgl, 
        val,
        row["GLOSS_IN_SOURCE"],
        row["DEFINITION"],
        row["ALIAS"],
        row["DOMAIN"],
        ]]

with UnicodeWriter("concepts-mapped.tsv", delimiter="\t") as writer:
    for row in table:
        writer.writerow(row)
