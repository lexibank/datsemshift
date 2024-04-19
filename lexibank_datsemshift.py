import pathlib
import attr
from clldutils.misc import slug
from pylexibank import Dataset as BaseDataset
from pylexibank import progressbar as pb
from pylexibank import Language, Lexeme, Concept
from pylexibank import FormSpec
from csvw.dsv import UnicodeWriter
import re
from collections import defaultdict


DOWNLOAD = False

def refine_gloss(gloss):
    for s, t in [
            ("&lt;", ""),
            ("&gt;", ""),
            ("&#39;", "'"),
            ("ZQ", "")
            ]:
        gloss = gloss.replace(s, t)
    return gloss


@attr.s
class CustomConcept(Concept):
    Linked_Concepts = attr.ib(
            default=None,
            metadata={"datatype": "json"})
    Target_Concepts = attr.ib(
            default=None,
            metadata={"datatype": "json"})
    Shifts = attr.ib(
            default=None,
            metadata={"datatype": "string", "separator": " "})
    Number = attr.ib(default=None, metadata={"datatype": "integer"})
    Gloss_in_Source = attr.ib(default=None)
    Definition = attr.ib(default=None)
    Alias = attr.ib(default=None)
    Domain = attr.ib(default=None)



@attr.s
class CustomLexeme(Lexeme):
    Source_Lexeme = attr.ib(
            default=None,
            metadata={"datatype": "string"})
    Source_Relation = attr.ib(default=None)
    Shifts = attr.ib(
            default=None,
            metadata={"datatype": "string", "separator": " "})
    Concepts_in_Source = attr.ib(
            default=None,
            metadata={"datatype": "string", "separator": " // "})
    Shift_Types = attr.ib(
            default=None,
            metadata={"datatype": "string", "separator": " "})



@attr.s
class CustomLanguage(Language):
    SubGroup = attr.ib(default=None)
    Words = attr.ib(default=None, metadata={"datatype": "integer"})

class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "datsemshift"
    language_class = CustomLanguage
    concept_class = CustomConcept
    lexeme_class = CustomLexeme
    form_spec = FormSpec(separators="~;,/", missing_data=["∅"], first_form_only=True)
    
    def cmd_download(self, args):
        if DOWNLOAD:
            for letter in pb("abcdefghijklmnopqrstuvwxyz", desc="downloading meanings"):
                self.raw_dir.download(
                        "https://datsemshift.ru/meanings/{0}".format(letter),
                        "raw/raw-data/datsemshift-concepts/{0}.html".format(letter)
                        )
            args.log.info("downloaded concepts")
            self.raw_dir.download(
                    'https://datsemshift.ru/languages',
                    "raw/raw-data/languages.html")
            args.log.info("downloaded languages")
            self.raw_dir.download(
                    'https://datsemshift.ru/browse',
                    "raw/raw-data/shifts.html")
            args.log.info("downloaded shift overview")
            for i in pb(range(1, 8648), desc="downloading shifts"):
                self.raw_dir.download(
                        "https://datsemshift.ru/shift{0}".format(
                            str(i).rjust(4, "0")),
                        "raw/raw-data/datsemshift-data/shift{0}.html".format(
                            str(i).rjust(4, "0")))
            args.log.info("downloaded all shifts")
        

        args.log.info("assembling languages...")
        correct_glottolog = {
                "kirikiri1256": "",
                "wuz1236": "",
                "none": "",
                "Na-Dene": "",
                "class1250": "",
                "None": "",
                "hmong1333": "hmon1333",
                "Afroasiatic": "",
                "litel1248": "lite1248",
                "sout 2976": "sout2976",
                "cccc": "",
                "Kra-Dai": "",
                "taa1242": "",
                "soo1256": "",
                "blaan1241": "",
                "Kra–Dai": "",
                "middl1321": "",
                
                }
        with open(self.raw_dir / "raw-data" / "languages.html") as f:
            languages = f.read()
        language_table = [['ID', "Name", "Glottocode", "Family", "SubGroup", "Words"]]
        all_languages = re.findall("<tr[^>]*>(.*?)</tr>", languages, re.DOTALL)
        for lng in all_languages:
            tabs = re.findall("<td[^>]*>(.*?)</td>", lng)
            if tabs:
                idf, name, glottolog, family, sgr, words = (
                        tabs[1].strip(), tabs[2].strip(), tabs[3].strip(),
                        tabs[4].strip(), tabs[5].strip(), tabs[6].strip())
                language_table += [[idf, name, correct_glottolog.get(
                    glottolog, glottolog), family, sgr, words]]
        lidx = max([int(row[0]) for row in language_table[1:]]) + 1
        language_lookup = {row[1]: row[0] for row in language_table[1:]}
        args.log.info('... assembled languages')

        args.log.info('assembling concepts...')
        concept_table = [["NUMBER", "ENGLISH", "GLOSS_IN_SOURCE", "DEFINITION", "ALIAS", "DOMAIN"]]
        cidx = 1
        concept_lookup = {}
        for pth in pb(self.raw_dir.glob("raw-data/datsemshift-concepts/*.html"), desc="loading concepts"):
            with open(pth) as f:
                data = f.read()
            for row in re.findall("<tr[^>]*?>(.*?)</tr>", data,re.DOTALL):
                if "<td" in row:
                    tabs = re.findall("<td[^>]*>(.*?)</td>", row, re.DOTALL)
                    if tabs:
                        gloss, definition, alias, taxon = tabs[0], tabs[1], tabs[2], tabs[3]
                        concept_table += [[
                            cidx, 
                            refine_gloss(gloss),
                            gloss.strip(), 
                            definition.replace("\n", " ").strip(),
                            alias.strip(),
                            taxon.strip()
                            ]]
                        concept_lookup[gloss.strip()] = cidx
                        cidx += 1
        args.log.info("... assembled concepts")
        args.log.info("assembling shifts...")
        table = [["ID", "Shift_ID", "Type", "Realization", "Status",
                  "Direction", "Source_Concept", "Source_Concept_ID",
                  "Target_Concept", "Target_Concept_ID", "Source_Language",
                  "Source_Language_ID", "Target_Language",
                  "Target_Language_ID", "Source_Meaning", "Target_Meaning",
                  "Source_Word", "Target_Word",
                 ]]
        base_shifts = [["ID", "Source", "Source_Number", "Direction", "Target",
                        "Target_Number", "Realizations", "Examples"]]
        idx = 1
        count = 0
        for pth in pb(self.raw_dir.glob("raw-data/datsemshift-data/shift*.html"), desc="loading data"):
            with open(pth) as f:
                data = f.read()
            shift_id = str(pth).split("/")[-1][:-5]
            shifts = re.findall(
                    '<table class="realization__table"[^>]*>(.*?)</table>',
                    data,
                    re.DOTALL
                    )
            shift_header = re.findall('<div class="shift__header">(.*?)</div>', 
                                      data,
                                      re.DOTALL)

            if shift_header:
                shift_header = shift_header[0]
                source, direction, target = re.findall('<span class="shift__header_item"[^>]*>(.*?)</span>',
                                     shift_header,
                                     re.DOTALL)
                source, direction, target = source.strip(), direction.strip(), target.strip()
                try:
                    sidx, tidx = concept_lookup[source], concept_lookup[target]
                except KeyError:
                    if source not in concept_lookup:
                        concept_lookup[source] = cidx
                        concept_table += [[
                            cidx,
                            refine_gloss(source),
                            source,
                            "", 
                            "",
                            ""]]
                        cidx += 1
                    if target not in concept_lookup:
                        concept_lookup[target] = cidx
                        concept_table += [[
                            cidx,
                            refine_gloss(target),
                            target,
                            "", 
                            "",
                            ""]]
                        cidx += 1
                    sidx, tidx = concept_lookup[source], concept_lookup[target]
                realizations = re.findall(
                        '<span class="realization_number">(.*?)</span>',
                        shift_header,
                        re.DOTALL
                        )[0].replace(" realizations", "").replace(" realization", "").strip()
                base_shifts += [[shift_id, source.strip(), sidx,
                                 direction.strip(), target.strip(), tidx, realizations,
                                 len(shifts)]]
                for shift in shifts:
                    count += 1
                    title_ = re.findall(
                            '<th[^>]*>(.*?)</th>',
                            shift,
                            re.DOTALL)[0].strip()
                    if "<span" in title_:
                        status, title = re.findall(
                                    '<span[^>]*>(.*?)</span>.*?Realization (.*?)$',
                                    title_,
                                    re.DOTALL)[0]
                    else:
                        status, title = "", title_.strip()
                    status, title = status.strip(), title.strip()
        
        
                    # get the type of the shift
                    shift_type = re.findall(
                            '<tr>.*?<td colspan="2">Type</td>.*?<td[^>]*>(.*?)</td>.*?</tr>',
                            shift,
                            re.DOTALL)[0].strip()
        
                    languages = re.findall(
                            '<tr>.*?<td colspan="2">(Language *[12]*)</td>.*?<td[^>]*>(.*?)</td>.*?</tr>',
                            shift,
                            re.DOTALL)
                    lexemes = re.findall(
                             '<tr>.*?<td colspan="2">(Lexeme *[12]*)</td>.*?<td[^>]*>(.*?)</td>.*?</tr>',
                             shift,
                             re.DOTALL)
                    meanings = re.findall(
                             '<tr>.*?<td[^>]*>(Meaning *[12]*)</td>.*?<td[^>]*>(.*?)</td>.*?<td[^>]*>.*?</td>.*?</tr>',
                             shift,
                             re.DOTALL)
                    direction = re.findall(
                             '<tr>.*?<td[^>]*>Direction</td>.*?<td[^>]*>(.*?)</td>.*?<td[^>]*>.*?</td>.*?</tr>',
                             shift,
                             re.DOTALL)
                    if not direction:
                        direction = "?"
                    else:
                        direction = direction[0]
                    
                    lids = []
                    for _, language in languages:
                        if language not in language_lookup:
                            language_table += [[
                                lidx,
                                refine_gloss(language),
                                "", "", "", 0]]
                            language_lookup[language] = lidx
                            lids += [lidx]
                            lidx += 1
                        lids += [language_lookup[language]]
        
                    # first simple case, one language, one lexeme, two meanings
                    if len(languages) == 1 and len(lexemes) == 1 and len(meanings) == 2:
                        table += [[
                            idx,
                            shift_id,
                            shift_type,
                            title,
                            status,
                            direction,
                            source,
                            sidx,
                            target,
                            tidx,
                            refine_gloss(languages[0][1].strip()),
                            lids[0],
                            refine_gloss(languages[0][1].strip()),
                            lids[0],
                            meanings[0][1].strip(), 
                            meanings[1][1].strip(),
                            lexemes[0][1].strip(),
                            lexemes[0][1].strip()
                            ]]
                    elif len(languages) == 1 and len(lexemes) == 2 and len(meanings) == 2:
                        table += [[
                            idx,
                            shift_id,
                            shift_type,
                            title,
                            status,
                            direction,
                            source,
                            sidx,
                            target,
                            tidx,
                            refine_gloss(languages[0][1].strip()),
                            lids[0],
                            refine_gloss(languages[0][1].strip()),
                            lids[0],
                            meanings[0][1].strip(), 
                            meanings[1][1].strip(),
                            lexemes[0][1].strip(),
                            lexemes[1][1].strip()
                            ]]
                    elif len(languages) == 2 and len(lexemes) == 2 and len(meanings) == 2:
                        table += [[
                            idx,
                            shift_id,
                            shift_type,
                            title,
                            status,
                            direction,
                            source,
                            sidx,
                            target,
                            tidx,
                            refine_gloss(languages[0][1].strip()),
                            lids[0],
                            refine_gloss(languages[1][1].strip()),
                            lids[1],
                            meanings[0][1].strip(), 
                            meanings[1][1].strip(),
                            lexemes[0][1].strip(),
                            lexemes[1][1].strip()
                            ]]
                    elif len(languages) == 2 and len(lexemes) == 1 and len(meanings) == 2:
                        table += [[
                            idx,
                            shift_id,
                            shift_type,
                            title,
                            status,
                            direction,
                            source,
                            sidx,
                            target,
                            tidx,
                            refine_gloss(languages[0][1].strip()),
                            lids[0],
                            refine_gloss(languages[1][1].strip()),
                            lids[1],
                            meanings[0][1].strip(), 
                            meanings[1][1].strip(),
                            lexemes[0][1].strip(),
                            lexemes[0][1].strip()
                            ]]
                    idx += 1

        args.log.info("... assembled shifts")
    
        with UnicodeWriter(self.raw_dir / "shifts.tsv", delimiter="\t") as writer:
            for row in base_shifts:
                writer.writerow(row)
        
        with UnicodeWriter(self.raw_dir / "lexemes.tsv", delimiter="\t") as writer:
            for row in table:
                writer.writerow(row)
        with UnicodeWriter(self.etc_dir / "languages.tsv", delimiter="\t") as writer:
            for row in language_table:
                row[1] = refine_gloss(row[1])
                writer.writerow(row)
        with UnicodeWriter(self.etc_dir / "concepts.tsv", delimiter="\t") as writer:
            for row in concept_table:
                writer.writerow(row)

    def cmd_makecldf(self, args):
        # add bib
        args.writer.add_sources()
        args.log.info("added sources")

        # concepts to concepticon id
        c2i = {}
        for c in self.conceptlists[0].concepts.values():
            if c.english not in c2i:
                c2i[c.english] = (c.concepticon_id, c.concepticon_gloss)

        # load concepts
        # assemble data on shifts in concept before
        concepts = {}
        concept_names = {}
        concepts_to_add = {}
        unify_concepts = {}
        for row in self.etc_dir.read_csv("unify_concepts.tsv", delimiter="\t", dicts=True):
            for lexeme in row["LEXEME"].split(" // "):
                unify_concepts[lexeme] = row["NUMBER"]

        for concept in self.conceptlists[0].concepts.values():
            if not concept.english.startswith("*"):
                idx = concept.number + "_" + slug(concept.english)
                cid, cgl = c2i.get(concept.english, ("", ""))

                concepts_to_add[idx] = {
                        "ID": idx,
                        "Name": concept.english,
                        "Gloss_in_Source": concept.attributes["gloss_in_source"],
                        "Concepticon_ID": concept.concepticon_id,
                        "Concepticon_Gloss": concept.concepticon_gloss,
                        "Number": concept.number,
                        "Alias": concept.attributes["alias"],
                        "Domain": concept.attributes["domain"],
                        "Definition": concept.attributes["definition"]
                        }
                concepts[concept.number] = idx
                concept_names[idx] = concept.english
                unify_concepts[concept.english] = concept.number
        args.log.info(len(concepts_to_add))

        # load languages
        languages = args.writer.add_languages(lookup_factory="Name")
        lang2fam = {}
        for language in self.languages:
            lang2fam[language["Name"]] = language["Family"]

        language_data = defaultdict(list)

        # load individual semantic shifts
        shifts = self.raw_dir.read_csv("lexemes.tsv", delimiter="\t",
                                       dicts=True)
        targets = {
                concept["ID"]: defaultdict(
                    lambda : {
                        "Polysemy_Lexemes": [],
                        "Derivation_Lexemes": [],
                        "Polysemy_Shifts": [],
                        "Derivation_Shifts": [],
                        "Polysemy_Families": [],
                        "Derivation_Families": [],
                        "Polysemy": 0,
                        "Derivation": 0,
                        }) for concept in concepts_to_add.values()}
        links = {
                concept["ID"]: defaultdict(
                    lambda : {
                        "Polysemy_Lexemes": [],
                        "Derivation_Lexemes": [],
                        "Polysemy_Shifts": [],
                        "Derivation_Shifts": [],
                        "Polysemy_Families": [],
                        "Derivation_Families": [],
                        "Polysemy": 0,
                        "Derivation": 0}) for concept in concepts_to_add.values()}

        
        args.log.info("unified concepts from {0} to {1}".format(len(concepts_to_add), len(set(unify_concepts.values()))))
        lexeme_data, lexeme_graph = {}, {}
        for row in shifts:
            source_concept, target_concept = (
                concepts[unify_concepts[row["Source_Concept"]]],
                concepts[unify_concepts[row["Target_Concept"]]])
            
            source_id = (
                    source_concept, 
                    row["Source_Language_ID"],
                    row["Source_Word"])
            target_id = (
                    target_concept,
                    row["Target_Language_ID"],
                    row["Target_Word"])
            source_lexeme = (
                    1, row["ID"], source_concept, row["Source_Language_ID"],
                    row["Source_Word"], row["Source_Meaning"], row["Shift_ID"],
                    row["Type"])
            target_lexeme = (
                    2, row["ID"], target_concept, row["Target_Language_ID"],
                    row["Target_Word"], row["Target_Meaning"], row["Shift_ID"],
                    row["Type"], )
            

            if row["Type"] in ["Polysemy", "Derivation"]:
                if row["Direction"] == "→":
                    pass
                elif row["Direction"] == "←":
                    source_id, target_id = target_id, source_id
                    source_lexeme, target_lexeme = target_lexeme, source_lexeme

                if source_id not in lexeme_graph:
                    lexeme_graph[source_id] = []
                
                lexeme_graph[source_id] += [(target_id, row["Type"])]
                if source_id not in lexeme_data:
                    lexeme_data[source_id] = []
                if target_id not in lexeme_data:
                    lexeme_data[target_id] = []
                
                lexeme_data[source_id] += [source_lexeme]
                lexeme_data[target_id] += [target_lexeme]
            
            if row["Direction"] == "→":
                if row["Type"] in ["Polysemy", "Derivation"]:
                    targets[source_concept][
                            target_concept][row["Type"]+"_Lexemes"] += [row["ID"]]
                    targets[source_concept][
                            target_concept][row["Type"]] += 1
                    targets[source_concept][
                            target_concept][row["Type"]+"_Shifts"] += [row["Shift_ID"]]
                    targets[source_concept][
                            target_concept][row["Type"]+"_Families"] += [lang2fam[row["Source_Language"]]]


            if row["Direction"] == "←":
                if row["Type"] in ["Polysemy", "Derivation"]:
                    targets[target_concept][
                            source_concept][row["Type"]+"_Lexemes"] += [row["ID"]]
                    targets[target_concept][
                            source_concept][row["Type"]] += 1
                    targets[target_concept][
                            source_concept][row["Type"]+"_Shifts"] += [row["Shift_ID"]]
                    targets[target_concept][
                            source_concept][row["Type"]+"_Families"] += [lang2fam[row["Source_Language"]]]
            if row["Direction"] in ["?", "-", "—"]:
                if row["Type"] in ["Polysemy", "Derivation"]:
                    links[target_concept][
                            source_concept][row["Type"]+"_Lexemes"] += [row["ID"]]
                    links[target_concept][
                            source_concept][row["Type"]] += 1
                    links[source_concept][
                            target_concept][row["Type"]+"_Lexemes"] += [row["ID"]]
                    links[source_concept][
                            target_concept][row["Type"]] += 1
                    links[target_concept][
                            source_concept][row["Type"]+"_Shifts"] += [row["Shift_ID"]]
                    links[source_concept][
                            target_concept][row["Type"]+"_Shifts"] += [row["Shift_ID"]]
                    links[source_concept][
                            target_concept][row["Type"]+"_Families"] += [lang2fam[row["Source_Language"]]]
                    links[target_concept][
                            source_concept][row["Type"]+"_Families"] += [lang2fam[row["Source_Language"]]]

        args.log.info(len(concepts_to_add))
        args.log.info(len(set([c["ID"] for c in concepts_to_add.values()])))
        for concept in pb(concepts_to_add.values(), desc="adding concepts"):
            target_list = []
            link_list = []
            for target_id, values in targets[concept["ID"]].items():
                target_list += [
                        {"ID": target_id, "NAME": concept_names[target_id], 
                         "Polysemy": values.get("Polysemy", 0),
                         "Derivation": values.get("Derivation", 0),
                         "PolysemyByFamily": len(set(values.get("Polysemy_Families"))),
                         "DerivationByFamily": len(set(
                             values.get("Derivation_Families"))),
                         "Polysemy_Lexemes": values.get("Polysemy_Lexemes", []),
                         "Derivation_Lexemes": values.get("Derivation_Lexemes", []),
                         "Polysemy_Shifts": values.get("Polysemy_Shifts", []),
                         "Derivation_Shifts": values.get("Derivation_Shifts", []),
                         "Polysemy_Families": values.get("Polysemy_Families", []),
                         "Derivation_Families": values.get("Derivation_Families")
                         }
                        ]
            for target_id, values in links[concept["ID"]].items():
                link_list += [
                        {"ID": target_id, "NAME": concept_names[target_id],
                         "Polysemy": values.get("Polysemy", 0),
                         "Derivation": values.get("Derivation", 0),
                         "PolysemyByFamily": len(set(values.get("Polysemy_Families"))),
                         "DerivationByFamily": len(set(
                             values.get("Derivation_Families"))),
                         "Polysemy_Lexemes": values.get("Polysemy_Lexemes", 0),
                         "Derivation_Lexemes": values.get("Derivation_Lexemes", 0),
                         "Polysemy_Shifts": values.get("Polysemy_Shifts", []),
                         "Derivation_Shifts": values.get("Derivation_Shifts", []),
                         "Polysemy_Families": values.get("Polysemy_Families", []),
                         "Derivation_Families": values.get("Derivation_Families")
                         }]

                
            concept["Target_Concepts"] = target_list
            concept["Linked_Concepts"] = link_list
            args.writer.add_concept(**concept)
        
        visited = set()
        node2id = {}
        idx = 1
        source_nodes = set()
        for source_node, target_nodes in pb(lexeme_graph.items()):
            if source_node not in node2id:
                node2id[source_node] = idx
                idx += 1
            sidx = node2id[source_node]
            source_nodes.add(source_node)
            for target_node, relation in target_nodes:
                if target_node not in node2id:
                    node2id[target_node] = idx
                    idx += 1
                tidx = node2id[target_node]
                # write target nodes first

                d = lexeme_data[target_node]
                if target_node not in visited:
                    concepts = [x[5] for x in d]
                    shifts = [x[6] for x in d]
                    types = [x[7] for x in d]
                    args.writer.add_form(
                            Language_ID=d[0][3],
                            Parameter_ID=d[0][2],
                            Local_ID=tidx,
                            Value=d[0][4],
                            Form=d[0][4],
                            Concepts_in_Source=concepts,
                            Shifts=shifts,
                            Shift_Types=types,
                            Source_Lexeme=sidx,
                            Source_Relation=relation
                            )
                    visited.add(target_node)
        for source_node in source_nodes:
            if source_node not in visited:
                d = lexeme_data[source_node]
                concepts = [x[5] for x in d]
                shifts = [x[6] for x in d]
                types = [x[7] for x in d]
                args.writer.add_form(
                        Language_ID=d[0][3],
                        Parameter_ID=d[0][2],
                        Local_ID=node2id[source_node],
                        Value=d[0][4],
                        Form=d[0][4],
                        Concepts_in_Source=concepts,
                        Shifts=shifts,
                        Shift_Types=types,
                        Source_Lexeme="",
                        Source_Relation=""
                        )





#            source_lexeme = (
#                    1, row["ID"], source_concept, row["Source_Language_ID"],
#                    row["Source_Word"], row["Source_Meaning"], row["Shift_ID"],
#                    row["Type"])

        #for (c, l, w), values in pb(language_data.items()):
        #    #if len(values) > 1:
        #    #    args.log.info("found duplicate for {0} / {1} / {2}".format(c, l, w))
        #    shifts = [c["Shift_ID"] for c in values]
        #    concepts = [c["Gloss"] for c in values]
        #    lids = [c["Local_ID"] for c in values]

        #    args.writer.add_form(
        #            Language_ID=l,
        #            Parameter_ID=c,
        #            Local_IDS=lids,
        #            Value=w,
        #            Form=w,
        #            Concepts_in_Source=concepts,
        #            Shifts=shifts,
        #            Source="DatSemShift"
        #            )
                    



