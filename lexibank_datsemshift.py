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



@attr.s
class CustomLexeme(Lexeme):
    Shifts = attr.ib(
            default=None,
            metadata={"datatype": "string", "separator": " "})
    Concepts_in_Source = attr.ib(
            default=None,
            metadata={"datatype": "string", "separator": " // "})



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

        # load concepts
        c2i = {c.english: (c.concepticon_id, c.concepticon_gloss) for c in
               self.conceptlists[0].concepts.values()}
        # load concepts
        # assemble data on shifts in concept before
        concepts = {}
        concept_names = {}
        concepts_to_add = {}
        for concept in self.concepts:
            idx = concept["NUMBER"] + "_" + slug(concept["ENGLISH"])
            cid, cgl = c2i.get(concept["ENGLISH"], ("", ""))
            concepts_to_add[idx] = {
                    "ID": idx,
                    "Name": concept["ENGLISH"],
                    "Concepticon_ID": cid,
                    "Concepticon_Gloss": cgl,
                    }
            concepts[concept["NUMBER"]] = idx
            concept_names[idx] = concept["ENGLISH"]

        # load languages
        languages = args.writer.add_languages(lookup_factory="Name")

        language_data = defaultdict(list)

        # load individual semantic shifts
        shifts = self.raw_dir.read_csv("lexemes.tsv", delimiter="\t",
                                       dicts=True)
        targets = {
                concept["ID"]: defaultdict(
                    lambda : {
                        "Polysemy_Lexemes": [],
                        "Derivation_Lexemes": [],
                        "Polysemy": 0,
                        "Derivation": 0
                        }) for concept in concepts_to_add.values()}
        links = {
                concept["ID"]: defaultdict(
                    lambda : {
                        "Polysemy_Lexemes": [],
                        "Derivation_Lexemes": [],
                        "Polysemy": 0,
                        "Derivation": 0}) for concept in concepts_to_add.values()}


        for row in shifts:
            language_data[
                    concepts[row["Source_Concept_ID"]],
                    row["Source_Language_ID"],
                    row["Source_Word"]
                    ] += [{
                        "Gloss": row["Source_Meaning"],
                        "Parameter_ID": concepts[row["Source_Concept_ID"]],
                        "Language_ID": row["Source_Language_ID"],
                        "Shift_ID": row["Shift_ID"],
                        "Value": row["Source_Word"]
                        }]
            language_data[
                    concepts[row["Target_Concept_ID"]],
                    row["Target_Language_ID"],
                    row["Target_Word"]
                    ] += [{
                        "Gloss": row["Target_Meaning"],
                        "Parameter_ID": concepts[row["Target_Concept_ID"]],
                        "Language_ID": row["Target_Language_ID"],
                        "Shift_ID": row["Shift_ID"],
                        "Value": row["Target_Word"]
                        }]
            if row["Direction"] == "→":
                if row["Type"] in ["Polysemy", "Derivation"]:
                    targets[concepts[row["Source_Concept_ID"]]][
                            concepts[row["Target_Concept_ID"]]][row["Type"]+"_Lexemes"] += [row["ID"]]
                    targets[concepts[row["Source_Concept_ID"]]][
                            concepts[row["Target_Concept_ID"]]][row["Type"]] += 1
            if row["Direction"] == "←":
                if row["Type"] in ["Polysemy", "Derivation"]:
                    targets[concepts[row["Target_Concept_ID"]]][
                            concepts[row["Source_Concept_ID"]]][row["Type"]+"_Lexemes"] += [row["ID"]]
                    targets[concepts[row["Target_Concept_ID"]]][
                            concepts[row["Source_Concept_ID"]]][row["Type"]] += 1
            if row["Direction"] in ["?", "-", "—"]:
                if row["Type"] in ["Polysemy", "Derivation"]:
                    links[concepts[row["Target_Concept_ID"]]][
                            concepts[row["Source_Concept_ID"]]][row["Type"]+"_Lexemes"] += [row["ID"]]
                    links[concepts[row["Target_Concept_ID"]]][
                            concepts[row["Source_Concept_ID"]]][row["Type"]] += 1
                    links[concepts[row["Source_Concept_ID"]]][
                            concepts[row["Target_Concept_ID"]]][row["Type"]+"_Lexemes"] += [row["ID"]]
                    links[concepts[row["Source_Concept_ID"]]][
                            concepts[row["Target_Concept_ID"]]][row["Type"]] += 1




        
        for concept in pb(concepts_to_add.values(), desc="adding concepts"):
            target_list = []
            link_list = []
            for target_id, values in targets[concept["ID"]].items():
                target_list += [
                        {"ID": target_id, "NAME": concept_names[target_id], 
                         "Polysemy": values.get("Polysemy", 0),
                         "Derivation": values.get("Derivation", 0),
                         "Polysemy_Lexemes": values.get("Polysemy_Lexemes", 0),
                         "Derivation_Lexemes": values.get("Derivation_Lexemes", 0)}]
            for target_id, values in links[concept["ID"]].items():
                link_list += [
                        {"ID": target_id, "NAME": concept_names[target_id],
                         "Polysemy": values.get("Polysemy", 0),
                         "Derivation": values.get("Derivation", 0),
                         "Polysemy_Lexemes": values.get("Polysemy_Lexemes", 0),
                         "Derivation_Lexemes": values.get("Derivation_Lexemes", 0)}]

                
            concept["Target_Concepts"] = target_list
            concept["Linked_Concepts"] = link_list 
            args.writer.add_concept(**concept)
        for (c, l, w), values in pb(language_data.items()):
            #if len(values) > 1:
            #    args.log.info("found duplicate for {0} / {1} / {2}".format(c, l, w))
            shifts = [c["Shift_ID"] for c in values]
            concepts = [c["Gloss"] for c in values]

            args.writer.add_form(
                    Language_ID=l,
                    Parameter_ID=c,
                    Value=w,
                    Form=w,
                    Concepts_in_Source=concepts,
                    Shifts=shifts,
                    Source="DatSemShift"
                    )
                    



