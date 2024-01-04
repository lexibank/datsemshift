import pathlib
import attr
from clldutils.misc import slug
from pylexibank import Dataset as BaseDataset
from pylexibank import progressbar as pb
from pylexibank import Language
from pylexibank import FormSpec
from csvw.dsv import UnicodeWriter
import re


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
class CustomLanguage(Language):
    Location = attr.ib(default=None)
    Remark = attr.ib(default=None)


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "datsemshift"
    language_class = CustomLanguage
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
        with open(self.raw_dir / "raw-data" / "languages.html") as f:
            languages = f.read()
        language_table = [['ID', "Name", "Glottocode", "Family", "Words"]]
        all_languages = re.findall("<tr[^>]*>(.*?)</tr>", languages, re.DOTALL)
        for lng in all_languages:
            tabs = re.findall("<td[^>]*>(.*?)</td>", lng)
            if tabs:
                idf, name, glottolog, family, words = tabs[0], tabs[1], tabs[2], tabs[3], tabs[4]
                language_table += [[idf, name, glottolog, family, words]]
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
                        cidx += 1
                        concept_lookup[gloss.strip()] = cidx
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
            shift_id = str(pth).split("/")[-1][:-4]
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
                                "", "", "0"]]
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


