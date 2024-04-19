sqlite3 dss.sqlite <<EOF
.headers on
.mode csv 

SELECT 
  table_a.LID as LID,
  table_a.Language, 
  table_b.LanguageB, 
  table_a.Concept as ConceptA, 
  table_a.Form as FormA,
  table_b.ConceptB as ConceptB, 
  table_b.FormB as FormB,
  table_a.Shifts,
  table_b.ShiftsB
-- query German words in the first table
FROM
  (
    SELECT 
      f.local_id as LID,
      l.cldf_name as Language,
      l.cldf_glottocode as Glottocode, 
      l.family as Family, 
      p.cldf_name as Concept, 
      f.cldf_form as Form,
      f.source_lexeme as Source,
      f.shifts as Shifts
    FROM 
      formtable as f, 
      languagetable as l, 
      parametertable as p
    WHERE
      p.cldf_id = f.cldf_parameterReference
        AND
      l.cldf_id = f.cldf_languageReference
        AND
      l.cldf_glottocode = 'stan1295'
) as table_a
-- query the words in the second table to join them
INNER JOIN 
  (
    SELECT 
      f2.source_lexeme as IDB,
      l2.cldf_name as LanguageB,
      p2.cldf_name as ConceptB,
      f2.cldf_form as FormB,
      f2.shifts as ShiftsB
    FROM
      formtable as f2,
      parametertable as p2,
      languagetable as l2
    WHERE
      f2.cldf_languageReference = l2.cldf_id
        AND
      f2.cldf_parameterReference = p2.cldf_id
        AND
      l2.cldf_glottocode = 'stan1295'
  ) as table_b
-- conditions for the output, limit to the same language
-- and also to diverging concepts
ON
  table_a.LID == table_b.IDB
-- order to retrieve data for each language in a block
ORDER BY 
  Form,
  Language, 
  Concept
;

EOF
