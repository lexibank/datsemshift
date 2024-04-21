sqlite3 dss.sqlite <<EOF
.headers on
.mode csv
.separator "\t" 

SELECT 
  table_a.Lexeme_ID as ID_A,
  table_a.Language as Language_A, 
  table_a.Concept as Concept_A,
  table_a.Form ||
  ' «' || table_a.Concept_in_Source || '»' as Word_A,
  table_b.Lexeme_ID as ID_B,
  table_b.Language as Language_B, 
  table_b.Concept as Concept_B, 
  table_b.Form ||
  ' «' || table_b.Concept_in_Source || '»' as Word_B,
  table_b.Sources,
  table_b.Source_Relations
-- query German words in the first table
FROM
  (
    SELECT 
      f1.local_id as Lexeme_ID,
      l1.cldf_name as Language,
      p1.concepticon_gloss as Concept,
      p1.cldf_name as Concept_in_Source,
      f1.cldf_form as Form,
      f1.shifts as Shifts
    FROM 
      formtable as f1, 
      languagetable as l1, 
      parametertable as p1
    WHERE
      p1.cldf_id = f1.cldf_parameterReference
      AND l1.cldf_id = f1.cldf_languageReference
      AND l1.cldf_glottocode = 'stan1295'
      AND p1.concepticon_gloss != ''
) as table_a
-- query the words in the second table to join them
JOIN 
  (
    SELECT 
      f2.local_id as Lexeme_ID,
      l2.cldf_name as Language,
      p2.concepticon_gloss as Concept,
      p2.cldf_name as Concept_in_Source,
      f2.cldf_form as Form,
      f2.shifts as Shifts,
      f2.source_relations,
      f2.source_lexemes as Sources
    FROM
      formtable as f2,
      parametertable as p2,
      languagetable as l2
    WHERE
      f2.cldf_languageReference = l2.cldf_id
      AND f2.cldf_parameterReference = p2.cldf_id
      AND l2.cldf_glottocode = 'stan1295'
      AND p2.concepticon_gloss != ''
  ) as table_b
-- conditions for the output, limit to the same language
-- and to concepts related via source_lexemes
ON
  (
    table_b.Sources like '% ' || table_a.Lexeme_ID || ' %' 
    OR table_b.Sources like table_a.Lexeme_ID || ' %' 
    OR table_b.Sources like table_a.Lexeme_ID
    OR table_b.Sources like '% ' || table_a.Lexeme_ID
  ) 
-- order to retrieve data for each language in a block
ORDER BY
  Word_A,
  Language_A, 
  Concept_A
;

EOF
