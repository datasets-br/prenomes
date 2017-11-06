/**
 * LIB PARA TRATAMENTO DADOS IBGE.
 */

-- general
CREATE FUNCTION ROUND(float, text, int DEFAULT 0)
RETURNS FLOAT AS $$
   SELECT CASE WHEN $2='dec'
               THEN ROUND($1::numeric,$3)::float
               -- ... WHEN $2='hex' THEN ... WHEN $2='bin' THEN... complete!
               ELSE 'NaN'::float  -- like an error message
           END;
$$ language SQL IMMUTABLE;


/**
 * Percent avoiding divisions by zero.
 */
CREATE or replace FUNCTION lib.div_percent(
  float, float, -- a/b
  int DEFAULT NULL, -- 0-N decimal places or NULL for full
  boolean DEFAULT true -- returns zero when NULL inputs, else returns NULL
) RETURNS float AS $f$
   SELECT CASE
      WHEN $1 IS NULL OR $2 IS NULL THEN (CASE WHEN $4 THEN 0.0 ELSE NULL END)
      WHEN $1=0.0 THEN 0.0
      WHEN $2=0.0 THEN 'Infinity'::float
      ELSE CASE
        WHEN $3 IS NOT NULL AND $3>=0 THEN ROUND(100.0*$1/$2,$3)::float
        ELSE 100.0*$1/$2
      END
   END
$f$ language SQL IMMUTABLE;
CREATE or replace FUNCTION lib.div_percent(
  bigint, bigint, int DEFAULT NULL
) RETURNS float AS $wrap$
   SELECT lib.div_percent($1::float, $2::float, $3)
$wrap$ language SQL IMMUTABLE;
CREATE or replace FUNCTION lib.div_percent_int(bigint,bigint) RETURNS bigint AS $wrap$
   SELECT lib.div_percent($1::float, $2::float, 0)::bigint
$wrap$ language SQL IMMUTABLE;


--- specific


 /**
  * Sum of a slice of columns of the table dataset.big, avoiding nulls.
  */
 CREATE or replace FUNCTION dataset.bigsum_colslice(
   p_j JSONb,   -- from dataset.big.c
   p_ini int DEFAULT 0,  -- first column of the slice, starting with 0
   p_fim int DEFAULT NULL  -- last column of the slice, NULL for all cols
 ) RETURNS bigint  AS $f$
 DECLARE
      i int;
      tsum bigint :=0;
 BEGIN
   IF p_fim IS NULL OR p_fim<0 THEN p_fim:=jsonb_array_length($1); END IF;
   FOR i IN p_ini..p_fim LOOP
      tsum := tsum + COALESCE( ($1->>i)::integer, 0 );
   END LOOP;
   RETURN tsum;
 END;
$f$ LANGUAGE plpgsql IMMUTABLE;

--

/**
 * Stemming of a portuguese name, by to_tsvector() internal function.
 */
CREATE or replace FUNCTION lib.stem_pt(text) RETURNS text AS $$
  SELECT upper(translate(to_tsvector('portuguese',$1)::text,E':1\'',''))
$$ language SQL IMMUTABLE;



/**
 * Melhor performance (1/3 do tempo exec.) em dataset.bigsum_colslice(c,1)
 * para otimizar o TotalGeral das colunas IBGE.
 */
CREATE or replace FUNCTION dataset.bigsum_cols1to9(p_j JSONb) RETURNS int  AS $f$
  SELECT COALESCE(($1->>1)::int,0) + COALESCE(($1->>2)::int,0) + COALESCE(($1->>3)::int,0)
    + COALESCE(($1->>4)::int,0) + COALESCE(($1->>5)::int,0) + COALESCE(($1->>6)::int,0)
    + COALESCE(($1->>7)::int,0) + COALESCE(($1->>8)::int,0) + COALESCE(($1->>9)::int,0)
$f$ LANGUAGE SQL IMMUTABLE;

/**
 * Acrescenta colunas de TotalGeral e Metaphone aos dados IBGE. Evita nulls.
 * NOTA: para melhor performance usar MATERIALIZED VIEW...
 *  ...Aí vale também indexar pelo nome e incluir percentual do total geral.
 */
DROP VIEW IF EXISTS dataset.vw_ibge_censolast_nomes2 CASCADE;
CREATE VIEW dataset.vw_ibge_censolast_nomes2 AS
  SELECT big.c ->> 0 AS nome,
    COALESCE( (big.c ->> 1)::integer, 0 ) AS ate1930,
    COALESCE( (big.c ->> 2)::integer, 0 ) AS ate1940,
    COALESCE( (big.c ->> 3)::integer, 0 ) AS ate1950,
    COALESCE( (big.c ->> 4)::integer, 0 ) AS ate1960,
    COALESCE( (big.c ->> 5)::integer, 0 ) AS ate1970,
    COALESCE( (big.c ->> 6)::integer, 0 ) AS ate1980,
    COALESCE( (big.c ->> 7)::integer, 0 ) AS ate1990,
    COALESCE( (big.c ->> 8)::integer, 0 ) AS ate2000,
    COALESCE( (big.c ->> 9)::integer, 0 ) AS ate2010,
    dataset.bigsum_cols1to9(c) AS TotalGeral, -- ou bigsum_colslice(c,1)
    metaphone(big.c->>0,10) AS metaphone
  FROM dataset.big
  WHERE big.source = dataset.idconfig('ibge_censolast_nomes')
;


DROP MATERIALIZED VIEW IF EXISTS dataset.mtv_ibge_censolast_nomes CASCADE;
CREATE MATERIALIZED VIEW dataset.mtv_ibge_censolast_nomes AS
  SELECT t1.nome, t1.metaphone, t1.stem, t1.ate2010, t1.totalgeral,
    ate2010 - ate2000 as dif2000,
    lib.div_percent(ate2010 - ate2000,ate2010 + ate2000,2) as difperc,
    lib.div_percent(t1.ate2010, t3.tot2010, 6) as perctot2010,
    lib.div_percent(t1.totalgeral,t2.tot, 6) as perctot
  FROM (
    SELECT b.c ->> 0 AS nome,
      metaphone(b.c->>0,10) AS metaphone,
      lib.stem_pt(b.c->>0) AS stem,
      --COALESCE( (b.c ->> 1)::integer, 0 ) AS ate1930,
      --COALESCE( (b.c ->> 2)::integer, 0 ) AS ate1940,
      --COALESCE( (b.c ->> 3)::integer, 0 ) AS ate1950,
      --COALESCE( (b.c ->> 4)::integer, 0 ) AS ate1960,
      --COALESCE( (b.c ->> 5)::integer, 0 ) AS ate1970,
      --COALESCE( (b.c ->> 6)::integer, 0 ) AS ate1980,
      --COALESCE( (b.c ->> 7)::integer, 0 ) AS ate1990,
      -- COALESCE( (b.c ->> 8)::integer, 0 ) AS ate2000,
      COALESCE( (b.c ->> 8)::integer, 0 ) AS ate2000,
      COALESCE( (b.c ->> 9)::integer, 0 ) AS ate2010,
      dataset.bigsum_colslice(b.c,1) AS TotalGeral
    FROM dataset.big b
    WHERE b.source = dataset.idconfig('ibge_censolast_nomes')
  ) t1, (
    SELECT sum(dataset.bigsum_colslice(c,1)::bigint)::float AS tot
    FROM dataset.big
    WHERE source = dataset.idconfig('ibge_censolast_nomes')
  ) t2, (
    SELECT sum((c->> 8)::bigint)::float AS tot2010
    FROM dataset.big
    WHERE source = dataset.idconfig('ibge_censolast_nomes')
  ) t3
;

PEDRO = Q15897419
      = P31 Q12308941 (instance of male given name)
      = P460 (Peter, Petter, Pero, Pietro, Petro, Pierre)




-- -- -- -- -- -- --
/* USO DO METAPHONE NA FORMACAO DO GR-HOMOFONOS

-- Sugere homofonos dos nomes mais populares (correto metaphone PT-BR)
SELECT metaphone, nome, totalGeral
FROM dataset.vw_ibge_censolast_nomes2
WHERE metaphone IN (
    SELECT DISTINCT metaphone
    FROM dataset.vw_ibge_censolast_nomes2 where ate2010>100000
    )
ORDER BY 1,2
;

-- BENCHMARK de custo EXPLAIN ou real por
EXPLAIN ANALYSE SELECT sum(dataset.bigsum_cols1to9(c)) AS tot1
FROM dataset.big
WHERE big.source = dataset.idconfig('ibge_censolast_nomes')
; -- Planning time: 7.822 ms  Execution time: 227.310 ms


EXPLAIN ANALYSE SELECT sum(dataset.bigsum_colslice(c,1)) AS tot2
FROM dataset.big
WHERE big.source = dataset.idconfig('ibge_censolast_nomes')
; -- Planning time: 1.033 ms Execution time: 771.702 ms


----
# nomes femininos
SELECT ?wdId ?itemLabel
WHERE {
  ?item wdt:P31  wd:Q11879590.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "pt,es,it,en,[AUTO_LANGUAGE]". }
  BIND(  REPLACE(STR(?item),"http://www.wikidata.org/entity/","") AS ?wdId) .
}

# nomes masculinos
SELECT ?wdId ?itemLabel
WHERE {
  ?item wdt:P31  wd:Q12308941.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "pt,es,it,en,[AUTO_LANGUAGE]". }
  BIND(  REPLACE(STR(?item),"http://www.wikidata.org/entity/","") AS ?wdId) .
}


---
# Query Wikidata para obter nomes equivalentes a PEDRO (Q15897419)
SELECT  ?itemLabel
WHERE {
  ?item wdt:P460  wd:Q15897419.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],pt". }
}

então limpa com verificacao na base de nomes reais:

SELECT * FROM dataset.mtv_ibge_censolast_nomes WHERE nome IN (SELECT upper(x) from unnest(array['Pieter', 'Piet', 'Peeter', 'Pierre', 'Peter', 'Piero', 'Petko', 'Peder', 'Petri', 'Pjeter', 'Petar', 'Pero', 'Per', 'Peer', 'Petter', 'Piotr', 'Pjetur', 'Pétur', 'Pietro', 'Petrus', 'Petteri', 'Pietari', 'Peetu', 'Petr', 'Petro', 'Peru', 'Petru', 'Pere', 'Pehr', 'Pèdar', 'Petelo', 'Pēteris', 'Péter', 'Petros', 'Kepa', 'Pjetër']) t(x))
order by totalgeral desc;

*/
