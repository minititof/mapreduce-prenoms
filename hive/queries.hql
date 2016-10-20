CREATE TEMPORARY EXTERNAL TABLE prenoms_tmp (
prenom STRING,
gender ARRAY<VARCHAR(3)>,
origin ARRAY<VARCHAR(30)>,
version DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\073' 
COLLECTION ITEMS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/res';

CREATE TABLE prenoms AS SELECT * from prenoms_tmp;

SELECT exploded_origin, COUNT(DISTINCT prenom)
FROM (SELECT exploded_origin, prenom FROM prenoms LATERAL VIEW EXPLODE(origin) tb AS exploded_origin) sub
GROUP BY exploded_origin;

SELECT SIZE(origin), COUNT(prenom)
FROM prenoms
GROUP BY SIZE(origin);
