--------------------------------------------------
-- PARTIE 3 : AIRLINES
--------------------------------------------------

-- Charger flights.csv (test.csv)
flights_raw =
    LOAD '/airline/test.csv'
    USING PigStorage(',')
    AS (
        year:int, month:int, day:int, dow:int,
        dep_time:int, sched_dep:int,
        arr_time:int, sched_arr:int,
        carrier:chararray, flight:int, tail:chararray,
        air_time:int, actual_dep:int, actual_arr:int,
        arr_delay:int, dep_delay:int,
        origin:chararray, dest:chararray, distance:int
    );

-- Nettoyage : garder seulement lignes valides
flights = FILTER flights_raw BY arr_delay IS NOT NULL;

--------------------------------------------------
-- 3.1 Retard moyen par compagnie
--------------------------------------------------

grp_carrier = GROUP flights BY carrier;

avg_delay_carrier = FOREACH grp_carrier GENERATE
    group AS carrier,
    AVG(flights.arr_delay) AS avg_arr_delay;

STORE avg_delay_carrier INTO '/output/part3/avg_delay_carrier'
    USING PigStorage(',');

--------------------------------------------------
-- 3.2 Retard moyen par aéroport d’origine
--------------------------------------------------

grp_origin = GROUP flights BY origin;

result_origin = FOREACH grp_origin GENERATE
    group AS origin,
    AVG(flights.arr_delay) AS avg_arr_delay;

STORE result_origin INTO '/output/part3/avg_delay_origin'
    USING PigStorage(',');

--------------------------------------------------
-- 3.3 Retard moyen par aéroport de destination
--------------------------------------------------

grp_dest = GROUP flights BY dest;

result_dest = FOREACH grp_dest GENERATE
    group AS dest,
    AVG(flights.arr_delay) AS avg_arr_delay;

STORE result_dest INTO '/output/part3/avg_delay_dest'
    USING PigStorage(',');

--------------------------------------------------
-- 3.4 Nombre de vols par compagnie
--------------------------------------------------

grp_count = GROUP flights BY carrier;

result_count_carrier = FOREACH grp_count GENERATE
    group AS carrier,
    COUNT(flights) AS nb_vols;

STORE result_count_carrier INTO '/output/part3/count_flights'
    USING PigStorage(',');

--------------------------------------------------
-- 3.5 Retard moyen par (compagnie + origine)
--------------------------------------------------

grp_carrier_origin = GROUP flights BY (carrier, origin);

avg_carrier_origin = FOREACH grp_carrier_origin GENERATE
    group.carrier AS carrier,
    group.origin AS origin,
    AVG(flights.arr_delay) AS avg_arr_delay;

STORE avg_carrier_origin INTO '/output/part3/avg_delay_carrier_origin'
    USING PigStorage(',');
