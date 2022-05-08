select * from flights limit 100;

SELECT AVG(arr_delay), EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) FROM flights
GROUP BY EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD'));

SELECT * FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) != 12
and EXTRACT(year FROM TO_DATE(fl_date, 'YYYY-MM-DD')) != 2019;

SELECT AVG(arr_delay), 
EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) as month, 
EXTRACT(year FROM TO_DATE(fl_date, 'YYYY-MM-DD')) as year FROM flights
GROUP BY EXTRACT(year FROM TO_DATE(fl_date, 'YYYY-MM-DD')), EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD'))
ORDER BY EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD'));

SELECT * FROM flights WHERE random() <= 0.008

SELECT origin_airport_id, count(*), AVG(arr_delay) FROM flights group by origin_airport_id order by count(*) desc


SELECT fl_date, op_unique_carrier, tail_num, origin_airport_id, dest_airport_id, crs_dep_time, dep_time, dep_delay, taxi_out, taxi_in, crs_arr_time, arr_time, arr_delay, crs_elapsed_time, actual_elapsed_time  
FROM flights
where random() <= 0.03

select * from flights limit 5

SELECT fl_date, op_unique_carrier, tail_num, origin_airport_id, dest_airport_id, crs_dep_time, dep_time, dep_delay, taxi_out, taxi_in, crs_arr_time, arr_time, arr_delay, crs_elapsed_time, actual_elapsed_time 
from flights TABLESAMPLE bernoulli(4) where tail_num IN ()

SELECT reltuples AS estimate FROM pg_class WHERE relname = 'passengers';

show table status

select EXTRACT(year FROM fl_date) as year, EXTRACT(month FROM fl_date) as month, EXTRACT(day FROM fl_date) as day, op_unique_carrier, tail_num, origin_airport_id, dest_airport_id, crs_dep_time, crs_arr_time, crs_elapsed_time from flights_test 
where EXTRACT(MONTH FROM fl_date) = 1 and EXTRACT(day FROM fl_date) < 8



select * from flights limit 5

select fl_date, data_type from information_schema.columns
 where table_name = 'flights_test';

 SELECT *, EXTRACT(month FROM fl_date) as month, EXTRACT(doy FROM fl_date) as day FROM flights_test;

CREATE TABLE t AS (SELECT tail_num, op_unique_carrier, origin_airport_id, dest_airport_id FROM flights_test where EXTRACT(day FROM fl_date) < 8)

 SELECT fl_date, op_unique_carrier, tail_num, origin_airport_id, dest_airport_id, crs_dep_time, dep_time, dep_delay, taxi_out, taxi_in, crs_arr_time, arr_time, arr_delay, crs_elapsed_time, actual_elapsed_time 
 FROM flights TABLESAMPLE bernoulli(4)
 WHERE tail_num IN (SELECT DISTINCT(tail_num) FROM flights_test WHERE (EXTRACT(day FROM fl_date)) < 8)
    AND
      origin_airport_id IN (SELECT DISTINCT(origin_airport_id) FROM flights_test WHERE (EXTRACT(day FROM fl_date)) < 8)
    AND 
      op_unique_carrier IN (SELECT DISTINCT(op_unique_carrier) FROM flights_test WHERE (EXTRACT(day FROM fl_date)) < 8)
    AND
      dest_airport_id IN (SELECT DISTINCT(dest_airport_id) FROM flights_test WHERE (EXTRACT(day FROM fl_date)) < 8)