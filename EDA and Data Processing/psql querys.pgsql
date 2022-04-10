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


SELECT PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY arr_delay), EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights GROUP BY EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD'));


SELECT arr_delay, EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) = 1;

SELECT arr_delay, EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) = 2;

SELECT arr_delay, EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) = 3;

SELECT arr_delay, EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) = 4;

SELECT arr_delay, EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) = 5;

SELECT arr_delay, EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) = 6;

SELECT arr_delay, EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) = 7;

SELECT arr_delay, EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) = 8;

SELECT arr_delay, EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) = 9;

SELECT arr_delay, EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) = 10;

SELECT arr_delay, EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) = 11;

SELECT arr_delay, EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) = 12;

SELECT * FROM flights WHERE random() <= 0.008

SELECT origin_airport_id, count(*), AVG(arr_delay) FROM flights group by origin_airport_id order by count(*) desc


SELECT * FROM (SELECT * FROM flights WHERE EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) != 12
and EXTRACT(year FROM TO_DATE(fl_date, 'YYYY-MM-DD')) != 2019) as t 
where random() <= 0.016

select * from flights limit 5

SELECT * from passengers TABLESAMPLE bernoulli(8) WHERE (month != 12
and year != 2019)

SELECT reltuples AS estimate FROM pg_class WHERE relname = 'passengers';

show table status