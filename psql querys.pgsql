SELECT median(arr_delay) AS median, extract(month from to_date(fl_date, 'YYYY-MM-DD')) as months FROM flights group by extract(month from to_date(fl_date, 'YYYY-MM-DD'));

SELECT PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY arr_delay), EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD')) 
FROM flights GROUP BY EXTRACT(MONTH FROM TO_DATE(fl_date, 'YYYY-MM-DD'))

select * from flights limit 100;

SELECT count(*) AS ct              -- optional
     , min(op_carrier_fl_num)  AS min_id
     , max(op_carrier_fl_num)  AS max_id
     , max(op_carrier_fl_num) - min(op_carrier_fl_num) AS id_span
FROM   flights;