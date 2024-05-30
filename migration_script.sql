DROP VIEW IF EXISTS shipping_datamart ;
DROP TABLE IF EXISTS shipping_info;
DROP TABLE IF EXISTS shipping_country_rates;
DROP TABLE IF EXISTS shipping_agreement;
DROP TABLE IF EXISTS shipping_transfer;
DROP TABLE IF EXISTS shipping_status;

--
-- CREATE AND FULLFILL TABLE shipping_country_rates
--

CREATE TABLE shipping_country_rates(
	id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	shipping_country TEXT,
	shipping_country_base_rate NUMERIC(14,3)
);

INSERT INTO shipping_country_rates(shipping_country, shipping_country_base_rate)
SELECT shipping_country, shipping_country_base_rate
FROM public.shipping
GROUP BY shipping_country, shipping_country_base_rate;

--
-- CREATE AND FULLFILL TABLE shipping_agreement
--

CREATE TABLE shipping_agreement(
	agreementid BIGINT PRIMARY KEY,
	agreement_number TEXT, 
	agreement_rate NUMERIC(3, 2),
	agreement_commission NUMERIC(3, 2)
);

INSERT INTO shipping_agreement
SELECT DISTINCT
		CAST(vendor_agreement_description[1] AS BIGINT) AS agreementid,
		vendor_agreement_description[2] AS agreement_number,
		CAST(vendor_agreement_description[3] AS NUMERIC(3, 2)) AS agreement_rate,
		CAST(vendor_agreement_description[4] AS NUMERIC(3, 2)) AS agreement_commission
FROM (
	SELECT regexp_split_to_array(vendor_agreement_description, ':') AS vendor_agreement_description
	FROM public.shipping) AS vendor_info
ORDER BY agreementid;

--
-- CREATE AND FULLFILL TABLE shipping_transfer
--

CREATE TABLE shipping_transfer(
	id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	transfer_type VARCHAR(2),
	transfer_model VARCHAR(20),
	shipping_transfer_rate NUMERIC(14, 2)
);

INSERT INTO shipping_transfer(transfer_type, transfer_model, shipping_transfer_rate)
SELECT  shipping_transfer_description[1] AS transfer_type, 
		shipping_transfer_description[2] AS transfer_model, 
		shipping_transfer_rate
FROM (
	SELECT DISTINCT 
		regexp_split_to_array(shipping_transfer_description, ':') AS shipping_transfer_description, 
		shipping_transfer_rate
	FROM public.shipping) AS transfer_info
ORDER BY transfer_type, transfer_model;

--
-- CREATE AND FULLFILL TABLE shipping_info
--

CREATE TABLE shipping_info(
	shippingid INT8 PRIMARY KEY,
	shipping_plan_datetime TIMESTAMP, 
	payment_amount NUMERIC(14, 2), 
	vendorid INT8,
	country_rate_id INT,
	agreement_id BIGINT,
	transfer_id BIGINT,
	CONSTRAINT shipping_info_country_rate_id_fkey FOREIGN KEY (country_rate_id) REFERENCES shipping_country_rates(id),
	CONSTRAINT shipping_info_agreementid_fkey FOREIGN KEY (agreement_id) REFERENCES shipping_agreement(agreementid),
	CONSTRAINT shipping_info_transfer_id_fkey FOREIGN KEY (transfer_id) REFERENCES shipping_transfer(id)
);

INSERT INTO shipping_info
SELECT  sit.shippingid, sit.shipping_plan_datetime, sit.payment_amount, sit.vendorid, scr.id AS country_rate_id, sa.agreementid, st.id AS transfer_id
FROM (
	SELECT  shippingid, shipping_plan_datetime, payment_amount, vendorid, shipping_country, 
			vendor_agreement_description[1]::INT AS agreement_id, 
			shipping_transfer_description[1] AS transfer_type, 
			shipping_transfer_description[2] AS transfer_model
	FROM (
		SELECT DISTINCT ON (shippingid) 
			shippingid, 
			shipping_plan_datetime, 
			payment_amount, 
			vendorid, 
			shipping_country, 
			regexp_split_to_array(vendor_agreement_description, ':') AS vendor_agreement_description, 
			regexp_split_to_array(shipping_transfer_description, ':') AS shipping_transfer_description
		FROM public.shipping s) AS shipping_info_temp) AS sit
JOIN shipping_country_rates scr USING(shipping_country)
JOIN shipping_agreement sa ON sit.agreement_id = sa.agreementid
JOIN shipping_transfer st USING(transfer_type, transfer_model)
ORDER BY sit.shippingid;

--
-- CREATE AND FULLFILL TABLE shipping_status
--

CREATE TABLE shipping_status (
	shippingid INT8 PRIMARY KEY, 
	status TEXT, 
	state TEXT, 
	shipping_start_fact_datetime TIMESTAMP, 
	shipping_end_fact_datetime TIMESTAMP
);

INSERT INTO shipping_status
WITH detail_shipping_info AS (
	SELECT  s.shippingid, s.status, s.state, s.state_datetime, bs.state_datetime AS shipping_start_fact_datetime,
			br.state_datetime AS shipping_end_fact_datetime,
			ROW_NUMBER() OVER(PARTITION BY s.shippingid ORDER BY s.state_datetime DESC) AS id_history
	FROM public.shipping s
	LEFT JOIN (
		SELECT shippingid, state, state_datetime
		FROM public.shipping
		WHERE state = 'booked') AS bs USING(shippingid)
	LEFT JOIN (
		SELECT shippingid, state, state_datetime
		FROM public.shipping
		WHERE state = 'recieved') AS br USING(shippingid)
)
SELECT shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime
FROM detail_shipping_info
WHERE id_history = 1;

--
-- CREATE VIEW shipping_datamart 
--

CREATE OR REPLACE VIEW shipping_datamart AS
SELECT  si.shippingid, si.vendorid, st.transfer_type, EXTRACT('day' FROM shipping_end_fact_datetime - shipping_start_fact_datetime) AS full_day_at_shipping,
		(CASE WHEN shipping_end_fact_datetime IS NULL THEN NULL
		  	  WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN 1
		 	  ELSE 0
		 END) AS is_delay,
		(CASE WHEN status = 'finished' THEN 1		  
		 	   ELSE 0
		 END) AS is_shipping_finish,
		(CASE WHEN shipping_end_fact_datetime IS NULL THEN NULL
		  	  WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN EXTRACT('day' FROM shipping_end_fact_datetime - shipping_plan_datetime)
		 	  ELSE 0
		 END) AS delay_day_at_shipping,
		payment_amount, 
		payment_amount * (shipping_country_base_rate + agreement_rate + shipping_transfer_rate) AS vat,
		payment_amount * agreement_commission AS profit 
FROM shipping_info si 
JOIN shipping_transfer st ON si.transfer_id = st.id
LEFT JOIN shipping_status ss USING(shippingid)
LEFT JOIN shipping_country_rates scr ON scr.id = si.country_rate_id
LEFT JOIN shipping_agreement sa ON sa.agreementid = si.agreement_id;