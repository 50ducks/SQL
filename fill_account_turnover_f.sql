CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate date)
LANGUAGE plpgsql
AS $$
DECLARE
	procedure_name TEXT := 'fill_account_turnover_f';
	start_time TIMESTAMP;
	end_time TIMESTAMP;
BEGIN
	start_time := clock_timestamp();
	
	DELETE FROM dm.dm_account_turnover_f 
	WHERE "ON_DATE" = i_OnDate;
	
	WITH turnover AS (
		SELECT i_OnDate AS "ON_DATE"
		, posting."CREDIT_ACCOUNT_RK" AS "ACCOUNT_RK"
		, posting."CREDIT_AMOUNT" AS "CREDIT_AMOUNT"
		, posting."CREDIT_AMOUNT" * COALESCE(exchange."REDUCED_COURCE", 1) AS "CREDIT_AMOUNT_RUB"
		, CAST(0 AS DOUBLE PRECISION) AS "DEBET_AMOUNT"
		, CAST(0 AS DOUBLE PRECISION) AS "DEBET_AMOUNT_RUB"
	FROM ds.ft_posting_f AS posting
    LEFT JOIN ds.md_account_d account
		ON account."ACCOUNT_RK" = posting."CREDIT_ACCOUNT_RK"
	LEFT JOIN ds.md_exchange_rate_d exchange
		ON exchange."CURRENCY_RK" = account."CURRENCY_RK"
		AND i_OnDate BETWEEN exchange."DATA_ACTUAL_DATE" AND exchange."DATA_ACTUAL_END_DATE"
	WHERE "OPER_DATE" = i_OnDate
	
	UNION ALL
		
	SELECT i_OnDate AS "ON_DATE"
		, posting."DEBET_ACCOUNT_RK" AS "ACCOUNT_RK"
		, CAST(0 AS DOUBLE PRECISION) AS "DEBET_AMOUNT"
		, CAST(0 AS DOUBLE PRECISION) AS "DEBET_AMOUNT_RUB"
		, posting."DEBET_AMOUNT" AS "CREDIT_AMOUNT"
		, posting."DEBET_AMOUNT" * COALESCE(exchange."REDUCED_COURCE", 1) AS "CREDIT_AMOUNT_RUB"
	FROM ds.ft_posting_f AS posting
    LEFT JOIN ds.md_account_d account
		ON account."ACCOUNT_RK" = posting."CREDIT_ACCOUNT_RK"
	LEFT JOIN ds.md_exchange_rate_d exchange
		ON exchange."CURRENCY_RK" = account."CURRENCY_RK"
		AND i_OnDate BETWEEN exchange."DATA_ACTUAL_DATE" AND exchange."DATA_ACTUAL_END_DATE"
	WHERE "OPER_DATE" = i_OnDate
	)
		
	INSERT INTO dm.dm_account_turnover_f (
		"ON_DATE"
		, "ACCOUNT_RK"
		, "CREDIT_AMOUNT"
		, "CREDIT_AMOUNT_RUB"
		, "DEBET_AMOUNT"
		, "DEBET_AMOUNT_RUB"
	)
	SELECT i_OnDate AS "ON_DATE"
		, t."ACCOUNT_RK"
		, SUM(t."CREDIT_AMOUNT") AS "CREDIT_AMOUNT"
		, SUM(t."CREDIT_AMOUNT_RUB") AS "CREDIT_AMOUNT_RUB"
		, SUM(t."DEBET_AMOUNT") AS "DEBET_AMOUNT"
		, SUM(t."DEBET_AMOUNT_RUB") AS "DEBET_AMOUNT_RUB"
	FROM turnover AS t
	GROUP BY t."ACCOUNT_RK";

--	PERFORM pg_sleep(1);

	end_time := clock_timestamp();

	INSERT INTO logs.procedure_logs (
		procedure_name
		, start_time
		, end_time
	)
	VALUES ( 
		procedure_name
		, start_time
		, end_time
	);
END;
$$;