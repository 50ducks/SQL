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

	INSERT INTO dm.dm_account_turnover_f (
		"ON_DATE"
		, "ACCOUNT_RK"
		, "CREDIT_AMOUNT"
		, "CREDIT_AMOUNT_RUB"
		, "DEBET_AMOUNT"
		, "DEBET_AMOUNT_RUB"
	)
	SELECT i_OnDate
		, COALESCE(posting."CREDIT_ACCOUNT_RK", posting."DEBET_ACCOUNT_RK") AS "ACCOUNT_RK"
		, COALESCE(SUM(posting."CREDIT_AMOUNT"), 0) AS "CREDIT_AMOUNT"
		, COALESCE(SUM(posting."CREDIT_AMOUNT" * COALESCE(exchange."REDUCED_COURCE", 1)), 0) AS "CREDIT_AMOUNT_RUB"
		, COALESCE(SUM(posting."DEBET_AMOUNT"), 0) AS "DEBET_AMOUNT"
		, COALESCE(SUM(posting."DEBET_AMOUNT" * COALESCE(exchange."REDUCED_COURCE", 1)), 0) AS "DEBET_AMOUNT_RUB"
	FROM ds.ft_posting_f AS posting
	LEFT JOIN ds.md_account_d account
		ON account."ACCOUNT_RK" = posting."CREDIT_ACCOUNT_RK" OR account."ACCOUNT_RK" = posting."DEBET_ACCOUNT_RK"
	LEFT JOIN ds.md_exchange_rate_d exchange
		ON exchange."CURRENCY_RK" = account."CURRENCY_RK"
		AND i_OnDate BETWEEN exchange."DATA_ACTUAL_DATE" AND exchange."DATA_ACTUAL_END_DATE"
	WHERE "OPER_DATE" = i_OnDate
	GROUP BY COALESCE("CREDIT_ACCOUNT_RK", "DEBET_ACCOUNT_RK");

	PERFORM pg_sleep(1);

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
