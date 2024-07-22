CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
	procedure_name TEXT := 'fill_account_balance_f';
	start_time TIMESTAMP;
	end_time TIMESTAMP;
BEGIN
	start_time := clock_timestamp();
	
	DELETE FROM dm.dm_account_balance_f
	WHERE "ON_DATE" = i_OnDate;

	CREATE TEMP TABLE IF NOT EXISTS dm_previous_account_balance_f AS
	SELECT
		"ON_DATE"
		, "ACCOUNT_RK"
		, "BALANCE_OUT"
		, "BALANCE_OUT_RUB"
	FROM dm.dm_account_balance_f AS balance
	WHERE balance."ON_DATE" = i_OnDate - INTERVAL '1 day';
	
	INSERT INTO dm.dm_account_balance_f (
		"ON_DATE"
		, "ACCOUNT_RK"
		, "BALANCE_OUT"
		, "BALANCE_OUT_RUB"
	)
	SELECT i_OnDate
		, account."ACCOUNT_RK"
		, CASE
			WHEN account."CHAR_TYPE" = 'А' THEN COALESCE(previous_balance."BALANCE_OUT", 0) 
				+ COALESCE(turnover."DEBET_AMOUNT", 0) 
				- COALESCE(turnover."CREDIT_AMOUNT", 0)
			WHEN account."CHAR_TYPE" = 'П' THEN COALESCE(previous_balance."BALANCE_OUT", 0) 
				- COALESCE(turnover."DEBET_AMOUNT", 0) 
				+ COALESCE(turnover."CREDIT_AMOUNT", 0)
		END AS "BALANCE_OUT"
		, CASE
			WHEN account."CHAR_TYPE" = 'А' THEN (COALESCE(previous_balance."BALANCE_OUT", 0)
				+ COALESCE(turnover."DEBET_AMOUNT", 0)
				- COALESCE(turnover."CREDIT_AMOUNT", 0))
				*  COALESCE(exchange."REDUCED_COURCE", 1)
			WHEN account."CHAR_TYPE" = 'П' THEN (COALESCE(previous_balance."BALANCE_OUT", 0)
				- COALESCE(turnover."DEBET_AMOUNT", 0)
				+ COALESCE(turnover."CREDIT_AMOUNT", 0))
				*  COALESCE(exchange."REDUCED_COURCE", 1)
		END AS "BALANCE_OUT_RUB"
	FROM ds.md_account_d AS account
	LEFT JOIN dm_previous_account_balance_f AS previous_balance
		ON previous_balance."ON_DATE" = i_OnDate - INTERVAL '1 day'
		and previous_balance."ACCOUNT_RK" = account."ACCOUNT_RK"
	LEFT JOIN dm.dm_account_turnover_f AS turnover
		ON turnover."ACCOUNT_RK" = account."ACCOUNT_RK"
		AND turnover."ON_DATE" = i_OnDate
	LEFT JOIN ds.md_exchange_rate_d AS exchange
		ON exchange."CURRENCY_RK" = account."CURRENCY_RK"
		AND i_OnDate BETWEEN exchange."DATA_ACTUAL_DATE" AND exchange."DATA_ACTUAL_END_DATE"
	WHERE i_OnDate BETWEEN account."DATA_ACTUAL_DATE" AND account."DATA_ACTUAL_END_DATE";
	DROP TABLE IF EXISTS dm_previous_account_balance_f;
	
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