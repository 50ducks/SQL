CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
	fromDate DATE;
	toDate DATE;
	procedure_name TEXT := 'fill_f101_round_f';
	start_time TIMESTAMP;
	end_time TIMESTAMP;
BEGIN
	start_time := clock_timestamp();

	fromDate := date_trunc('month', i_OnDate - INTERVAL '1 month' );
	toDate := i_OnDate - INTERVAL '1 day';

	DELETE FROM dm.dm_f101_round_f
	WHERE "FROM_DATE" = fromDate;

	INSERT INTO dm.dm_f101_round_f(
		"FROM_DATE"
		, "TO_DATE"
		, "CHAPTER"
		, "LEDGER_ACCOUNT"
		, "CHARACTERISTIC"
		, "BALANCE_IN_RUB"
		, "BALANCE_IN_VAL"
		, "BALANCE_IN_TOTAL"
		, "TURN_DEB_RUB"
		, "TURN_DEB_VAL"
		, "TURN_DEB_TOTAL"
		, "TURN_CRE_RUB"
		, "TURN_CRE_VAL"
		, "TURN_CRE_TOTAL"
		, "BALANCE_OUT_RUB"
		, "BALANCE_OUT_VAL"
		, "BALANCE_OUT_TOTAL"
	)
	SELECT
		fromDate
		, toDate
		, ledger."CHAPTER"
		, ledger."LEDGER_ACCOUNT"
		, account."CHAR_TYPE"
		
		--BALANCE_IN
		--RUB
		, SUM(CASE WHEN account."CURRENCY_CODE" IN ('810', '643') 
				THEN balance_in."BALANCE_OUT_RUB" 
				ELSE 0 
			END)
		AS "BALANCE_IN_RUB"
		
		--VAL
		, SUM(CASE WHEN account."CURRENCY_CODE" NOT IN ('810', '643')
				THEN balance_in."BALANCE_OUT_RUB" 
				ELSE 0 
			END)
		AS "BALANCE_IN_VAL"
		
		--TOTAL
		, COALESCE(SUM(balance_in."BALANCE_OUT_RUB"), 0)
		AS "BALANCE_IN_TOTAL"
		
		--DEBET
		--RUB
		, SUM(CASE WHEN account."CURRENCY_CODE" IN ('810', '643') 
				THEN turnover."DEBET_AMOUNT_RUB" 
				ELSE 0 
			END)
		 AS "TURN_DEB_RUB"
		
		--VAL
		, SUM(CASE WHEN account."CURRENCY_CODE" NOT IN ('810', '643') 
				THEN turnover."DEBET_AMOUNT_RUB" 
				ELSE 0 
			END)
		 AS "TURN_DEB_VAL"
		
		--TOTAL
		, COALESCE(SUM(turnover."DEBET_AMOUNT_RUB"), 0)
		 AS "TURN_DEB_TOTAL"
		
		--CREDIT
		--RUB
		, SUM(CASE WHEN account."CURRENCY_CODE" IN ('810', '643') 
				THEN turnover."CREDIT_AMOUNT_RUB" 
				ELSE 0 
			END)
		 AS "TURN_CRE_RUB"
		
		--VAL
		, SUM(CASE WHEN account."CURRENCY_CODE" NOT IN ('810', '643')
				THEN turnover."CREDIT_AMOUNT_RUB" 
				ELSE 0 
			END)
		 AS "TURN_CRE_VAL"
		
		--TOTAL
		, COALESCE(SUM(turnover."CREDIT_AMOUNT_RUB"), 0)
		AS "TURN_CRE_TOTAL"

		--BALANCE_OUT
		--RUB
		, SUM(CASE WHEN account."CURRENCY_CODE" IN ('810', '643') 
				THEN balance_out."BALANCE_OUT_RUB" 
				ELSE 0 
			END)
		AS "BALANCE_OUT_RUB"

		--VAL
		, SUM(CASE WHEN account."CURRENCY_CODE" NOT IN ('810', '643')
				THEN balance_out."BALANCE_OUT_RUB" 
				ELSE 0 
			END)
		AS "BALANCE_OUT_VAL"

		--TOTAL
		, COALESCE(SUM(balance_out."BALANCE_OUT_RUB"), 0)
		AS "BALANCE_OUT_TOTAL"
		
	FROM ds.md_account_d AS account
	LEFT JOIN ds.md_ledger_account_s AS ledger
		ON CAST(ledger."LEDGER_ACCOUNT" AS character(5)) = SUBSTRING(account."ACCOUNT_NUMBER" FROM 1 FOR 5)
	LEFT JOIN dm.dm_account_balance_f AS balance_in
		ON balance_in."ACCOUNT_RK" = account."ACCOUNT_RK"
		AND balance_in."ON_DATE" = fromDate - INTERVAL '1 day'
	LEFT JOIN dm.dm_account_turnover_f AS turnover
		ON turnover."ACCOUNT_RK" = account."ACCOUNT_RK"
		AND turnover."ON_DATE" BETWEEN fromDate AND toDate
	LEFT JOIN dm.dm_account_balance_f AS balance_out
		ON balance_out."ACCOUNT_RK" = account."ACCOUNT_RK"
		AND balance_out."ON_DATE" = toDate
	GROUP BY ledger."CHAPTER", ledger."LEDGER_ACCOUNT", account."CHAR_TYPE";
	
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