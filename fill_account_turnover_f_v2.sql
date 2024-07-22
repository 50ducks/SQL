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
		, account."ACCOUNT_RK"
		--CREDIT
		, COALESCE((
			SELECT SUM(credit."CREDIT_AMOUNT")
			FROM ds.ft_posting_f credit
			WHERE credit."CREDIT_ACCOUNT_RK" = account."ACCOUNT_RK"
				AND credit."OPER_DATE" = i_OnDate), 0)
		AS "CREDIT_AMOUNT"
		
		, COALESCE((SELECT SUM(credit."CREDIT_AMOUNT")
			FROM ds.ft_posting_f credit
			WHERE credit."CREDIT_ACCOUNT_RK" = account."ACCOUNT_RK"
				AND credit."OPER_DATE" = i_OnDate), 0)
		* COALESCE(exchange."REDUCED_COURCE", 1)
		AS "CREDIT_AMOUNT_RUB"
		--DEBET
		, COALESCE((
			SELECT SUM(debet."DEBET_AMOUNT")
			FROM ds.ft_posting_f debet
			WHERE debet."DEBET_ACCOUNT_RK" = account."ACCOUNT_RK"
				AND debet."OPER_DATE" = i_OnDate ), 0)
		AS "CREDIT_AMOUNT"
		
		, COALESCE((
			SELECT SUM(debet."DEBET_AMOUNT")
			FROM ds.ft_posting_f debet
			WHERE debet."DEBET_ACCOUNT_RK" = account."ACCOUNT_RK"
				AND debet."OPER_DATE" = i_OnDate ), 0) 
		* COALESCE(exchange."REDUCED_COURCE", 1) 
		AS "DEBET_AMOUNT_RUB"
	
	FROM ds.ft_posting_f AS posting
	LEFT JOIN ds.md_account_d account
		ON account."ACCOUNT_RK" = posting."CREDIT_ACCOUNT_RK" OR account."ACCOUNT_RK" = posting."DEBET_ACCOUNT_RK"
	LEFT JOIN ds.md_exchange_rate_d exchange
		ON exchange."CURRENCY_RK" = account."CURRENCY_RK"
		AND i_OnDate BETWEEN exchange."DATA_ACTUAL_DATE" AND exchange."DATA_ACTUAL_END_DATE"
	WHERE posting."OPER_DATE" = i_OnDate
	GROUP BY account."ACCOUNT_RK", exchange."REDUCED_COURCE";

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
