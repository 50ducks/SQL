TRUNCATE TABLE dm.dm_account_balance_f;

INSERT INTO dm.dm_account_balance_f (
	"ON_DATE"
	, "ACCOUNT_RK"
	, "BALANCE_OUT"
	, "BALANCE_OUT_RUB"
)
SELECT '2017-12-31' AS "ON_DATE"
	, balance."ACCOUNT_RK" 
	, balance."BALANCE_OUT"
	, balance."BALANCE_OUT" * COALESCE(exchange."REDUCED_COURCE", 1) AS "BALANCE_OUT_RUB"
FROM ds.ft_balance_f AS balance
LEFT JOIN ds.md_account_d AS account
	ON account."ACCOUNT_RK" = balance."ACCOUNT_RK"
LEFT JOIN ds.md_exchange_rate_d AS exchange
	ON exchange."CURRENCY_RK" = account."CURRENCY_RK"
	AND '2017-12-31' BETWEEN exchange."DATA_ACTUAL_DATE" AND exchange."DATA_ACTUAL_END_DATE";

SELECT * FROM dm.dm_account_balance_f;