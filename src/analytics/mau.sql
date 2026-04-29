WITH tb_daily AS (
    SELECT DISTINCT
            substr(DtCriacao, 0, 11) as DtDia, 
            IdCliente
    FROM transacoes
    ORDER BY DtDia
),

tb_distinct_day as (
    SELECT DISTINCT 
        DtDia AS DtRef
    FROM 
        tb_daily
)

SELECT 
    t1.DtRef,
    count(DISTINCT IdCliente) as MAU
FROM 
    tb_distinct_day as t1
LEFT JOIN
    tb_daily as t2
ON
    t2.DtDia <= t1.DtRef
AND julianday(t1.DtRef) - julianday(t2.DtDia) < 28
GROUP BY  t1.dtRef
ORDER BY t1.DtRef ASC

