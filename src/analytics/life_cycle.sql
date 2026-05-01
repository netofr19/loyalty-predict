WITH tb_daily AS (
    SELECT DISTINCT
        IdCliente,
        substr(DtCriacao, 0, 11) as dtDia
    FROM transacoes
    WHERE dtDia < '{date}'
),
tb_idade AS (
    SELECT
        IdCliente,
        -- min(dtDia) as dtPrimeiraTransacao,
        CAST(julianday('{date}') - julianday(min(dtDia)) as int) as qtdDiasPrimeiraTransacao,

        -- max(dtDia) as dtUltimaTransacao,
        CAST(julianday('{date}') - julianday(max(dtDia)) as int) as qtdDiasUltimaTransacao


    FROM
        tb_daily
    GROUP BY
        IdCliente
),

tb_rn AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY IdCliente ORDER BY dtDia DESC) AS rnDia
    FROM
        tb_daily
),

tb_penultima_ativacao AS (
    SELECT
        *,
        CAST(julianday('{date}') - julianday(dtDia) AS int) as qtdDiasPenultimaTransacao
    FROM
        tb_rn
    WHERE
        rnDia = 2
),

tb_life_cycle AS (
    SELECT
        t1.*,
        t2.qtdDiasPenultimaTransacao,
        CASE
            WHEN qtdDiasPrimeiraTransacao <= 7 THEN '01-CURIOSO'
            WHEN qtdDiasUltimaTransacao <= 7 AND qtdDiasPenultimaTransacao - qtdDiasUltimaTransacao <= 14 THEN '02-FIEL'
            WHEN qtdDiasUltimaTransacao BETWEEN 8 AND 14 THEN '03-TURISTA'
            WHEN qtdDiasUltimaTransacao BETWEEN 15 AND 27 THEN '04-DESENCANTADA'
            WHEN qtdDiasUltimaTransacao >= 28 THEN '05-ZUMBI'
            WHEN qtdDiasUltimaTransacao <= 7 AND qtdDiasPenultimaTransacao - qtdDiasUltimaTransacao BETWEEN 15 AND 27 THEN '06-RECONQUISTADO'
            WHEN qtdDiasUltimaTransacao <= 7 AND qtdDiasPenultimaTransacao - qtdDiasUltimaTransacao > 28 THEN '07-REBORN'
        END AS descLifeCycle
    FROM
        tb_idade as t1
    LEFT JOIN
        tb_penultima_ativacao as t2
    ON
        t1.idCliente = t2.idCliente
)

SELECT
    date('{date}', '-1 day') AS dtRef,
    *
FROM
    tb_life_cycle
