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
            WHEN qtdDiasUltimaTransacao BETWEEN 15 AND 28 THEN '04-DESENCANTADA'
            WHEN qtdDiasUltimaTransacao > 28 THEN '05-ZUMBI'
            WHEN qtdDiasUltimaTransacao <= 7 AND qtdDiasPenultimaTransacao - qtdDiasUltimaTransacao BETWEEN 15 AND 27 THEN '02-RECONQUISTADO'
            WHEN qtdDiasUltimaTransacao <= 7 AND qtdDiasPenultimaTransacao - qtdDiasUltimaTransacao > 28 THEN '02-REBORN'
        END AS descLifeCycle
    FROM
        tb_idade as t1
    LEFT JOIN
        tb_penultima_ativacao as t2
    ON
        t1.idCliente = t2.idCliente
),

tb_freq_valor AS (
    SELECT
        idCliente,
        count(DISTINCT substr(DtCriacao, 0, 11)) as qtdFrequencia,
        sum(CASE WHEN qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPos,
        sum(abs(qtdePontos)) AS qtdePontosAbs
    FROM
        transacoes
    WHERE
        DtCriacao < '{date}'
        AND
        DtCriacao >= date('{date}', '-28 days')
    GROUP BY
        idCliente
    ORDER BY
        qtdFrequencia DESC
),

tb_cluster AS (
    SELECT
        *,
        CASE
            WHEN qtdFrequencia <= 10 AND qtdePontosPos >= 1500 THEN '12-HYPER'
            WHEN qtdFrequencia > 10 AND qtdePontosPos >=1500 THEN '22-EFICIENTES'
            WHEN qtdFrequencia <= 10 AND qtdePontosPos >= 750 THEN '11-INDECISO'
            WHEN qtdFrequencia > 10 AND qtdePontosPos >= 750 THEN '21-ESFORCADO'
            WHEN qtdFrequencia < 5 THEN '00-LURKER'
            WHEN qtdFrequencia <= 10 THEN '01-PREGUICOSO'
            WHEN qtdFrequencia > 10 THEN '20-POTENCIAL'
        END AS cluster
    FROM
        tb_freq_valor
)

SELECT
    date('{date}', '-1 day') AS dtRef,
    t1.*,
    t2.qtdFrequencia,
    t2.qtdePontosPos,
    t2.cluster
FROM
    tb_life_cycle AS t1
LEFT JOIN tb_cluster AS t2
    ON t1.idCliente = t2.idCliente
