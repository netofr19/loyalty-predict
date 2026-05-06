WITH tb_freq_valor AS (
    SELECT
        idCliente,
        count(DISTINCT substr(DtCriacao, 0, 11)) as qtdFrequencia,
        sum(CASE WHEN qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPos,
        sum(abs(qtdePontos)) AS qtdePontosAbs
    FROM
        transacoes
    WHERE
        DtCriacao < '2025-09-01'
        AND
        DtCriacao >= date('2025-09-01', '-28 days')
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
    *
FROM
    tb_cluster