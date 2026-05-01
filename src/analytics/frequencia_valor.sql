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