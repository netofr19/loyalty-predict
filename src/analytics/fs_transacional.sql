WITH tb_transacao AS (
    SELECT
        *,
        substr(DtCriacao, 0, 11) as dtDia
    FROM
        transacoes
    WHERE
        DtCriacao < '2025-10-01'
),

tb_agg_transacao as (
    SELECT
        IdCliente,
        count(DISTINCT dtDia) AS saldoDVida,
        count(DISTINCT case when dtDia >= date('2025-10-01', '-7 day') THEN dtDia END) AS qtdeAtivacaoD7,
        count(DISTINCT case when dtDia >= date('2025-10-01', '-14 day') THEN dtDia END) AS qtdeAtivacaoD14,
        count(DISTINCT case when dtDia >= date('2025-10-01', '-28 day') THEN dtDia END) AS qtdeAtivacaoD28,
        count(DISTINCT case when dtDia >= date('2025-10-01', '-56 day') THEN dtDia END) AS qtdeAtivacaoD56,

        count(DISTINCT IdTransacao) AS qtdeTransacaoVida,
        count(DISTINCT case when dtDia >= date('2025-10-01', '-7 day') THEN IdTransacao END) AS qtdeTransacaoD7,
        count(DISTINCT case when dtDia >= date('2025-10-01', '-14 day') THEN IdTransacao END) AS qtdeTransacaoD14,
        count(DISTINCT case when dtDia >= date('2025-10-01', '-28 day') THEN IdTransacao END) AS qtdeTransacaoD28,
        count(DISTINCT case when dtDia >= date('2025-10-01', '-56 day') THEN IdTransacao END) AS qtdeTransacaoD56,

        sum(qtdePontos) AS saldoVida,
        sum(case when dtDia >= date('2025-10-01', '-7 day') THEN qtdePontos ELSE 0 END) AS saldoD7,
        sum(case when dtDia >= date('2025-10-01', '-14 day') THEN qtdePontos ELSE 0 END) AS saldoD14,
        sum(case when dtDia >= date('2025-10-01', '-28 day') THEN qtdePontos ELSE 0 END) AS saldoD28,
        sum(case when dtDia >= date('2025-10-01', '-56 day') THEN qtdePontos ELSE 0 END) AS saldoD56,

        sum(case when qtdePontos > 0 then qtdePontos else 0 end) AS qtdePontosPositivosVida,
        sum(case when dtDia >= date('2025-10-01', '-7 day') and qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPositivosD7,
        sum(case when dtDia >= date('2025-10-01', '-14 day') and qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPositivosD14,
        sum(case when dtDia >= date('2025-10-01', '-28 day') and qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPositivosD28,
        sum(case when dtDia >= date('2025-10-01', '-56 day') and qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPositivosD56,

        sum(case when qtdePontos < 0 then qtdePontos else 0 end) AS qtdePontosNegativosVida,
        sum(case when dtDia >= date('2025-10-01', '-7 day') and qtdePontos < 0 THEN qtdePontos ELSE 0 END) AS qtdePontosNegativosD7,
        sum(case when dtDia >= date('2025-10-01', '-14 day') and qtdePontos < 0 THEN qtdePontos ELSE 0 END) AS qtdePontosNegativosD14,
        sum(case when dtDia >= date('2025-10-01', '-28 day') and qtdePontos < 0 THEN qtdePontos ELSE 0 END) AS qtdePontosNegativosD28,
        sum(case when dtDia >= date('2025-10-01', '-56 day') and qtdePontos < 0 THEN qtdePontos ELSE 0 END) AS qtdePontosNegativosD56

    FROM
        tb_transacao
    GROUP BY
        IdCliente
),

tb_agg_calc as (
    select
        *,
        coalesce(1. * qtdeTransacaoD7 / qtdeAtivacaoD7, 0) as qtdeTransacaoDiaD7,
        coalesce(1. * qtdeTransacaoD14 / qtdeAtivacaoD14, 0) as qtdeTransacaoDiaD14,
        coalesce(1. * qtdeTransacaoD28 / qtdeAtivacaoD28, 0) as qtdeTransacaoDiaD28,
        coalesce(1. * qtdeTransacaoD56 / qtdeAtivacaoD56, 0) as qtdeTransacaoDiaD56,

        coalesce(1. * qtdeAtivacaoD28 / 28, 0) as pctAtivacaoMAU
    from
        tb_agg_transacao
),

tb_horas_dia as (
    select
        idCliente,
        dtDia,
        (max(julianday(DtCriacao)) - min(julianday(DtCriacao))) * 24 as duracao
    from
        tb_transacao
    group by idCliente, dtDia
),

tb_hora_cliente as (
    select
        idCliente,
        sum(duracao) as qtdeHorasVida,
        sum(case when dtDia >= date('2025-10-01', '-7 day') then duracao else 0 end) as qtdeHorasD7,
        sum(case when dtDia >= date('2025-10-01', '-14 day') then duracao else 0 end) as qtdeHorasD14,
        sum(case when dtDia >= date('2025-10-01', '-28 day') then duracao else 0 end) as qtdeHorasD28,
        sum(case when dtDia >= date('2025-10-01', '-56 day') then duracao else 0 end) as qtdeHorasD56

    from
        tb_horas_dia
    group by
        idCliente
),

tb_lag_dia as (
    SELECT
        idCliente,
        dtDia,
        LAG(dtDia) OVER (PARTITION BY idCliente ORDER BY dtDia) as lagDia
    FROM
        tb_horas_dia
),

tb_intervalo_dias AS (
    SELECT
        idCliente,
        avg(julianday(dtDia) - julianday(lagDia)) as avgIntervaloDiasVida,
        avg(CASE WHEN dtDia >= date('2025-10-01', '-28 day') THEN julianday(dtDia) - julianday(lagDia) END) as avgIntervaloDiasD28
    FROM
        tb_lag_dia
    GROUP BY
        idCliente
)

SELECT
    t1.*,
    t2.qtdeHorasVida,
    t2.qtdeHorasD7,
    t2.qtdeHorasD14,
    t2.qtdeHorasD28,
    t2.qtdeHorasD56,
    t3.avgIntervaloDiasVida,
    t3.avgIntervaloDiasD28
FROM
    tb_agg_calc as t1
LEFT JOIN
    tb_hora_cliente as t2
    ON t1.IdCliente = t2.IdCliente
LEFT JOIN
    tb_intervalo_dias AS t3
    ON t1.idCliente = t3.idCliente