-- models/bronze/brasileirao_serie_a_bronze.sql
-- +silver

with bronze_source_brasileirao_serie_a as (
    select * from {{ source('bronze', 'brasileirao_serie_a_bronze')}}
),

-- Realizando as transformações necessárias
partidas AS (
	SELECT
		ano_campeonato,
		rodada,
		time_mandante,
		time_visitante,
		colocacao_mandante,
		colocacao_visitante
	FROM bronze_source_brasileirao_serie_a
	--WHERE
	--	ano_campeonato = 2023
	ORDER BY rodada, "data"
),
rodadas AS (
	SELECT DISTINCT rodada FROM bronze_source_brasileirao_serie_a
	--WHERE ano_campeonato = 2023
),
times AS (
	SELECT DISTINCT time_mandante AS time FROM bronze_source_brasileirao_serie_a
	--WHERE ano_campeonato = 2023
), transformed_data AS (
    SELECT
        p.ano_campeonato,
        r.rodada,
        t.time,
        CASE WHEN t.time = p.time_mandante THEN colocacao_mandante ELSE colocacao_visitante END AS colocacao
    FROM rodadas r
    CROSS JOIN times t
    LEFT JOIN
        partidas p
    ON
        r.rodada = p.rodada
        AND CASE WHEN t.time = p.time_mandante
        THEN
            t.time = p.time_mandante
        ELSE
            t.time = p.time_visitante
        END
    --WHERE
    --	r.rodada = (SELECT MAX(rodada) FROM rodadas)
        --AND p.ano_campeonato = 2021
    order by p.ano_campeonato, r.rodada, colocacao
)

select
    *
from transformed_data
