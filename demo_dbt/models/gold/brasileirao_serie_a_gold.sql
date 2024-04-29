-- models/bronze/brasileirao_serie_a_bronze.sql
-- +gold

with bronze_source_brasileirao_serie_a as (
    select * from {{ source('silver', 'brasileirao_serie_a_silver')}}
),

transformed_data AS (
    WITH ultimas_rodadas AS (
        SELECT
            ano_campeonato,
            MAX(rodada) AS ultima_rodada
        FROM silver.brasileirao_serie_a_silver
        GROUP BY ano_campeonato
    )
    SELECT
        s.ano_campeonato,
        s.time,
        s.colocacao
    FROM
        silver.brasileirao_serie_a_silver s
    JOIN
        ultimas_rodadas u ON s.ano_campeonato = u.ano_campeonato AND s.rodada = u.ultima_rodada
    WHERE
        s.colocacao = 1
    ORDER BY
        s.ano_campeonato
)

select
    *
from transformed_data
