-- forked from Solend Prices @ https://flipsidecrypto.xyz/edit/queries/fd354036-93ee-411a-838b-fb465f098270
WITH slnd_swaps AS (
    SELECT
        swaps.block_timestamp,
        swaps.tx_id,
        prices.close,
        prices.close * swaps.swap_from_amount AS amount_usd
    FROM
        solana.defi.fact_swaps AS swaps
        INNER JOIN solana.price.ez_token_prices_hourly AS prices ON DATE_TRUNC('hour', swaps.block_timestamp) = prices.recorded_hour
        AND prices.token_address = 'SLNDpmoWTVADgEdndyvWzroNL7zSi1dF9PC3xHGtPwp'
    WHERE
        'SLNDpmoWTVADgEdndyvWzroNL7zSi1dF9PC3xHGtPwp' = swaps.swap_from_mint
        AND swaps.block_timestamp > DATE('2022-11-27')
        AND swaps.succeeded = TRUE
),
slnd_volume_tradeCount_ats_inter AS (
    SELECT
        DATE_TRUNC('day', block_timestamp) AS day,
        SUM(amount_usd) OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp
        ) AS volume,
        COUNT(tx_id) OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp
        ) AS trade_count,
        AVG(amount_usd) OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp
        ) AS avg_trade_size,
        AVG(close) OVER (
            ORDER BY
                day ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
        ) AS close_7,
        ROW_NUMBER() OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp DESC
        ) AS rank
    FROM
        slnd_swaps
),
slnd_volume_tradeCount_ats AS (
    SELECT
        day,
        volume,
        trade_count,
        avg_trade_size,
        AVG(volume) OVER (
            ORDER BY
                day ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
        ) AS volume_7,
        (volume_7 / (99999799.69 * close_7)) AS turnover_rate
    FROM
        slnd_volume_tradeCount_ats_inter
    WHERE
        rank = 1
)
SELECT
    *
FROM
    slnd_volume_tradeCount_ats
ORDER BY
    day