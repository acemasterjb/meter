-- forked from $EUL  Volume, Trade Count, Avg Trade Size, Turnover Rate @ https://flipsidecrypto.xyz/edit/queries/71529f3d-e919-4487-95c1-9314335dd04d
WITH eul_swaps AS (
    SELECT
        block_timestamp,
        DATE_TRUNC('day', block_timestamp) AS day,
        CASE
            WHEN token_in = '0xd9fcd98c322942075a5c3860693e9f4f03aae07b' THEN DIV0(amount_out_usd, amount_in)
            ELSE DIV0(amount_in_usd, amount_out)
        END AS price,
        MAX(price) OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp
        ) AS high,
        MIN(price) OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp
        ) AS low,
        CASE
            WHEN token_in = '0xd9fcd98c322942075a5c3860693e9f4f03aae07b' THEN amount_out_usd
            ELSE amount_in_usd
        END AS amount_usd,
        SUM(amount_usd) OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp
        ) AS volume,
        COUNT(block_timestamp) OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp
        ) AS trade_count,
        ROW_NUMBER() OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp DESC
        ) AS rank
    FROM
        ethereum.defi.ez_dex_swaps
    WHERE
        (
            token_in = '0xd9fcd98c322942075a5c3860693e9f4f03aae07b'
            OR token_out = '0xd9fcd98c322942075a5c3860693e9f4f03aae07b'
        )
        AND block_timestamp > DATE('2021-12-31 00:00')
),
eul_volume_trade_count_ats_tr_inter AS (
    SELECT
        day,
        volume,
        trade_count,
        volume / trade_count AS avg_trade_size,
        AVG(volume) OVER (
            ORDER BY
                block_timestamp ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
        ) AS volume_7,
        AVG(price) OVER (
            ORDER BY
                block_timestamp ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
        ) AS close_7
    FROM
        eul_swaps
    WHERE
        rank = 1
),
uni_swaps AS (
    SELECT
        block_timestamp,
        DATE_TRUNC('day', block_timestamp) AS day,
        CASE
            WHEN token_in = '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984' THEN DIV0(amount_out_usd, amount_in)
            ELSE DIV0(amount_in_usd, amount_out)
        END AS price,
        MAX(price) OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp
        ) AS high,
        MIN(price) OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp
        ) AS low,
        CASE
            WHEN token_in = '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984' THEN amount_out_usd
            ELSE amount_in_usd
        END AS amount_usd,
        SUM(amount_usd) OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp
        ) AS volume,
        COUNT(block_timestamp) OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp
        ) AS trade_count,
        ROW_NUMBER() OVER (
            PARTITION BY day
            ORDER BY
                block_timestamp DESC
        ) AS rank
    FROM
        ethereum.defi.ez_dex_swaps
    WHERE
        (
            token_in = '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984'
            OR token_out = '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984'
        )
        AND block_timestamp > DATE('2021-12-31 00:00')
),
uni_volume_trade_count_ats_tr_inter AS (
    SELECT
        day,
        volume,
        trade_count,
        volume / trade_count AS avg_trade_size,
        AVG(volume) OVER (
            ORDER BY
                block_timestamp ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
        ) AS volume_7,
        AVG(price) OVER (
            ORDER BY
                block_timestamp ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
        ) AS close_7
    FROM
        uni_swaps
    WHERE
        rank = 1
),
defi_volume_trade_count_ats_tr AS (
    SELECT
        eul.day,
        eul.volume AS eul_volume,
        eul.trade_count AS eul_trade_count,
        eul.avg_trade_size AS eul_avg_trade_size,
        (
            eul.volume_7 / (27182818.284590452353602874 * eul.close_7)
        ) AS eul_turnover_rate,
        uni.volume AS uni_volume,
        uni.trade_count AS uni_trade_count,
        uni.avg_trade_size AS uni_avg_trade_size,
        (uni.volume_7 / (1000000000.00 * uni.close_7)) AS uni_turnover_rate
    FROM
        eul_volume_trade_count_ats_tr_inter AS eul
        JOIN uni_volume_trade_count_ats_tr_inter AS uni ON eul.day = uni.day
)
SELECT
    *
FROM
    defi_volume_trade_count_ats_tr
ORDER BY
    day