{{ config(schema="unipilot", materialized="table") }}

WITH decoded_log AS (
  SELECT
    `spock-main.p_unipilot.decode_mint`(data, topics) AS decoded_data,
    LOWER(address) AS `pool`
  FROM 
    `spock-main.raw_data.ethereum_logs`
  WHERE 
    topics[0] = '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde'
)

SELECT 
    LOWER(decoded_data.sender) AS `sender`,
    LOWER(decoded_data.owner) AS `owner`,
    decoded_data.tickLower AS `tick_lower`,
    decoded_data.tickUpper AS `tick_upper`,
    decoded_data.amount AS `amount`,
    decoded_data.amount0 AS `amount_0`,
    decoded_data.amount1 AS `amount_1`,
    `pool`
FROM 
    decoded_log
