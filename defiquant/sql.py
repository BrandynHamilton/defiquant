import datetime as dt
import pandas as pd
import requests
import time
import json

def stablecoin_volume_func(start_date): 

  query = f"""

  WITH RECURSIVE date_series AS (
    SELECT
      TIMESTAMP '{start_date}' AS DT
    UNION
    ALL
    SELECT
      DT + INTERVAL '1 HOUR'
    FROM
      date_series
    WHERE
      DT + INTERVAL '1 HOUR' <= CURRENT_TIMESTAMP
  ),

  addresses AS (
    SELECT
      column1 AS token_address
    FROM
      (
        VALUES
          (
            LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')
          ),
          (
            LOWER('0xdAC17F958D2ee523a2206206994597C13D831ec7')
          ),
          (
            LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F')
          )
      ) AS tokens(column1)
  ),
  stable_vol AS (
    SELECT
      dt,
      SUM(volume) AS stablecoin_volume
    FROM
      (
        SELECT
          DATE_TRUNC('hour', block_timestamp) AS dt,
          SUM(amount_in_usd) AS volume
        FROM
          ethereum.defi.ez_dex_swaps
        WHERE
          TOKEN_IN IN (
            SELECT
              token_address
            FROM
              addresses
          )
        GROUP BY
          DATE_TRUNC('hour', block_timestamp)
        UNION
        ALL
        SELECT
          DATE_TRUNC('hour', block_timestamp) AS dt,
          SUM(amount_out_usd) AS volume
        FROM
          ethereum.defi.ez_dex_swaps
        WHERE
          TOKEN_OUT IN (
            SELECT
              token_address
            FROM
              addresses
          )
        GROUP BY
          DATE_TRUNC('hour', block_timestamp)
      ) AS combined_volumes
    GROUP BY
      dt
  ),
  individual_vols AS (
    SELECT
      dt,
      symbol,
      SUM(volume) AS token_volume
    FROM
      (
        SELECT
          DATE_TRUNC('hour', block_timestamp) AS dt,
          TOKEN_IN AS SYMBOL,
          SUM(amount_in_usd) AS volume
        FROM
          ethereum.defi.ez_dex_swaps
        WHERE
          TOKEN_IN IN (
            SELECT
              token_address
            FROM
              addresses
          )
        GROUP BY
          DATE_TRUNC('hour', block_timestamp),
          TOKEN_IN
        UNION
        ALL
        SELECT
          DATE_TRUNC('hour', block_timestamp) AS dt,
          TOKEN_OUT as SYMBOL,
          SUM(amount_out_usd) AS volume
        FROM
          ethereum.defi.ez_dex_swaps
        WHERE
          TOKEN_OUT IN (
            SELECT
              token_address
            FROM
              addresses
          )
        GROUP BY
          DATE_TRUNC('hour', block_timestamp),
          TOKEN_OUT
      ) AS individual_volumes
    GROUP BY
      dt,
      SYMBOL
  )
  SELECT 
    dt,
    LAST_VALUE(stablecoin_volume IGNORE NULLS)
      OVER (
        ORDER BY dt
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS stablecoin_volume
  FROM (
    SELECT 
      dss.dt,
      sv.stablecoin_volume
    FROM date_series dss
    LEFT JOIN stable_vol sv 
      ON sv.dt = dss.dt
  )
  ORDER BY dt DESC;

  """
  return query

def token_dex_stats(network, token_address, start_date):

  query = f"""

WITH RECURSIVE date_series AS (
  SELECT
    TIMESTAMP '{start_date}' AS DT
  UNION
  ALL
  SELECT
    DT + INTERVAL '1 HOUR'
  FROM
    date_series
  WHERE
    DT + INTERVAL '1 HOUR' <= CURRENT_TIMESTAMP
),
buyer_stats as (
  select
    date_trunc('hour', block_timestamp) as dt,
    sum(amount_out) as buying_volume,
    count(EZ_DEX_SWAPS_ID) as buy_orders,
    avg(amount_out) as average_buy
  from
    {network}.defi.ez_dex_swaps
  where
    TOKEN_OUT = lower('{token_address}')
  group by
    date_trunc('hour', block_timestamp)
),
seller_stats as (
  select
    date_trunc('hour', block_timestamp) as dt,
    sum(amount_in) as selling_volume,
    count(EZ_DEX_SWAPS_ID) as sell_orders,
    avg(amount_in) as average_sold
  from
    {network}.defi.ez_dex_swaps
  where
    TOKEN_IN = lower('{token_address}')
  group by
    date_trunc('hour', block_timestamp)
)
select
  dss.dt,
  coalesce(b.buying_volume,0) AMPL_BUYING_VOLUME,
  coalesce(s.selling_volume,0) AMPL_SELLING_VOLUME,
  coalesce(b.buy_orders,0) AMPL_BUY_ORDERS,
  coalesce(s.sell_orders,0) AMPL_SELL_ORDERS,
  coalesce(b.average_buy,0) average_buy,
  coalesce(s.average_sold,0) average_sold
from
  date_series dss
  left join seller_stats s on s.dt = dss.dt
  left join buyer_stats b on b.dt = dss.dt
order by
  b.dt desc 



  """

  return query

def active_addresses(address, start_date):
    query = f"""

WITH RECURSIVE date_series AS (
  SELECT TIMESTAMP '{start_date}' AS DT
  UNION ALL
  SELECT DT + INTERVAL '1 HOUR'
  FROM date_series
  WHERE DT + INTERVAL '1 HOUR' <= CURRENT_TIMESTAMP
),

data AS (
  SELECT dt, COUNT(DISTINCT active_addresses) AS active_addresses
  FROM (
    SELECT DATE_TRUNC('hour', block_timestamp) AS dt,
           COUNT(DISTINCT ORIGIN_FROM_ADDRESS) AS active_addresses
    FROM arbitrum.core.ez_token_transfers
    WHERE CONTRACT_ADDRESS = LOWER('{address}')
    GROUP BY DATE_TRUNC('hour', block_timestamp)
    UNION ALL 
    SELECT DATE_TRUNC('hour', block_timestamp) AS dt,
           COUNT(DISTINCT ORIGIN_TO_ADDRESS) AS active_addresses
    FROM arbitrum.core.ez_token_transfers
    WHERE CONTRACT_ADDRESS = LOWER('{address}')
    GROUP BY DATE_TRUNC('hour', block_timestamp)
  )
  GROUP BY dt
)

SELECT 
    dss.dt,
    coalesce(d.active_addresses, 0) active_addresses 
FROM date_series AS dss
LEFT JOIN data AS d 
    ON dss.dt = d.dt;


"""
    return query

def token_prices(token_addresses, network, start_date):
    start_date = dt.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    """
    Generate a SQL query to get historical price data for given token addresses from a specific start date.

    Parameters:
    - token_addresses (list): List of token addresses.
    - start_date (str): Start date in 'YYYY-MM-DD' format.

    Returns:
    - str: The SQL query string.
    """
    # Format the addresses into the SQL VALUES clause
    addresses_clause = ", ".join(f"(LOWER('{address}'))" for address in token_addresses)

    beginning = f"'{start_date.strftime('%Y-%m-%d %H:%M:%S')}'"
    print('Beginning:', beginning)
    
    prices_query = f"""
    WITH addresses AS (
        SELECT column1 AS token_address 
        FROM (VALUES
            {addresses_clause}
        ) AS tokens(column1)
    )

    SELECT 
        hour,
        symbol,
        price
    FROM 
        {network}.price.ez_prices_hourly
    WHERE 
        token_address IN (SELECT token_address FROM addresses)
        AND hour >= DATE_TRUNC('hour', TO_TIMESTAMP({beginning}, 'YYYY-MM-DD HH24:MI:SS'))
    ORDER BY 
        hour DESC, symbol
    """

    return prices_query

def lp_data(address):
    query = f"""

  WITH RECURSIVE date_series AS (
  SELECT
    TIMESTAMP '2021-01-11 00:00:00' AS DT
  UNION
  ALL
  SELECT
    DT + INTERVAL '1 HOUR'
  FROM
    date_series
  WHERE
    DT + INTERVAL '1 HOUR' <= CURRENT_TIMESTAMP
),
symbols AS (
  SELECT
    'AMPL' AS symbol
  UNION
  ALL
  SELECT
    'WETH'
),
date_series_symbol AS (
  SELECT
    d.DT,
    s.symbol
  FROM
    date_series d
    CROSS JOIN symbols s
),
hourly_lp AS (
  SELECT
    date_trunc('hour', block_timestamp) AS dt,
    symbol,
    AVG(current_bal) AS current_bal,
    MAX(decimals) AS decimals
  FROM
    ethereum.core.ez_balance_deltas
  WHERE
    user_address = lower('{address}')
    AND symbol IN ('AMPL', 'WETH')
  GROUP BY
    date_trunc('hour', block_timestamp),
    symbol
),
joined_data AS (
  SELECT
    dss.DT,
    dss.symbol,
    hlp.current_bal,
    hlp.decimals
  FROM
    date_series_symbol dss
    LEFT JOIN hourly_lp hlp ON dss.DT = hlp.dt
    AND dss.symbol = hlp.symbol
),
front_filled_data AS (
  SELECT
    DT,
    symbol,
    LAST_VALUE(current_bal IGNORE NULLS) OVER (
      PARTITION BY symbol
      ORDER BY
        DT ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS current_bal,
    LAST_VALUE(decimals IGNORE NULLS) OVER (
      PARTITION BY symbol
      ORDER BY
        DT ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS decimals
  FROM
    joined_data
),
prices as (
  select
    hour,
    symbol,
    price
  from
    ethereum.price.ez_prices_hourly
  where
    token_address in (
      lower('0xd46ba6d942050d489dbd938a2c909a5d5039a161'),
      lower('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
    )
   AND hour <= date_trunc('hour',current_timestamp)
  order by
    hour desc
),
tvl_per_token as (
  SELECT
    ff.DT,
    ff.symbol,
    ff.current_bal,
    p.price,
    ff.current_bal * p.price as TVL
  FROM
    front_filled_data ff
    join prices p on p.hour = ff.DT
    and p.symbol = ff.symbol
  ORDER BY
    DT,
    symbol
),
total_tvl as (
  select
    dt,
    sum(TVL) over (
      partition by dt
      order by
        dt desc
    ) as Total_TVL
  from
    tvl_per_token
  order by
    dt desc
)
select
  a.dt,
  a.symbol,
  a.current_bal,
  a.TVL,
  sum(a.TVL) over (
    partition by dt
    order by
      dt desc
  ) as Total_TVL
from
  tvl_per_token a
order by
  a.dt desc

"""
    return query

def wallet_flows(token_addresses, model_address, network):
   addresses_clause = ", ".join(f"(LOWER('{address}'))" for address in token_addresses)
   query = f"""

WITH portfolio AS (
        SELECT column1 AS token_address 
        FROM (VALUES
            {addresses_clause}
        ) AS tokens(column1)
    ),
inflows as (
  select
    date_trunc('hour',block_timestamp) as dt,
    SYMBOL,
    amount_usd,
    'inflow' as transaction_type
  from
    {network}.core.ez_token_transfers
  where
    to_address = lower('{model_address}')
    AND amount_usd is not NULL
    AND TX_HASH NOT IN (
      SELECT
        DISTINCT TX_HASH
      FROM
        {network}.defi.ez_dex_swaps
    )
    AND CONTRACT_ADDRESS IN (
      select
        distinct token_address
      from
        portfolio
    )
),
outflows as (
  select
    date_trunc('hour',block_timestamp) as dt,
    SYMBOL,
    amount_usd * -1 as amount_usd,
    -- Use negative values for outflows
    'outflow' as transaction_type
  from
    {network}.core.ez_token_transfers
  where
    from_address = lower('{model_address}')
    AND amount_usd is not NULL
    AND TX_HASH NOT IN (
      SELECT
        DISTINCT TX_HASH
      FROM
        {network}.defi.ez_dex_swaps
    )
    AND CONTRACT_ADDRESS IN (
      select
        distinct token_address
      from
        portfolio
    )
)
select
  *
from
  inflows
union
all
select
  *
from
  outflows
order by
  dt;


"""
   return query
