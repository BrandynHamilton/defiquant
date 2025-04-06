import datetime as dt
import pandas as pd
import requests
import time
import json

from defiquant.metadata import PANDAS_TO_SNOWFLAKE_INTERVAL

def token_dex_stats(network, token_address, start_date, freq='h'):

  freq = freq.lower()
  interval = PANDAS_TO_SNOWFLAKE_INTERVAL.get(freq)
  if not interval:
      raise ValueError(f"Unsupported frequency: {freq}")

  query = f"""

  WITH RECURSIVE date_series AS (
    SELECT
      TIMESTAMP '{start_date}' AS DT
    UNION
    ALL
    SELECT
      DT + INTERVAL '1 {interval}'
    FROM
      date_series
    WHERE
      DT + INTERVAL '1 {interval}' <= CURRENT_TIMESTAMP
  ),
  buyer_stats as (
    select
      date_trunc('{interval}', block_timestamp) as dt,
      sum(amount_out) as buying_volume,
      count(EZ_DEX_SWAPS_ID) as buy_orders,
      avg(amount_out) as average_buy
    from
      {network}.defi.ez_dex_swaps
    where
      TOKEN_OUT = lower('{token_address}')
    group by
      date_trunc('{interval}', block_timestamp)
  ),
  seller_stats as (
    select
      date_trunc('{interval}', block_timestamp) as dt,
      sum(amount_in) as selling_volume,
      count(EZ_DEX_SWAPS_ID) as sell_orders,
      avg(amount_in) as average_sold
    from
      {network}.defi.ez_dex_swaps
    where
      TOKEN_IN = lower('{token_address}')
    group by
      date_trunc('{interval}', block_timestamp)
  )
  select
    dss.dt,
    coalesce(b.buying_volume,0) BUYING_VOLUME,
    coalesce(s.selling_volume,0) SELLING_VOLUME,
    coalesce(b.buy_orders,0) BUY_ORDERS,
    coalesce(s.sell_orders,0) SELL_ORDERS,
    coalesce(b.average_buy,0) AVERAGE_BUY,
    coalesce(s.average_sold,0) AVERAGE_SOLD
  from
    date_series dss
    left join seller_stats s on s.dt = dss.dt
    left join buyer_stats b on b.dt = dss.dt
  order by
    b.dt desc 

    """

  return query

def active_addresses(address, network, start_date, freq='h'):
    freq = freq.lower()
    interval = PANDAS_TO_SNOWFLAKE_INTERVAL.get(freq)
    if not interval:
        raise ValueError(f"Unsupported frequency: {freq}")

    query = f"""
    WITH RECURSIVE date_series AS (
      SELECT TIMESTAMP '{start_date}' AS DT
      UNION ALL
      SELECT DT + INTERVAL '1 {interval}'
      FROM date_series
      WHERE DT + INTERVAL '1 {interval}' <= CURRENT_TIMESTAMP
    ),

    data AS (
      SELECT dt, COUNT(DISTINCT address) AS active_addresses
      FROM (
        SELECT DATE_TRUNC('{interval}', block_timestamp) AS dt,
               ORIGIN_FROM_ADDRESS AS address
        FROM {network}.core.ez_token_transfers
        WHERE CONTRACT_ADDRESS = LOWER('{address}')

        UNION ALL

        SELECT DATE_TRUNC('{interval}', block_timestamp) AS dt,
               ORIGIN_TO_ADDRESS AS address
        FROM {network}.core.ez_token_transfers
        WHERE CONTRACT_ADDRESS = LOWER('{address}')
      )
      GROUP BY dt
    )

    SELECT 
        dss.dt,
        COALESCE(d.active_addresses, 0) AS active_addresses 
    FROM date_series AS dss
    LEFT JOIN data AS d 
        ON dss.dt = d.dt;
    """
    return query

def transactions_overtime(address, network, start_date, freq='h'):

  freq = freq.lower()
  interval = PANDAS_TO_SNOWFLAKE_INTERVAL.get(freq)
  if not interval:
      raise ValueError(f"Unsupported frequency: {freq}")

  query = f"""

    WITH RECURSIVE date_series AS (
      SELECT TIMESTAMP '{start_date}' AS DT
      UNION ALL
      SELECT DT + INTERVAL '1 {interval}'
      FROM date_series
      WHERE DT + INTERVAL '1 {interval}' <= CURRENT_TIMESTAMP
    ),

    data AS (
      SELECT dt, COUNT(DISTINCT active_addresses) AS active_addresses
      FROM (
        SELECT DATE_TRUNC('{interval}', block_timestamp) AS dt,
              COUNT(DISTINCT ORIGIN_FROM_ADDRESS) AS active_addresses
        FROM {network}.core.ez_token_transfers
        WHERE CONTRACT_ADDRESS = LOWER('{address}')
        GROUP BY DATE_TRUNC('{interval}', block_timestamp)
        UNION ALL 
        SELECT DATE_TRUNC('{interval}', block_timestamp) AS dt,
              COUNT(DISTINCT ORIGIN_TO_ADDRESS) AS active_addresses
        FROM {network}.core.ez_token_transfers
        WHERE CONTRACT_ADDRESS = LOWER('{address}')
        GROUP BY DATE_TRUNC('{interval}', block_timestamp)
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

def token_prices(token_addresses, network, start_date, freq='h'):
    """
    Generate a SQL query to fetch median token prices by time frequency.

    Parameters:
    - token_addresses (list): List of token addresses (strings).
    - network (str): The blockchain network (e.g. ethereum, optimism).
    - start_date (str): Start datetime string in 'YYYY-MM-DD HH:MM:SS' format.
    - freq (str): Frequency shorthand ('h', 'd', 'w', 'm', 'q', 'y').

    Returns:
    - str: SQL query string.
    """
    freq = freq.lower()
    interval = PANDAS_TO_SNOWFLAKE_INTERVAL.get(freq)
    if not interval:
        raise ValueError(f"Unsupported frequency: {freq}")

    start_dt = dt.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    formatted_start = f"'{start_dt.strftime('%Y-%m-%d %H:%M:%S')}'"

    # Format token list into VALUES clause
    addresses_clause = ", ".join(f"(LOWER('{address}'))" for address in token_addresses)

    query = f"""
    WITH addresses AS (
        SELECT column1 AS token_address 
        FROM (VALUES
            {addresses_clause}
        ) AS tokens(column1)
    )

    SELECT 
        DATE_TRUNC('{interval}', hour) AS time_period,
        symbol,
        AVG(price) AS median_price
    FROM 
        {network}.price.ez_prices_hourly
    WHERE 
        token_address IN (SELECT token_address FROM addresses)
        AND hour >= DATE_TRUNC('{interval}', TO_TIMESTAMP({formatted_start}, 'YYYY-MM-DD HH24:MI:SS'))
    GROUP BY 
        1, 2
    ORDER BY 
        time_period DESC, symbol;
    """

    return query

def pool_data(address, network, start_date, freq='h'):
    
    freq = freq.lower()
    interval = PANDAS_TO_SNOWFLAKE_INTERVAL.get(freq)
    if not interval:
        raise ValueError(f"Unsupported frequency: {freq}")
    
    query = f"""

      WITH RECURSIVE date_series AS (
      SELECT
        TIMESTAMP '{start_date}' AS DT
      UNION
      ALL
      SELECT
        DT + INTERVAL '1 {interval}'
      FROM
        date_series
      WHERE
        DT + INTERVAL '1 {interval}' <= CURRENT_TIMESTAMP
    ),



    main_tokens as (

    SELECT
        USD_VALUE_NOW,
        symbol, 
        CONTRACT_ADDRESS,
        CURRENT_BAL,
        decimals,
      FROM
        {network}.core.ez_current_balances
      WHERE
        user_address = lower('{address}')
      AND USD_VALUE_NOW IS NOT NULL
      order by USD_VALUE_NOW desc 
      LIMIT 2
    ),

    symbols AS (
      SELECT
        distinct symbol from main_tokens
    ),
    date_series_symbol AS (
      SELECT
        d.DT,
        s.symbol
      FROM
        date_series d
        CROSS JOIN symbols s
    ),

    lp AS (
      SELECT
        date_trunc('{interval}', block_timestamp) AS dt,
        symbol,
        AVG(current_bal) AS current_bal,
        MAX(decimals) AS decimals
      FROM
        {network}.core.ez_balance_deltas
      WHERE
        user_address = lower('{address}')
        AND contract_address in (select distinct contract_address from main_tokens)
      GROUP BY
        date_trunc('{interval}', block_timestamp),
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
        LEFT JOIN lp hlp ON dss.DT = hlp.dt
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
        {network}.price.ez_prices_hourly
      where
        token_address in (select distinct contract_address from main_tokens)
        and hour <= date_trunc('{interval}',current_timestamp)
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

def wallet_flows(token_addresses, model_address, network, freq='h'):
   
  freq = freq.lower()
  interval = PANDAS_TO_SNOWFLAKE_INTERVAL.get(freq)
  if not interval:
    raise ValueError(f"Unsupported frequency: {freq}")

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
      date_trunc('{freq}',block_timestamp) as dt,
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
      date_trunc('{freq}',block_timestamp) as dt,
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
