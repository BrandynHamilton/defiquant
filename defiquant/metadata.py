PANDAS_TO_SNOWFLAKE_INTERVAL = {
    'h': 'HOUR',
    'd': 'DAY',
    'w': 'WEEK',
    'm': 'MONTH',   # careful: pandas 'm' is minute; use 'M' for month
    'q': 'QUARTER',
    'y': 'YEAR',
}