use role accountadmin;

use warehouse compute_wh;

Drop database binance_stg;

create or replace database binance;

create or replace schema binance.staging;
create or replace schema binance.prod;

use database binance;
use schema binance.staging;
create or replace table binance.staging.BTC_trades
(json_VALUE VARIANT,  ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP);
create or replace table binance.staging.BTC_PRICES
(json_VALUE VARIANT,  ingest_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP);

--------------------------------------------------------------

show storage integrations;
----------

CREATE OR REPLACE FILE FORMAT JSON_FILES
    TYPE = JSON
    -- STRIP_OUTER_ARRAY = TRUE -- IF USING JSON ARRARY IN FILE
    ;
 -------------------------------------------
 
-- Create external stage via UI or
CREATE STAGE Azure_stg_price 
	URL = 'azure://1data06.blob.core.windows.net/binance/BTC_prices/' 
	CREDENTIALS = ( AZURE_SAS_TOKEN = '*****' ) 
	DIRECTORY = ( ENABLE = true );
CREATE STAGE Azure_stg_price 
	URL = 'azure://1data06.blob.core.windows.net/binance/trade/' 
	CREDENTIALS = ( AZURE_SAS_TOKEN = '*****' ) 
	DIRECTORY = ( ENABLE = true );

    ----------------------------
                CREATE OR REPLACE PIPE BTC_PRICES_PIPE
                    --AUTO_INGEST = TRUE    -- removed auto_ingest as i didnt enable event notifications and will manually refresh the pipe 
                AS 
                    COPY INTO binance.staging.BTC_PRICES (JSON_VALUE)
                    FROM @Azure_stg_price
                --PATTERN = '.*\\.json$'
                FILE_FORMAT = JSON_FILES
                ON_ERROR = 'CONTINUE';
-- ###############
                    CREATE OR REPLACE PIPE BTC_trade_PIPE
                        --AUTO_INGEST = TRUE    -- removed auto_ingest as i didnt enable event notifications and will manually refresh the pipe 
                    AS 
                        COPY INTO binance.staging.BTC_trades (JSON_VALUE)
                        FROM @Azure_stg_trade
                    --PATTERN = '.*\\.json$'
                    FILE_FORMAT = JSON_FILES
                    ON_ERROR = 'CONTINUE';
----------------------------
alter pipe BTC_PRICES_PIPE refresh;
alter pipe BTC_trade_PIPE refresh;

-- to manually run the pipe

 select * from    BTC_trades;
