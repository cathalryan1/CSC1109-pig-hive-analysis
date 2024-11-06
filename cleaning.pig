-- This file carries out the initial cleaning of the data using Pig.

-- Before conducting any analysis, the supermarket sales dataset needed to be cleaned and transformed into a suitable format for querying.
-- This process was performed locally using Apache Pig. The general steps and issues addressed are outlined below.

-- 1. CSVLoader was used to correctly load the data without splitting columns on quoted commas, ensuring data consistency.

sales_data = LOAD 'hdfs://localhost:9000/user/ryanc247/supermarket_sales/supermarket_sales.csv' USING PigStorage(',')
               AS (InvoiceID:chararray, Branch:chararray, City:chararray, CustomerType:chararray,
                   Gender:chararray, ProductLine:chararray, UnitPrice:float, Quantity:int,
                   Tax:float, Total:float, Date:chararray, Time:chararray, Payment:chararray,
                   COGS:float, GrossMarginPercentage:float, GrossIncome:float, Rating:float);


-- 2. The header row containing column names was removed.

cleaned_data = FILTER sales_data BY InvoiceID != 'InvoiceID';

-- 3. Transaction dates were reformatted into DD-MM-YYYY format to facilitate chronological analysis.

formatted_data = FOREACH cleaned_data GENERATE
                     InvoiceID,
                     Branch,
                     City,
                     CustomerType,
                     Gender,
                     ProductLine,
                     UnitPrice,
                     Quantity,
                     Tax,
                     Total,
                     ToString(ToDate(Date, 'MM/dd/yyyy'), 'dd-MM-yyyy') AS FormattedDate,
                     Time,
                     Payment,
                     COGS,
                     GrossMarginPercentage,
                     GrossIncome,
                     Rating;

-- 4. Rows with critical missing data were then filtered out.

filtered_data = FILTER formatted_data BY Quantity > 0 AND Total > 0;

-- 5. Once the data was cleaned and reformatted, the processed dataset was saved in outputs/clean_supermarket_sales.

STORE filtered_data INTO 'outputs/clean_supermarket_sales' USING PigStorage(',');

-- After preparing the data in Pig, I used Hive for further in-depth analysis on this cleaned dataset.