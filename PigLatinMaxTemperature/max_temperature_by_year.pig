-- Load the data from HDFS. Assume data is comma-separated with fields year and temperature
weather_data = LOAD '/user/ganesh/BigData_Lab/PigLatinMaxTemperature/weather_data.txt' USING PigStorage(',') AS (year:int, temperature:int);

-- Group the data by year
grouped_data = GROUP weather_data BY year;

-- Calculate the maximum temperature for each year
max_temp_by_year = FOREACH grouped_data GENERATE group AS year, MAX(weather_data.temperature) AS max_temperature;

-- Store the result back to HDFS
STORE max_temp_by_year INTO '/user/ganesh/BigData_Lab/PigLatinMaxTemperature/output' USING PigStorage(',');
