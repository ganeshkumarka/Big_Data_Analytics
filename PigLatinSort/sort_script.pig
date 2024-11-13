-- Load the input data from HDFS
data = LOAD '/user/ganesh/BigData_Lab/PigLatinSort/input.txt' USING PigStorage(',') AS (value:int);

-- Sort the data in ascending order
sorted_data = ORDER data BY value;

-- Store the sorted data back to HDFS
STORE sorted_data INTO '/user/ganesh/BigData_Lab/PigLatinSort/output' USING PigStorage(',');
