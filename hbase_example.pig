--CREATING A RELATION BY LOADING USER DATA FILE
users = LOAD '/user/maria_dev/pig/movies.user' 
USING PigStorage('|') 
AS (userID:int, age:int, gender:chararray, occupation:chararray, zip:int);

--LOADING THE USERS IN HBASE TABLE
STORE users INTO 'hbase://users' 
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage ('userinfo:age,userinfo:gender,userinfo:occupation,userinfo:zip');
