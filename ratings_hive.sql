--Drops existing database
DROP DATABASE IF EXISTS movies CASCADE;

--Creates new database 
CREATE DATABASE IF NOT EXISTS movies;

--Use database
USE movies;

--Creates Ratings table
CREATE TABLE Ratings
(user_id INT,
movie_id INT,
rating INT,
ts INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

--Loads data from local data file
LOAD DATA LOCAl INPATH '/home/maria_dev/ratings.data' 
INTO TABLE Ratings;

--Get metadata of table
DESCRIBE Ratings;

--Get count of table
SELECT COUNT(*) FROM Ratings;

--Get the number of occurences for each ratings
SELECT rating, count(rating) as total_occurences
FROM Ratings
GROUP BY rating
ORDER BY total_occurences DESC;