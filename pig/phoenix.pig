REGISTER /usr/hdp/current/phoenix-client/phoenix-client.jar;

users = LOAD '/ml-100k/u.user'
        USING PigStorage('|')
        AS (USERID:int, AGE:int, GENDER:chararray, OCCUPATION:chararray, ZIP:chararray);

STORE users INTO 'hbase://users'
        USING org.apache.phoenix.pig.PhoenixHBaseStorage('localhost','-batchSize 5000');

-- Only userid and occupation columns from the users table
occupations = LOAD 'hbase://table/users/USERID,OCCUPATION'
              USING org.apache.phoenix.pig.PhoenixHBaseLoader('localhost');

-- Querying
grouped = GROUP occupations BY OCCUPATION;
count = FOREACH grouped GENERATE group AS OCCUPATION, COUNT(occupations);

DUMP count;
