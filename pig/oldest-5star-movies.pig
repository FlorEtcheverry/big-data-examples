-- Load main dataset
ratings = LOAD 'ml-100k/u.data' AS (userId:int, movieId:int, rating:int, ratingTime:int);
--DESCRIBE ratings;

metadata = LOAD ' ml-100k/u.item' USING PigStorage('|')
    AS (movieId:int, movieTitle:chararray, relaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

-- Transform date to Unix timestamp
nameLookup = FOREACH metadata GENERATE movieId, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

-- Group will generate a relation {group (movieId) :int, {ratings}}
ratingsByMovie = GROUP ratings BY movieId;
--DESCRIBE ratingByMovies;

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieId, AVG(ratings.rating) AS avgRating;
--DESCRIBE avgRatings;

fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;

fiveStarsWithData = JOIN fiveStarMovies BY movieId, nameLookup BY movieId;

oldestFiveStarMovies = ORDER fiveStarsWithData BY nameLookup::releaseTime;

DUMP oldestFiveStarMovies;
