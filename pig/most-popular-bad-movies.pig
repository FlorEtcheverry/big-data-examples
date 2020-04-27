-- Load main dataset
ratings = LOAD 'ml-100k/u.data' AS (userId:int, movieId:int, rating:int, ratingTime:int);

metadata = LOAD 'ml-100k/u.item' USING PigStorage('|')
    AS (movieId:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

-- Transform date to Unix timestamp
nameLookup = FOREACH metadata GENERATE movieId, movieTitle;

-- Group will generate a relation {group (movieId) :int, {ratings}}
ratingsByMovie = GROUP ratings BY movieId;

avgCountRatings = FOREACH ratingsByMovie GENERATE
    group AS movieId,
    AVG(ratings.rating) AS avgRating,
    COUNT(ratings.rating) AS countRating;

oneStarMovies = FILTER avgCountRatings BY avgRating < 2.0;

oneStarWithData = JOIN oneStarMovies BY movieId, nameLookup BY movieId;

mostPopularOneStarMovies = ORDER oneStarWithData BY oneStarMovies::countRating DESC;

DUMP mostPopularOneStarMovies;
