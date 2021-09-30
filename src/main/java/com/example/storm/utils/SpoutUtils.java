package com.example.storm.utils;

public class SpoutUtils {
    public static String getSpoutName(String tableName) {
        String spout = tableName;
        spout = tableName.substring(0, 1).toUpperCase() + tableName.substring(1) + "Spout";
        return spout;
    }

    public static String getCommaSperatedFields(String tableName) {
        if(tableName.equals("users")) {
            return "UsersSpout:userid, UsersSpout:age, UsersSpout:gender, UsersSpout:occupation, UsersSpout:zipcode";
        } else if(tableName.equals("rating")) {
            return "RatingSpout:userid, RatingSpout:movieid, RatingSpout:rating, RatingSpout:timestamp";
        } else if(tableName.equals("zipcodes")) {
            return "ZipcodesSpout:zipcode, ZipcodesSpout:zipcodetype, ZipcodesSpout:city, ZipcodesSpout:state";
        } else if(tableName.equals("movies")) {
            return "MoviesSpout:movieid, MoviesSpout:title, MoviesSpout:releasedate, MoviesSpout:unknown, MoviesSpout:action, MoviesSpout:adventure, MoviesSpout:animation, MoviesSpout:children, MoviesSpout:comedy, MoviesSpout:crime, MoviesSpout:documentary, MoviesSpout:drama, MoviesSpout:fantasy, MoviesSpout:film_noir, MoviesSpout:horror, MoviesSpout:musical, MoviesSpout:mystery, MoviesSpout:romance, MoviesSpout:sci_fi, MoviesSpout:thriller, MoviesSpout:war, MoviesSpout:western";
        } else {
            return "";
        }
    }
}
