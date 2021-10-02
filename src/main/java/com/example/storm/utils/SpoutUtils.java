package com.example.storm.utils;

public class SpoutUtils {
    public static String getSpoutName(String tableName) {
        String spout = tableName;
        spout = tableName.substring(0, 1).toUpperCase() + tableName.substring(1) + "Spout";
        return spout;
    }

    public static String getCommaSperatedFields(String tableName) {
        if(tableName.equals("users")) {
            return "userid,age,gender,occupation,zipcode";
        } else if(tableName.equals("rating")) {
            return "userid,movieid,rating,timestamp";
        } else if(tableName.equals("zipcodes")) {
            return "zipcode,zipcodetype,city,state";
        } else if(tableName.equals("movies")) {
            return "movieid,title,releasedate,unknown,action,adventure,animation,children,comedy,crime,documentary,drama,fantasy,film_noir,horror,musical,mystery,romance,sci_fi,thriller,war,western";
        } else {
            return "";
        }
    }

    public static String getCommaSperatedFieldsTruncated(String tableName1, String tableName2) {
        if(tableName2.equals("users")) {
            if(tableName1.equals("rating")) {
                return "age,gender,occupation,zipcode";
            } else {
                return "userid,age,gender,occupation";
            }
        } else if(tableName2.equals("rating")) {
            if(tableName1.equals("users")) {
                return "movieid,rating,timestamp";
            } else {
                return "userid,rating,timestamp";
            }
        } else if(tableName2.equals("zipcodes")) {
            return "zipcodetype,city,state";
        } else if(tableName2.equals("movies")) {
            return "title,releasedate,unknown,action,adventure,animation,children,comedy,crime,documentary,drama,fantasy,film_noir,horror,musical,mystery,romance,sci_fi,thriller,war,western";
        } else {
            return "";
        }
    }
}
