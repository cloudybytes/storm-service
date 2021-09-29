package com.example.storm.spouts;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class MoviesSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private Scanner sc;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.spoutOutputCollector = collector;
        String inputFolder = conf.get("InputFolder").toString();
        try {
            this.sc = new Scanner(new File(inputFolder + "movies.csv"));
            // this.sc.useDelimiter(",|\\r\\n");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        // String temp;
        if(sc.hasNext()) {
            // temp = sc.next();
            // // System.out.println("Temp = " + temp);
            // if(!temp.isEmpty()) {
            //     int movieId = Integer.parseInt(temp);
            //     String title = "";
            //     sc.useDelimiter("");
            //     char ch = sc.next().charAt(0);
            //     do {
            //         ch = sc.next().charAt(0);
            //         title = title + ch;
            //     } while(ch != '"');
            //     title = title.substring(0, title.length() - 1);
            //     // ch = sc.next().charAt(0);
            //     sc.useDelimiter(",|\\r\\n");
            //     // title = title.substring(1, title.length() - 1);
            //     String releaseDate = sc.next();
            //     releaseDate = releaseDate.substring(1, releaseDate.length() - 1);
            //     int unknown = Integer.parseInt(sc.next());
            //     int action = Integer.parseInt(sc.next());
            //     int adventure = Integer.parseInt(sc.next());
            //     int animation = Integer.parseInt(sc.next());
            //     int children = Integer.parseInt(sc.next());
            //     int comedy = Integer.parseInt(sc.next());
            //     int crime = Integer.parseInt(sc.next());
            //     int documentary = Integer.parseInt(sc.next());
            //     int drama = Integer.parseInt(sc.next());
            //     int fantasy = Integer.parseInt(sc.next());
            //     int filmNoir = Integer.parseInt(sc.next());
            //     int horror = Integer.parseInt(sc.next());
            //     int musical = Integer.parseInt(sc.next());
            //     int mystery = Integer.parseInt(sc.next());
            //     int romance = Integer.parseInt(sc.next());
            //     int sciFi = Integer.parseInt(sc.next());
            //     int thriller = Integer.parseInt(sc.next());
            //     int war = Integer.parseInt(sc.next());
            //     int western = Integer.parseInt(sc.next());
            //     spoutOutputCollector.emit(new Values(movieId, title, releaseDate, unknown, action, adventure, animation, children, comedy, crime, documentary, drama, fantasy, filmNoir, horror, musical, mystery, romance, sciFi, thriller, war, western));
            // }

            String temp = sc.nextLine();
            int index = temp.indexOf(',');
            int movieId = Integer.parseInt(temp.substring(0, index));
            int end = temp.indexOf("\",");
            String title = temp.substring(index + 2, end);
            temp = temp.substring(end + 2);
            index = temp.indexOf(',');
            String releaseDate = temp.substring(0, index);
            temp = temp.substring(index + 1);
            String genre[] = temp.split(",", -1);
            if(genre.length == 19) {
                int genres[] = new int[genre.length];
                for(int i = 0; i < genre.length; i++) {
                    genres[i] = Integer.parseInt(genre[i]);
                }
                spoutOutputCollector.emit(new Values(movieId, title, releaseDate, genres[0], genres[1], genres[2], genres[3], genres[4], genres[5], genres[6], genres[7], genres[8], genres[9], genres[10], genres[11], genres[12], genres[13], genres[14], genres[15], genres[16], genres[17], genres[18]));
            }
        } 
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("movieid", "title", "releasedate", "unknown", "action", "adventure", "animation", "children", "comedy", "crime", "documentary", "drama", "fantasy", "film_noir", "horror", "musical", "mystery", "romance", "sci_fi", "thriller", "war", "western"));        
    }
        
}
