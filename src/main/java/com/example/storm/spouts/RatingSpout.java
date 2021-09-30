package com.example.storm.spouts;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Scanner;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class RatingSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private Scanner sc;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        System.out.println("Config in Spout = " + conf.get("Key").toString());
        conf.put("Key", "Value1");
        this.spoutOutputCollector = collector;
        String inputFolder = conf.get("InputFolder").toString();
        try {
            this.sc = new Scanner(new File(inputFolder + "rating.csv"));
            this.sc.useDelimiter(",|\\r\\n");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        String temp;
        if(sc.hasNext()) {
            temp = sc.next();
            // System.out.println("Temp = " + temp);
            if(!temp.isEmpty()) {
                int userId = Integer.parseInt(temp);
                int movieId = Integer.parseInt(sc.next());
                int rating = Integer.parseInt(sc.next());
                String timeStamp = sc.next();
                spoutOutputCollector.emit(new Values(userId, movieId, rating, timeStamp));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userid", "movieid", "rating", "timestamp"));
    }
    
}
