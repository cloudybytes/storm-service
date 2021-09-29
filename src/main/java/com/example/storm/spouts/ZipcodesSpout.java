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

public class ZipcodesSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private Scanner sc;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.spoutOutputCollector = collector;
        String inputFolder = conf.get("InputFolder").toString();
        try {
            this.sc = new Scanner(new File(inputFolder + "zipcodes.csv"));
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
                int zip = Integer.parseInt(temp.substring(1, temp.length() - 1));
                String type = sc.next();
                type = type.substring(1, type.length() - 1);
                String city = sc.next();
                city = city.substring(1, city.length() - 1);
                String state = sc.next();
                state = state.substring(1, state.length() - 1);
                spoutOutputCollector.emit(new Values(zip, type, city, state));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("zipcode", "zipcodetype", "city", "state"));
    }
    
}
