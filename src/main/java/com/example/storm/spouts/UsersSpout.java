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

public class UsersSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private Scanner sc;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.spoutOutputCollector = collector;
        String inputFolder = conf.get("InputFolder").toString();
        try {
            this.sc = new Scanner(new File(inputFolder + "users.csv"));
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
                int age = Integer.parseInt(sc.next());
                String gender = sc.next();
                gender = gender.substring(1, gender.length() - 1);
                String occupation = sc.next();
                occupation = occupation.substring(1, occupation.length() - 1);
                String zipcode = sc.next();
                int zip = Integer.parseInt(zipcode.substring(1, zipcode.length() - 1));
                spoutOutputCollector.emit(new Values(userId, age, gender, occupation, zip));
                // System.out.println("Value emitted");
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userid", "age", "gender", "occupation", "zipcode"));
    }
    
}
