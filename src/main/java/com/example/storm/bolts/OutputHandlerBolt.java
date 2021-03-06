package com.example.storm.bolts;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import com.example.storm.utils.TopologyUtils;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class OutputHandlerBolt extends BaseBasicBolt {
    String groupByKey = "";
    HashMap<Object, Tuple> hashMap; 
    int count = 0;

    public void setOutputMap(HashMap<Object, Tuple> map) {
        this.hashMap = map;
    }

    private class OutputWritter extends TimerTask {
        @Override
        public void run() {
            System.out.println("Size of hashMap = " + hashMap.size());
            try {
                int leftLimit = 48; // numeral '0'
                int rightLimit = 122; // letter 'z'
                int targetStringLength = 10;
                Random random = new Random();

                String generatedString = random.ints(leftLimit, rightLimit + 1)
                    .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                    .limit(targetStringLength)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
                TopologyUtils.fileName = generatedString;
                File myFile = new File("./src/main/resources/static/" + TopologyUtils.fileName + ".csv");
                FileWriter fileWriter = new FileWriter(myFile);
                for(Object key: hashMap.keySet()) {
                    String value = hashMap.get(key).getValues().toString();
                    fileWriter.write(value.substring(1, value.length() - 1) + "\n");
                }
                fileWriter.close();
            } catch(Exception e) {
                System.out.println("Exception in writing = " + e);
            }
        }
    }

    @Override
    public void prepare(Map<String,Object> topoConf, TopologyContext context) {
        if(topoConf.containsKey("groupBy")) {
            groupByKey = topoConf.get("groupBy").toString();
        }
        Timer timer = new Timer();
        OutputHandlerBolt.OutputWritter outputWritter = new OutputHandlerBolt.OutputWritter();
        timer.schedule(outputWritter, 30000);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        TopologyUtils.endTime.set(System.currentTimeMillis());
        if(!groupByKey.equals("")) {
            hashMap.put(input.getValueByField(groupByKey), input);
        } else {
            hashMap.put(count, input);
            count++;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("field", "field1"));
    }    
}
