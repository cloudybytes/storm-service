package com.example.storm.bolts;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TestBolt extends BaseBasicBolt {
    private Boolean temp = true;

    @Override
    public void prepare(Map<String,Object> topoConf, TopologyContext context) {
        System.out.println("Config in Bolt = " + topoConf.get("Key").toString());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // System.out.println("***********************************************");
        if(temp) {
            System.out.println("Tuple Received in testBolt = " + input.toString());
            temp = !temp;
        }
        collector.emit(new Values(1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("field"));
    }    
}
