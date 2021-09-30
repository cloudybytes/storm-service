package com.example.storm.bolts;

import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class WhereBolt extends BaseBasicBolt {
    private String where[];
    private Boolean containsWhere = false;

    @Override
    public void prepare(Map<String,Object> topoConf, TopologyContext context) {
        if(topoConf.containsKey("where")) {
            containsWhere = true;
            where = (String [])topoConf.get("where");
        }
    }


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // collector.emit(input);        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        
    }
    
}
