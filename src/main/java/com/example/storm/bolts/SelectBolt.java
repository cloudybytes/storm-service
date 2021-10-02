package com.example.storm.bolts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.springframework.stereotype.Component;

@Component
public class SelectBolt extends BaseBasicBolt {
    private String[] fields;
    
    public void setOutputFields(String[] fields) {
        this.fields = Arrays.copyOf(fields, fields.length);
        // for(int i = 0; i < fields.length; i++) {
        //     System.out.println(fields[i]);
        // }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        List<String> inputFields = new ArrayList<String>();
        for(int i = 0; i < input.getFields().size(); i++) {
            if(input.getFields().get(i).indexOf(":") != -1) {
                inputFields.add(input.getFields().get(i).split(":")[1].toString());
            } else {
                inputFields.add(input.getFields().get(i));
            }
            // System.out.println("Inout Field in BOlt = " + inputFields.get(i));
        }
        Values output = new Values();
        for(int i = 0; i < this.fields.length; i++) {
            if(inputFields.contains(this.fields[i])) {
                output.add(input.getValue(inputFields.indexOf(this.fields[i])));
            } else {
                output.add("");
            }
        }
        // System.out.println("Emitting = " + output.toString());
        collector.emit(output);
        // collector.emit(input.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(this.fields));        
    }
    
}
