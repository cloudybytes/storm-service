package com.example.storm.bolts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class GroupByAndAggregateBolt extends BaseBasicBolt {
    private String[] fields;
    HashMap<Object, ArrayList<Object>> hashMap = new HashMap<Object, ArrayList<Object>>();
    public HashMap<Integer, Integer> countMap = new HashMap<Integer, Integer>();
    public HashMap<Integer, Integer> sumMap = new HashMap<Integer, Integer>();
    String groupByField = "";
    String aggregate = "";
    String aggregateColumn = "";

    @Override
    public void prepare(Map<String,Object> topoConf, TopologyContext context) {
        if(topoConf.containsKey("groupBy")) {
            groupByField = topoConf.get("groupBy").toString();
        }
        if(topoConf.containsKey("aggregate")) {
            aggregate = topoConf.get("aggregate").toString();
            aggregateColumn = topoConf.get("aggregateColumn").toString();
        }
        System.out.println("Inside bolt group and aggregate " + groupByField + "----" + aggregate + "+++++" + aggregateColumn);
    }

    public void setOutputFields(String []inputFields) {
        this.fields = Arrays.copyOf(inputFields, inputFields.length);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(this.fields));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(!groupByField.equals("")) {
            if(input.contains(groupByField)) {
                Object inputKey = input.getValueByField(groupByField);
                if(hashMap.containsKey(inputKey)) {
                    if(!aggregate.equals("")) {
                        ArrayList<Object> tuple = hashMap.get(inputKey);
                        if(aggregate.equals("count")) {
                            int value = (int) tuple.get(tuple.size() - 1);
                            value += 1;
                            tuple.set(tuple.size() - 1, value);
                        } else if(aggregate.equals("max")) {
                            int value = (int) tuple.get(tuple.size() - 1);
                            value = Math.max(value, input.getIntegerByField(aggregateColumn));
                            tuple.set(tuple.size() - 1, value);
                        } else if(aggregate.equals("min")) {
                            int value = (int) tuple.get(tuple.size() - 1);
                            value = Math.min(value, input.getIntegerByField(aggregateColumn));
                            tuple.set(tuple.size() - 1, value);
                        } else if(aggregate.equals("avg")) {
                            int count = countMap.get(input.getIntegerByField(groupByField));
                            count++;
                            int sum = sumMap.get(input.getIntegerByField(groupByField)) + input.getIntegerByField(aggregateColumn);
                            double avg = ((double) sum) / (double) count;
                            tuple.set(tuple.size() - 1, avg);
                            countMap.put(input.getIntegerByField(groupByField), count);
                            sumMap.put(input.getIntegerByField(groupByField), sum);                            
                        } else if(aggregate.equals("sum")) {
                            int value = (int) tuple.get(tuple.size() - 1);
                            value += input.getIntegerByField(aggregateColumn);
                            tuple.set(tuple.size() - 1, value);
                        }
                        Values output = new Values();
                        output.addAll(hashMap.get(inputKey));
                        collector.emit(output);
                    }
                } else {
                    hashMap.put(inputKey, new ArrayList<Object>(input.getValues()));
                    if(!aggregate.equals("")) {
                        if(aggregate.equals("count")) {
                            hashMap.get(inputKey).add(1);
                        } else if(aggregate.equals("max") || aggregate.equals("min") || aggregate.equals("sum")) {
                            hashMap.get(inputKey).add(input.getIntegerByField(aggregateColumn));
                            // System.out.println("New tuple fields = " + hashMap.get(input.getValueByField(groupByField)).toString());
                        }
                        if(aggregate.equals("avg")) {
                            hashMap.get(inputKey).add(new Double(input.getIntegerByField(aggregateColumn)));
                            countMap.put((int) inputKey, 1);
                            sumMap.put((int) inputKey, input.getIntegerByField(aggregateColumn));
                        }
                        // System.out.println("Emitting from groupBy = " + (new Values(hashMap.get(input.getIntegerByField(groupByField)))).toString());
                        Values output = new Values();
                        output.addAll(hashMap.get(inputKey));
                        // System.out.println("Emitted output = " + output.toString());
                        collector.emit(output);
                    } else {
                        // System.out.println("Emitting aggregate irrelevent = " + input.toString());
                        collector.emit(input.getValues());
                    }
                }
            }
        } else {
            // System.out.println("Emitting Groupby irrelevent = " + input.toString());
            collector.emit(input.getValues());
        }
    }
    
}
