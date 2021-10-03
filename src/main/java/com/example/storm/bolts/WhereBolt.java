package com.example.storm.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.ListUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class WhereBolt extends BaseBasicBolt {
    private String where[] = new String[3];
    private Boolean containsWhere = false;
    private List<String> fields = new ArrayList<String>();
    int whereIndex = -1;
    
    public void setOutputFields(String[] inputFields) {
        // System.out.println("Setting output fields");
        for(int i = 0; i < inputFields.length; i++) {
            // if(inputFields[i].indexOf(':') == -1) {
            //     fields.add(inputFields[i]);
            // } else {
            //     fields.add(inputFields[i].split(":")[1].toString());
            // }
            fields.add(inputFields[i]);
            // System.out.println("Column Name = " + fields.get(i));
        }
        // fields = fields.stream().distinct().collect(Collectors.toList());
    }

    public List<String> getOutputFields() {
        return fields;
    }

    @Override
    public void prepare(Map<String,Object> topoConf, TopologyContext context) {
        if(topoConf.containsKey("where[0]")) {
            containsWhere = true;
            where[0] = (String) topoConf.get("where[0]");
            where[1] = (String) topoConf.get("where[1]");
            where[2] = (String) topoConf.get("where[2]");
        } else {
            where = null;
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // System.out.println("Tuple in where = " + input.toString());
        if(this.containsWhere) {
            if(this.where[1].equals(">")) {
                if(input.contains(this.where[0]) && (input.getValueByField(this.where[0]) instanceof Integer)) {
                    int val = (Integer)input.getValueByField(this.where[0]);
                    int constant = Integer.parseInt(this.where[2]);
                    if(val > constant) {
                        // System.out.println("Emitting from Where = " + input.toString());
                        collector.emit(input.getValues());
                    }
                }
            } else if(this.where[1].equals(">=")) {
                if(input.contains(this.where[0]) && (input.getValueByField(this.where[0]) instanceof Integer)) {
                    int val = (Integer)input.getValueByField(this.where[0]);
                    int constant = Integer.parseInt(this.where[2]);
                    if(val >= constant) {
                        // System.out.println("Emitting from Where = " + input.toString());
                        collector.emit(input.getValues());
                    }
                }
            } else if(this.where[1].equals("<")) {
                if(input.contains(this.where[0]) && (input.getValueByField(this.where[0]) instanceof Integer)) {
                    int val = (Integer)input.getValueByField(this.where[0]);
                    int constant = Integer.parseInt(this.where[2]);
                    if(val < constant) {
                        // System.out.println("Emitting from Where = " + input.toString());
                        collector.emit(input.getValues());
                    }
                }
            } else if(this.where[1].equals("<=")) {
                if(input.contains(this.where[0]) && (input.getValueByField(this.where[0]) instanceof Integer)) {
                    int val = (Integer)input.getValueByField(this.where[0]);
                    int constant = Integer.parseInt(this.where[2]);
                    if(val <= constant) {
                        // System.out.println("Emitting from Where = " + input.toString());
                        collector.emit(input.getValues());
                    }
                }
            } else if(this.where[1].equals("=")) {
                if(input.contains(this.where[0]) && (input.getValueByField(this.where[0]) instanceof Integer)) {
                    int val = (Integer)input.getValueByField(this.where[0]);
                    int constant = Integer.parseInt(this.where[2]);
                    if(val == constant) {
                        // System.out.println("Emitting from Where = " + input.toString());
                        collector.emit(input.getValues());
                    }
                }
            } else if(this.where[1].equals("<>")) {
                if(input.contains(this.where[0]) && (input.getValueByField(this.where[0]) instanceof Integer)) {
                    int val = (Integer)input.getValueByField(this.where[0]);
                    int constant = Integer.parseInt(this.where[2]);
                    if(val != constant) {
                        // System.out.println("Emitting from Where = " + input.toString());
                        collector.emit(input.getValues());
                    }
                }
            } else if(this.where[1].equals("like")) {
                if(input.contains(this.where[0]) && (input.getValueByField(this.where[0]) instanceof String)) {
                    String val = (String) input.getValueByField(this.where[0]);
                    val = val.toLowerCase();
                    String regex = this.where[2].toLowerCase();
                    regex = regex.replace(".", "\\.").replace("%", ".*").replace("?", ".");
                    if(val.matches(regex)) {
                        // System.out.println("Emitting from Where = " + input.toString());
                        collector.emit(input.getValues());
                    }
                }
            } else if(this.where[1].equals("in")) {
                if(input.contains(this.where[0])) {
                    String val = input.getValueByField(this.where[0]).toString();
                    String temp = this.where[2];
                    String list[] = temp.trim().substring(1, temp.length() - 1).split(",");
                    if(list.length > 0) {
                        for(int i = 0; i < list.length; i++) {
                            if(list[0].charAt(0) == '\'') {
                                list[i] = list[i].substring(1, list[i].length());
                            }
                            if(list[i].equalsIgnoreCase(val)) {
                                // System.out.println("Emitting from Where = " + input.toString());
                                collector.emit(input.getValues());
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            // System.out.println("Emitting due to lack of where = " + input.toString());
            collector.emit(input.getValues());
        }    
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // System.out.println("Declaring output fields");
        // System.out.println("Length = " + fields.size());
        declarer.declare(new Fields(fields));
    }
    
}
