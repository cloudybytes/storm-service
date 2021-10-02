package com.example.storm.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class HavingBolt extends BaseBasicBolt {
    private String having[] = new String[3];
    private Boolean containsHaving = false;
    private List<String> fields = new ArrayList<String>();
    
    public void setOutputFields(String[] inputFields) {
        for(int i = 0; i < inputFields.length; i++) {
            if(inputFields[i].indexOf(':') == -1) {
                fields.add(inputFields[i]);
            } else {
                fields.add(inputFields[i].split(":")[1].toString());
            }
        }
    }

    @Override
    public void prepare(Map<String,Object> topoConf, TopologyContext context) {
        if(topoConf.containsKey("having[0]")) {
            containsHaving = true;
            having[0] = (String) topoConf.get("having[0]");
            having[1] = (String) topoConf.get("having[1]");
            having[2] = (String) topoConf.get("having[2]");
        } else {
            having = null;
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if(this.containsHaving) {
            if(this.having[1].equals(">")) {
                if(input.contains(this.having[0])) {
                    if((input.getValueByField(this.having[0]) instanceof Integer)) {
                        int val = (Integer)input.getValueByField(this.having[0]);
                        int constant = Integer.parseInt(this.having[2]);
                        if(val > constant) {
                            // System.out.println("Emitting = " + input.toString());
                            collector.emit(input.getValues());
                        }
                    } else if((input.getValueByField(this.having[0]) instanceof Double)) {
                        double val = (Double) input.getValueByField(this.having[0]);
                        double constant = Double.parseDouble(this.having[2]);
                        if(val > constant) {
                            // System.out.println("Emitting = " + input.toString());
                            collector.emit(input.getValues());
                        }
                    }
                }
            } else if(this.having[1].equals(">=")) {
                if(input.contains(this.having[0])) {
                    if((input.getValueByField(this.having[0]) instanceof Integer)) {
                        int val = (Integer)input.getValueByField(this.having[0]);
                        int constant = Integer.parseInt(this.having[2]);
                        if(val >= constant) {
                            // System.out.println("Emitting = " + input.toString());
                            collector.emit(input.getValues());
                        }
                    } else if((input.getValueByField(this.having[0]) instanceof Double)) {
                        double val = (Double) input.getValueByField(this.having[0]);
                        double constant = Double.parseDouble(this.having[2]);
                        if(val >= constant) {
                            // System.out.println("Emitting = " + input.toString());
                            collector.emit(input.getValues());
                        }
                    }
                }
            } else if(this.having[1].equals("<")) {
                if(input.contains(this.having[0])) {
                    if((input.getValueByField(this.having[0]) instanceof Integer)) {
                        int val = (Integer)input.getValueByField(this.having[0]);
                        int constant = Integer.parseInt(this.having[2]);
                        if(val < constant) {
                            // System.out.println("Emitting = " + input.toString());
                            collector.emit(input.getValues());
                        }
                    } else if((input.getValueByField(this.having[0]) instanceof Double)) {
                        double val = (Double) input.getValueByField(this.having[0]);
                        double constant = Double.parseDouble(this.having[2]);
                        if(val < constant) {
                            // System.out.println("Emitting = " + input.toString());
                            collector.emit(input.getValues());
                        }
                    }
                }
            } else if(this.having[1].equals("<=")) {
                if(input.contains(this.having[0])) {
                    if((input.getValueByField(this.having[0]) instanceof Integer)) {
                        int val = (Integer)input.getValueByField(this.having[0]);
                        int constant = Integer.parseInt(this.having[2]);
                        if(val <= constant) {
                            // System.out.println("Emitting = " + input.toString());
                            collector.emit(input.getValues());
                        }
                    } else if((input.getValueByField(this.having[0]) instanceof Double)) {
                        double val = (Double) input.getValueByField(this.having[0]);
                        double constant = Double.parseDouble(this.having[2]);
                        if(val <= constant) {
                            // System.out.println("Emitting = " + input.toString());
                            collector.emit(input.getValues());
                        }
                    }
                }
            } else if(this.having[1].equals("=")) {
                if(input.contains(this.having[0])) {
                    if((input.getValueByField(this.having[0]) instanceof Integer)) {
                        int val = (Integer)input.getValueByField(this.having[0]);
                        int constant = Integer.parseInt(this.having[2]);
                        if(val == constant) {
                            // System.out.println("Emitting = " + input.toString());
                            collector.emit(input.getValues());
                        }
                    } else if((input.getValueByField(this.having[0]) instanceof Double)) {
                        double val = (Double) input.getValueByField(this.having[0]);
                        double constant = Double.parseDouble(this.having[2]);
                        if(val == constant) {
                            // System.out.println("Emitting = " + input.toString());
                            collector.emit(input.getValues());
                        }
                    }
                }
            } else if(this.having[1].equals("<>")) {
                if(input.contains(this.having[0])) {
                    if((input.getValueByField(this.having[0]) instanceof Integer)) {
                        int val = (Integer)input.getValueByField(this.having[0]);
                        int constant = Integer.parseInt(this.having[2]);
                        if(val != constant) {
                            // System.out.println("Emitting = " + input.toString());
                            collector.emit(input.getValues());
                        }
                    } else if((input.getValueByField(this.having[0]) instanceof Double)) {
                        double val = (Double) input.getValueByField(this.having[0]);
                        double constant = Double.parseDouble(this.having[2]);
                        if(val != constant) {
                            // System.out.println("Emitting = " + input.toString());
                            collector.emit(input.getValues());
                        }
                    }
                }
            } else if(this.having[1].equals("like")) {
                if(input.contains(this.having[0]) && (input.getValueByField(this.having[0]) instanceof String)) {
                    String val = (String) input.getValueByField(this.having[0]);
                    val = val.toLowerCase();
                    String regex = this.having[2].toLowerCase();
                    regex = regex.replace(".", "\\.").replace("%", ".*").replace("?", ".");
                    if(val.matches(regex)) {
                        // System.out.println("Emitting = " + input.toString());
                        collector.emit(input.getValues());
                    }
                }
            } else if(this.having[1].equals("in")) {
                if(input.contains(this.having[0])) {
                    String val = input.getValueByField(this.having[0]).toString();
                    String temp = this.having[2];
                    String list[] = temp.trim().substring(1, temp.length() - 1).split(",");
                    if(list.length > 0) {
                        for(int i = 0; i < list.length; i++) {
                            if(list[0].charAt(0) == '\'') {
                                list[i] = list[i].substring(1, list[i].length());
                            }
                            if(list[i].equalsIgnoreCase(val)) {
                                // System.out.println("Emitting = " + input.toString());
                                collector.emit(input.getValues());
                                break;
                            }
                        }
                    }
                }
            }
        } else {
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
