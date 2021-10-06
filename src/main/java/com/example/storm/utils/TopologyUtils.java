package com.example.storm.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.example.storm.ParsedSqlQuery;

import org.apache.storm.Config;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

public class TopologyUtils {
    public static AtomicLong startTime = new AtomicLong(0);
    public static AtomicLong endTime = new AtomicLong(0);
    public static String fileName = "";

    public static String addJoinerBoltToTopology(TopologyBuilder builder, ParsedSqlQuery parsedSqlQuery) {
        JoinBolt joinBolt;
        String columns = SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getJoin()[1]) + "," + SpoutUtils.getCommaSperatedFieldsTruncated(parsedSqlQuery.getJoin()[1], parsedSqlQuery.getJoin()[3]);
        // System.out.println("Join Columns = " + columns);
        if(parsedSqlQuery.getJoin()[0].equals("inner")) {
            joinBolt = new JoinBolt(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]), parsedSqlQuery.getJoin()[2])
                .join(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[3]), parsedSqlQuery.getJoin()[4], SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]))
                .select(columns)
                .withTumblingWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS));
        } else if(parsedSqlQuery.getJoin()[0].equals("natural")) {
            joinBolt = new JoinBolt(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]), parsedSqlQuery.getJoin()[2])
                .join(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[3]), parsedSqlQuery.getJoin()[4], SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]))
                .select(columns)
                .withTumblingWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS));
        } else {
            joinBolt = new JoinBolt(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]), parsedSqlQuery.getJoin()[2])
                .leftJoin(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[3]), parsedSqlQuery.getJoin()[4], SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]))
                .select(columns)
                .withTumblingWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS));
        }
        builder.setBolt("JoinerBolt", joinBolt, 8)
            .fieldsGrouping(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]), new Fields(parsedSqlQuery.getJoin()[2]))
            .fieldsGrouping(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[3]), new Fields(parsedSqlQuery.getJoin()[4]));
        return columns;
    }

    public static Config getTopologyConfig(TopologyBuilder builder, ParsedSqlQuery parsedSqlQuery) {
        Config config = new Config();
        config.setDebug(false);
        config.put("InputFolder", "../input/");
        if(parsedSqlQuery.getWhere() != null) {
            config.put("where[0]", parsedSqlQuery.getWhere()[0]);
            config.put("where[1]", parsedSqlQuery.getWhere()[1]);
            config.put("where[2]", parsedSqlQuery.getWhere()[2]);
        }
        if(parsedSqlQuery.getGroup_by_column() != null) {
            config.put("groupBy", parsedSqlQuery.getGroup_by_column());
        }
        if(parsedSqlQuery.getAggr_function() != null && parsedSqlQuery.getHaving_condition() != null) {
            config.put("aggregate", parsedSqlQuery.getAggr_function());
            config.put("aggregateColumn", parsedSqlQuery.getHaving_condition()[0]);
        }
        if(parsedSqlQuery.getHaving_condition() != null) {
            config.put("having[0]", parsedSqlQuery.getAggr_function() + "(" + parsedSqlQuery.getHaving_condition()[0] + ")");
            config.put("having[1]", parsedSqlQuery.getHaving_condition()[1]);
            config.put("having[2]", parsedSqlQuery.getHaving_condition()[2]);
        }
        System.out.println("Topology Config = " + config.toString());
        return config;
    }
}
