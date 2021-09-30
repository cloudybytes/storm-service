package com.example.storm.utils;

import java.util.concurrent.TimeUnit;

import com.example.storm.ParsedSqlQuery;

import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

public class TopologyUtils {
    public static String addJoinerBoltToTopology(TopologyBuilder builder, ParsedSqlQuery parsedSqlQuery) {
        JoinBolt joinBolt;
        String columns;
        if(parsedSqlQuery.getJoin()[0].equals("inner")) {
            columns = SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getJoin()[1]) + ", " + SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getJoin()[3]);
            joinBolt = new JoinBolt(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]), parsedSqlQuery.getJoin()[2])
                .join(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[3]), parsedSqlQuery.getJoin()[4], SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]))
                .select(columns)
                .withTumblingWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS));
        } else if(parsedSqlQuery.getJoin()[0].equals("natural")) {
            columns = SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getJoin()[1]) + ", " + SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getJoin()[3]);
            joinBolt = new JoinBolt(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]), parsedSqlQuery.getJoin()[2])
                .join(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[3]), parsedSqlQuery.getJoin()[4], SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]))
                .select(columns)
                .withTumblingWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS));
        } else {
            columns = SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getJoin()[1]) + ", " + SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getJoin()[3]);
            joinBolt = new JoinBolt(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]), parsedSqlQuery.getJoin()[2])
                .leftJoin(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[3]), parsedSqlQuery.getJoin()[4], SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]))
                .select(columns)
                .withTumblingWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS));
        }
        builder.setBolt("JoinerBolt", joinBolt)
            .fieldsGrouping(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]), new Fields(parsedSqlQuery.getJoin()[2]))
            .fieldsGrouping(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[3]), new Fields(parsedSqlQuery.getJoin()[4]));
        return columns;
    }
}
