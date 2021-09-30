package com.example.storm;

import java.util.concurrent.TimeUnit;

import com.example.storm.bolts.TestBolt;
import com.example.storm.spouts.MoviesSpout;
import com.example.storm.spouts.RatingSpout;
import com.example.storm.spouts.UsersSpout;
import com.example.storm.spouts.ZipcodesSpout;
import com.example.storm.utils.SpoutUtils;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.joda.time.Seconds;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ServiceController {

    @PostMapping(value = "/execute", consumes = "application/json")
    public String execute(@RequestBody ParsedSqlQuery parsedSqlQuery) throws Exception {
        Boolean joinPresent = false;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("MoviesSpout", new MoviesSpout());
        builder.setSpout("UsersSpout", new UsersSpout());
        builder.setSpout("RatingSpout", new RatingSpout());
        builder.setSpout("ZipcodesSpout", new ZipcodesSpout());
        if(parsedSqlQuery.getJoin() != null) {
            joinPresent = true;
            JoinBolt joinBolt;
            if(parsedSqlQuery.getJoin()[0].equals("inner")) {
                joinBolt = new JoinBolt(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]), parsedSqlQuery.getJoin()[2])
                    .join(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[3]), parsedSqlQuery.getJoin()[4], SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]))
                    .select(SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getJoin()[1]) + ", " + SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getJoin()[3]))
                    .withTumblingWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS));
            } else {
                joinBolt = new JoinBolt(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]), parsedSqlQuery.getJoin()[2])
                    .leftJoin(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[3]), parsedSqlQuery.getJoin()[4], SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]))
                    .select(SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getJoin()[1]) + ", " + SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getJoin()[3]))
                    .withTumblingWindow(new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS));
            }
            builder.setBolt("JoinerBolt", joinBolt)
                .fieldsGrouping(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[1]), new Fields(parsedSqlQuery.getJoin()[2]))
                .fieldsGrouping(SpoutUtils.getSpoutName(parsedSqlQuery.getJoin()[3]), new Fields(parsedSqlQuery.getJoin()[4]));
        }
        builder.setBolt("TestBolt", new TestBolt()).shuffleGrouping(joinPresent ? "JoinerBolt" : SpoutUtils.getSpoutName(parsedSqlQuery.getFrom_table()));
        Config config = new Config();
        config.put("InputFolder", "../input/");
        config.setDebug(false);
        config.put("Key", "Value");
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Topo", config, builder.createTopology());
            Thread.sleep(1000);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return "Hello";
    }
    
}
