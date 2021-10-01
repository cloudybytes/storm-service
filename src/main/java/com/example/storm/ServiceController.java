package com.example.storm;

import com.example.storm.bolts.SelectBolt;
import com.example.storm.bolts.TestBolt;
import com.example.storm.bolts.WhereBolt;
import com.example.storm.spouts.MoviesSpout;
import com.example.storm.spouts.RatingSpout;
import com.example.storm.spouts.UsersSpout;
import com.example.storm.spouts.ZipcodesSpout;
import com.example.storm.utils.SpoutUtils;
import com.example.storm.utils.TopologyUtils;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ServiceController {
    @PostMapping(value = "/execute", consumes = "application/json")
    public String execute(@RequestBody ParsedSqlQuery parsedSqlQuery) throws Exception {
        Boolean joinPresent = false;
        String columns[];
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("MoviesSpout", new MoviesSpout());
        builder.setSpout("UsersSpout", new UsersSpout());
        builder.setSpout("RatingSpout", new RatingSpout());
        builder.setSpout("ZipcodesSpout", new ZipcodesSpout());
        if(parsedSqlQuery.getJoin() != null) {
            joinPresent = true;
            String commaColumns = TopologyUtils.addJoinerBoltToTopology(builder, parsedSqlQuery);
            columns = commaColumns.split(",");
        } else {
            columns = SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getFrom_table()).split(",");
        }
        WhereBolt whereBolt = new WhereBolt();
        whereBolt.setOutputFields(columns);
        builder.setBolt("WhereBolt", whereBolt).shuffleGrouping(joinPresent ? "JoinerBolt" : SpoutUtils.getSpoutName(parsedSqlQuery.getFrom_table()));
        SelectBolt selectBolt = new SelectBolt();
        selectBolt.setOutputFields(parsedSqlQuery.getSelect_columns());
        builder.setBolt("SelectBolt", selectBolt).shuffleGrouping("WhereBolt");
        builder.setBolt("TestBolt", new TestBolt()).shuffleGrouping("SelectBolt");
        Config config = new Config();
        config.setDebug(true);
        config.put("InputFolder", "../input/");
        config.put("where[0]", parsedSqlQuery.getWhere()[0]);
        config.put("where[1]", parsedSqlQuery.getWhere()[1]);
        config.put("where[2]", parsedSqlQuery.getWhere()[2]);
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("Topo", config, builder.createTopology());
            // Thread.sleep(1000);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return "Hello";
    }
    
}
