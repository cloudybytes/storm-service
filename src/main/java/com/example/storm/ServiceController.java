package com.example.storm;

import com.example.storm.bolts.GroupByAndAggregateBolt;
import com.example.storm.bolts.HavingBolt;
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
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
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
        Config config = new Config();
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
        System.out.println(parsedSqlQuery.getGroup_by_column() == null ? "" : parsedSqlQuery.getGroup_by_column());
        System.out.println(parsedSqlQuery.getAggr_function() == null ? "" : parsedSqlQuery.getAggr_function());
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
        if(parsedSqlQuery.getGroup_by_column() != null && parsedSqlQuery.getAggr_function() != null) {
            String arr[] = new String[columns.length + 1];
            for(int i = 0; i < columns.length; i++) {
                arr[i] = columns[i];
            }
            arr[columns.length] = parsedSqlQuery.getAggr_function() + "(" + parsedSqlQuery.getHaving_condition()[0] + ")";
            columns = arr;
        }
        builder.setBolt("WhereBolt", whereBolt).shuffleGrouping(joinPresent ? "JoinerBolt" : SpoutUtils.getSpoutName(parsedSqlQuery.getFrom_table()));
        GroupByAndAggregateBolt groupByAndAggregateBolt = new GroupByAndAggregateBolt();
        groupByAndAggregateBolt.setOutputFields(columns);
        BoltDeclarer declarer = builder.setBolt("GroupByBolt", groupByAndAggregateBolt);
        if(parsedSqlQuery.getGroup_by_column() != null) {
            System.out.println("Adding Fields grouping");
            declarer.fieldsGrouping("WhereBolt", new Fields(parsedSqlQuery.getGroup_by_column()));
        } else {
            declarer.shuffleGrouping("WhereBolt");
        }
        HavingBolt havingBolt = new HavingBolt();
        havingBolt.setOutputFields(columns);
        builder.setBolt("HavingBolt", havingBolt).shuffleGrouping("GroupByBolt");
        SelectBolt selectBolt = new SelectBolt();
        selectBolt.setOutputFields(parsedSqlQuery.getSelect_columns());
        builder.setBolt("SelectBolt", selectBolt).shuffleGrouping("HavingBolt");
        builder.setBolt("TestBolt", new TestBolt()).shuffleGrouping("SelectBolt");
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
