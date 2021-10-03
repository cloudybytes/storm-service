package com.example.storm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.example.storm.bolts.GroupByAndAggregateBolt;
import com.example.storm.bolts.HavingBolt;
import com.example.storm.bolts.SelectBolt;
import com.example.storm.bolts.OutputHandlerBolt;
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
import org.apache.storm.tuple.Tuple;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ServiceController {
    @PostMapping(value = "/execute", consumes = "application/json")
    public Map<String, Object> execute(@RequestBody ParsedSqlQuery parsedSqlQuery) throws Exception {
        Boolean joinPresent = false;
        String columns[];
        TopologyBuilder builder = new TopologyBuilder();
        Config config = TopologyUtils.getTopologyConfig(builder, parsedSqlQuery);
        
        // Add spouts to topology
        builder.setSpout("MoviesSpout", new MoviesSpout(), 1);
        builder.setSpout("UsersSpout", new UsersSpout(), 1);
        builder.setSpout("RatingSpout", new RatingSpout(), 1);
        builder.setSpout("ZipcodesSpout", new ZipcodesSpout(), 1);

        // Add JoinBolt to topology if required
        if(parsedSqlQuery.getJoin() != null) {
            joinPresent = true;
            String commaColumns = TopologyUtils.addJoinerBoltToTopology(builder, parsedSqlQuery);
            columns = commaColumns.split(",");
        } else {
            columns = SpoutUtils.getCommaSperatedFields(parsedSqlQuery.getFrom_table()).split(",");
        }
        System.out.println("Join present = " + columns);
        // Add where bolt to topology
        WhereBolt whereBolt = new WhereBolt();
        whereBolt.setOutputFields(columns);
        builder.setBolt("WhereBolt", whereBolt).shuffleGrouping(joinPresent ? "JoinerBolt" : SpoutUtils.getSpoutName(parsedSqlQuery.getFrom_table()));

        // Modify columns for GroupByAndAggregateBolt
        List<String> returnedColumns = whereBolt.getOutputFields();
        columns = new String[returnedColumns.size()];
        columns = returnedColumns.toArray(columns);
        if(parsedSqlQuery.getGroup_by_column() != null && parsedSqlQuery.getAggr_function() != null) {
            String arr[] = new String[columns.length + 1];
            for(int i = 0; i < columns.length; i++) {
                arr[i] = columns[i];
            }
            arr[columns.length] = parsedSqlQuery.getAggr_function() + "(" + parsedSqlQuery.getHaving_condition()[0] + ")";
            columns = arr;
        }

        // Add GroupByAndAggregate Bolt
        GroupByAndAggregateBolt groupByAndAggregateBolt = new GroupByAndAggregateBolt();
        groupByAndAggregateBolt.setOutputFields(columns);
        BoltDeclarer declarer = builder.setBolt("GroupByBolt", groupByAndAggregateBolt, 1);
        declarer.allGrouping("WhereBolt");
        if(parsedSqlQuery.getGroup_by_column() != null) {
            System.out.println("Adding Fields grouping");
            declarer.fieldsGrouping("WhereBolt", new Fields(parsedSqlQuery.getGroup_by_column()));
        } else {
            declarer.shuffleGrouping("WhereBolt");
        }

        // Add Having Bolt
        HavingBolt havingBolt = new HavingBolt();
        havingBolt.setOutputFields(columns);
        builder.setBolt("HavingBolt", havingBolt).shuffleGrouping("GroupByBolt");

        // Add Select Bolt
        SelectBolt selectBolt = new SelectBolt();
        selectBolt.setOutputFields(parsedSqlQuery.getSelect_columns());
        builder.setBolt("SelectBolt", selectBolt).shuffleGrouping("HavingBolt");

        OutputHandlerBolt outputHandlerBolt = new OutputHandlerBolt();
        HashMap<Object, Tuple> outputMap = new HashMap<Object, Tuple>();
        outputHandlerBolt.setOutputMap(outputMap);
        builder.setBolt("TestBolt", outputHandlerBolt).shuffleGrouping("SelectBolt");
        LocalCluster cluster = new LocalCluster();
        try {
            TopologyUtils.startTime.set(System.currentTimeMillis());
            cluster.submitTopology("Topo", config, builder.createTopology());
            Thread.sleep(25000);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        Map<String, Object> response = new HashMap<String, Object>();
        response.put("time", TopologyUtils.endTime.get() - TopologyUtils.startTime.get() + " miliseconds");
        System.out.println("Size of map = " + outputMap.size());
        response.put("result", "/output" + TopologyUtils.outputFileNumber + ".csv");
        return response;
    }
}
