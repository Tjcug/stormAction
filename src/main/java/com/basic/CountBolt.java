package com.basic;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * locate com.basic
 * Created by 79875 on 2017/7/5.
 */
public class CountBolt extends BaseRichBolt{
    private Map<String,Long> counts=new HashMap<>();
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String word=tuple.getStringByField("word");
            Long counts=this.counts.get(word);
            if(counts==null){
                counts=0L;
            }
            counts++;
            this.counts.put(word,counts);
            System.out.println(word+" : "+counts);
            outputCollector.ack(tuple);
        } catch (Exception e) {
            outputCollector.fail(tuple);
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
