package com.basic.grouping;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * locate com.basic.grouping
 * Created by 79875 on 2017/7/7.
 */
public class MyBolt extends BaseRichBolt{
    private OutputCollector outputCollector;
    private Logger logger= LoggerFactory.getLogger(MyBolt.class);

    private long num=0L;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String valueString=tuple.getStringByField("log");

        if(valueString!=null){
            num++;
            logger.info("{} {}--id={} lines :{} session_id: {}",tuple.getSourceComponent(),Thread.currentThread().getName(),Thread.currentThread().getId(),num,valueString);
        }
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
