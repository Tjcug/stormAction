package com.basic.drpc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * locate com.basic.drpc
 * Created by 79875 on 2017/7/10.
 */
public class ExclamationBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String args=tuple.getString(0);
        Object returninfo=tuple.getValue(1);

        String[] words=args.split(" ");
        int result= Integer.parseInt(words[0])+Integer.valueOf(words[1]);
        outputCollector.emit(tuple,new Values(""+result,returninfo));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("result","return-info"));
    }
}
