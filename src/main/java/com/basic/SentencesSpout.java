package com.basic;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * locate com.basic
 * Created by 79875 on 2017/7/5.
 */
public class SentencesSpout extends BaseRichSpout{

    private  SpoutOutputCollector outputCollector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentences"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector=spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        outputCollector.emit(new Values("i love xxxx"),"tanjie");
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("ack: "+msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.err.println("fail"+msgId);
        outputCollector.emit(new Values("i love xxxx"),"fail tanjie");
    }
}
