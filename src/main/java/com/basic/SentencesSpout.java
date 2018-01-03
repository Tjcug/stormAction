package com.basic;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * locate com.basic
 * Created by 79875 on 2017/7/5.
 */
public class SentencesSpout extends BaseRichSpout{

    private  SpoutOutputCollector outputCollector;

    private ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentences"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector=spoutOutputCollector;
        this.pending=new ConcurrentHashMap<UUID, Values>();
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Values value = new Values(new Values("i love xxxx"));
        UUID uuid=UUID.randomUUID();
        pending.put(uuid,value);
        outputCollector.emit(value,uuid);
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("ack: "+msgId);
        pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.err.println("fail"+msgId);
        outputCollector.emit(pending.get(msgId),msgId);
    }
}
