package com.basic.grouping;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

/**
 * locate com.basic.grouping
 * Created by 79875 on 2017/7/7.
 */
public class MySpout extends BaseRichSpout{

    private SpoutOutputCollector outputCollector;
    private Logger logger= LoggerFactory.getLogger(MySpout.class);
    private BufferedReader bufferedReader;

    private String str=null;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            bufferedReader=new BufferedReader(new InputStreamReader(new FileInputStream("data/track.log")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.outputCollector=spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        try {
            while ((str=bufferedReader.readLine())!=null)
                outputCollector.emit(new Values(str,str.split("\t")[1]));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("log","session_id"));
    }

    @Override
    public void close() {
        try {
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
