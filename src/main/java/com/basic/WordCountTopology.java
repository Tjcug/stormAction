package com.basic;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * locate com.basic
 * Created by 79875 on 2017/7/5.
 *
 */
public class WordCountTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new SentencesSpout(),1);
        builder.setBolt("splitbolt",new SplitBolt(),2).shuffleGrouping("spout");
        builder.setBolt("countbolt",new CountBolt(),8).fieldsGrouping("splitbolt",new Fields("word"));

        Config config=new Config();
        config.setMessageTimeoutSecs(30);

        if(args!=null && args.length <= 0){
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("wc",config,builder.createTopology());
            Utils.sleep(50*1000);//50s
            localCluster.killTopology("wc");
            localCluster.shutdown();
        }else {
            config.setNumWorkers(3);//每一个Worker 进程默认都会对应一个Acker 线程
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }

    }
}
