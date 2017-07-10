package com.basic.grouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * locate com.basic.grouping
 * Created by 79875 on 2017/7/7.
 */
public class Main {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("spout",new MySpout(),1);
        //shuffleGrouping就是随机往下游发送数据，不自觉的做到了负载均衡
        builder.setBolt("bolt",new MyBolt(),2).shuffleGrouping("spout");
        //fieldsGrouping 按照字段进行分组
        //builder.setBolt("bolt",new MyBolt(),2).fieldsGrouping("spout",new Fields("session_id"));
        //allGrouping 广播
        //builder.setBolt("bolt",new MyBolt()).allGrouping("spout");
        //直往一个里面发，往taskid的那个最小的里面分发
        //uilder.setBolt("bolt",new MyBolt()).globalGrouping("spout");
        //等于shuffleGrouping
        //builder.setBolt("bolt",new MyBolt(),2).noneGrouping("spout");
        Config config=new Config();

        if(args!=null && args.length <= 0){
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("main",config,builder.createTopology());
//            Utils.sleep(50*1000);//50s
//            localCluster.killTopology("main");
//            localCluster.shutdown();
        }else {
            config.setNumWorkers(3);//每一个Worker 进程默认都会对应一个Acker 线程
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }
    }
}
