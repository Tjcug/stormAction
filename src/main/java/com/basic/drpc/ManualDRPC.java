package com.basic.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * locate com.basic.drpc
 * Created by 79875 on 2017/7/10.
 * Storm DRPC
 * 提交任务 storm jar  stormAction-1.0-SNAPSHOT.jar com.basic.drpc.ManualDRPC manualdrpc
 */
public class ManualDRPC {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder= new TopologyBuilder();

        DRPCSpout drpcSpoutRPC=new DRPCSpout("exclaimation");
        builder.setSpout("drpcspout",drpcSpoutRPC);
        builder.setBolt("exclaim",new ExclamationBolt(),3).shuffleGrouping("drpcspout");
        builder.setBolt("return",new ReturnResults(),3).shuffleGrouping("exclaim");
        Config config=new Config();
        config.setMessageTimeoutSecs(30);

        if(args!=null && args.length <= 0){
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("exclaim",config,builder.createTopology());
            Utils.sleep(50*1000);//50s
            localCluster.killTopology("exclaim");
            localCluster.shutdown();
        }else {
            config.setNumWorkers(3);//每一个Worker 进程默认都会对应一个Acker 线程
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }
    }
}
