package com.basic.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * locate com.basic.topology
 * Created by 79875 on 2017/7/10.
 * 提交任务 storm jar  stormAction-1.0-SNAPSHOT.jar com.basic.topology.LogFilterTopology logfilter
 */
public class LogFilterTopology {

    public static class FilterBolt extends BaseRichBolt{
        private OutputCollector outputCollector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector=outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String line=tuple.getString(0);
            if(line.contains("WARN")){
                System.out.println(line);
                outputCollector.emit(tuple,new Values(line));
            }
            outputCollector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("message"));//这个地方写message是给后面的FieldNameBasedTupleToKafkaMapper调用
        }
    }


    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        String zks = "root2:2181,root4:2181,root5:2181";
        String topic= "mylog";
        String zkRoot = "/stormkafka"; // default zookeeper root configuration for stormkafka
        String id = "MyTrack";

        BrokerHosts brokerHosts = new ZkHosts(zks,"/kafka/brokers");
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot,//偏移量offset的目录
                id);//对应一个应用
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.ignoreZkOffsets = true;
        spoutConf.zkServers = Arrays.asList(new String[] {"root2", "root4", "root5"});
        spoutConf.zkPort = 2181;
        //      spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();//从最新消息的开始读取
        spoutConf.startOffsetTime = -2L;;//从最旧的消息开始读取

        Properties props=new Properties();
        props.put("bootstrap.servers","root8:9092,root9:9092,root10:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "-1");
        // 0:表示不管Kafka中是否存储成功
        // 1：表示Kafka中Topic 里面的Leader已经存储成功
        // -1：表示leader和Followers都已经存储成功

        KafkaSpout kafkaSpout=new KafkaSpout(spoutConf);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka_spout",kafkaSpout,3);
        builder.setBolt("filter",new FilterBolt(),9).shuffleGrouping("kafka_spout");
        KafkaBolt kafkaBolt=new KafkaBolt().withTopicSelector(new DefaultTopicSelector("mylog_ERROR"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper())
                .withProducerProperties(props);
        builder.setBolt("kafkabolt",kafkaBolt,9).shuffleGrouping("filter");

        Config config=new Config();

        if(args!=null && args.length <= 0){
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("logfilter",config,builder.createTopology());
            Utils.sleep(50*1000);//50s
            localCluster.killTopology("logfilter");
            localCluster.shutdown();
        }else {
            config.setNumWorkers(3);//每一个Worker 进程默认都会对应一个Acker 线程
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }
    }
}
