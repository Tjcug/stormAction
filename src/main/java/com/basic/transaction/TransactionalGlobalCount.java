package com.basic.transaction;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.MemoryTransactionalSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.transactional.TransactionalTopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * locate com.basic.transaction
 * Created by 79875 on 2017/7/19.
 */
public class TransactionalGlobalCount {
    public static final int PARTITION_TAKE_PER_BATCH = 3;
    public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {{
        put(0, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("chicken"));
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("apple"));
        }});
        put(1, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("apple"));
            add(new Values("banana"));
        }});
        put(2, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("dog"));
            add(new Values("dog"));
            add(new Values("dog"));
        }});
    }};

    public static class Value {
        int count = 0;
        BigInteger txid;
    }

    public static Map<String, Value> DATABASE = new HashMap<String, Value>();
    public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";

    /**
     * 处理阶段的Bolt 用于统计局部的Tuple数量
     * BatchCount 继承 BaseBatchBolt 表明其对Batch的支持，主要反映在finishBatch函数。
     * 而普通Bolt的不同在于，只有在finishBatch的时候才会去emit结果，而不是每次execute出结果
     */
    public static class BatchCount extends BaseBatchBolt {
        private Object _id;//a TransactionAttempt object,并且从output定义来看，所有emit参数第一个必须是(id TransactionAttempt)
        private BatchOutputCollector _collector;

        private int _count = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            _count++;
        }

        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "count"));
        }
    }

    /**
     * commit阶段的Bolt 将统计的结果累加到全局数据库中
     * UpdateGlobalCount继承BaseTransactionalBolt，所以此处的prepare参数之间是TransactionAttempt而不是Object id
     * 并且比较重要的是实现了ICommitter接口 表明这个bolt是一个Committer，意味着这个Bolt的finishBatch函数需要在commit阶段被调用
     * 另外一种把bolt标记为Committer实在topology builder的时候使用setCommitterBolt代替setBolt
     */
    public static class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {
        private TransactionAttempt _attempt;
        private BatchOutputCollector _collector;

        private int _sum = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt attempt) {
            _collector = collector;
            _attempt = attempt;
        }

        @Override
        public void execute(Tuple tuple) {
            _sum += tuple.getInteger(1);
        }

        //storm 会保证Committer 里面的finishBatch会被顺序执行，并且在finishBatch里面需要check_transcation_id 确保只有新的transaction的结果才会被更新到数据库中
        @Override
        public void finishBatch() {
            Value val = DATABASE.get(GLOBAL_COUNT_KEY);
            Value newval;
            if (val == null || !val.txid.equals(_attempt.getTransactionId())) {
                newval = new Value();
                newval.txid = _attempt.getTransactionId();
                if (val == null) {
                    newval.count = _sum;
                }
                else {
                    newval.count = _sum + val.count;
                }
                DATABASE.put(GLOBAL_COUNT_KEY, newval);
            }
            else {
                newval = val;
            }
            _collector.emit(new Values(_attempt, newval.count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "sum"));
        }
    }

    public static void main(String[] args) throws Exception {
        MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields("word"), PARTITION_TAKE_PER_BATCH);
        //DATA 读取内存变量里面的数据
        //new Fields("word") 指定需要发送字段
        //PARTITION_TAKE_PER_BATCH 指定每个Batch最大的tuple数量
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 3);
        builder.setBolt("partial-count", new BatchCount(), 5).noneGrouping("spout");
        builder.setBolt("sum", new UpdateGlobalCount()).globalGrouping("partial-count");

        LocalCluster cluster = new LocalCluster();

        Config config = new Config();
        config.setDebug(true);
        config.setMaxSpoutPending(3);//同时活跃的batch数量，你必须设置同时处理的batch数量。你可以通过”topology.max.spout.pending” 来指定， 如果你不指定，默认是1。

        cluster.submitTopology("global-count-topology", config, builder.buildTopology());

        Thread.sleep(3000);
        cluster.shutdown();
    }
}
