package com.basic.drpc;

import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * locate com.basic.drpc
 * Created by 79875 on 2017/7/10.
 */
public class MyDRPClient {
    public static void main(String[] args) throws TTransportException {
        Map config = Utils.readDefaultConfig();
        DRPCClient drpcClient=new DRPCClient(config,"root2",3772);
        try {
            String exclaimation = drpcClient.execute("exclaimation", "3 100");
            System.out.println(exclaimation);
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
