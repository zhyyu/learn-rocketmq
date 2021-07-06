package com.zhyyu.learn.rocketmq.controller;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;

@RestController
public class SendMsgController {

    private DefaultMQProducer producer;

    @PostConstruct
    private void init() throws Exception {
        // 实例化消息生产者Producer
        producer = new DefaultMQProducer("please_rename_unique_group_name");
        // 设置NameServer的地址
        producer.setNamesrvAddr("192.168.100.3:9876");
        // 启动Producer实例
        producer.start();
    }

    @RequestMapping("sendMsg")
    public String sendMsg() throws Exception {
        // 创建消息，并指定Topic，Tag和消息体
        Message msg = new Message("TopicTest" /* Topic */,
                "TagA" /* Tag */,
                ("Hello RocketMQ sendMsg " + System.currentTimeMillis()).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
        );
        // 发送消息到一个BrokerRemotingTimeoutException
        SendResult sendResult = producer.send(msg);
        System.out.println(sendResult.toString());
        return sendResult.toString();
    }

    @RequestMapping("sendAsynMsg")
    public String sendAsynMsg() throws Exception {
        // 创建消息，并指定Topic，Tag和消息体
        Message msg = new Message("TopicTest" /* Topic */,
                "TagA" /* Tag */,
                ("Hello RocketMQ sendAsynMsg " + System.currentTimeMillis()).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
        );
        // 发送消息到一个Broker
        producer.send(msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println(sendResult.toString());
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println(throwable);
            }
        });

        return msg.toString();
    }

    @RequestMapping("sendOneway")
    public String sendOneway() throws Exception {
        // 创建消息，并指定Topic，Tag和消息体
        Message msg = new Message("TopicTest" /* Topic */,
                "TagA" /* Tag */,
                ("Hello RocketMQ sendOneway " + System.currentTimeMillis()).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
        );
        // 发送消息到一个Broker
        producer.sendOneway(msg);
        return msg.toString();
    }

}
