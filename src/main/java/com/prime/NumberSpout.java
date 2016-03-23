package com.prime;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class NumberSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	private static int currentNum = 1;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		//System.out.println("----------" + currentNum);
		collector.emit(new Values(new Integer(currentNum ++)));
		
	}
	
	public void ack(Object id){
		//TODO
	}
	
	public void fail(Object id) {
		//TODO
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("number"));
		
	}
}
