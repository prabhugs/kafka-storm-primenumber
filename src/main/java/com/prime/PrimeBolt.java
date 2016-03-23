package com.prime;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PrimeBolt extends BaseRichBolt {
	
	private OutputCollector collector;

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		int number = tuple.getInteger(0);
		//System.out.println("#################" + number);
		
		if (isPrime(number)){
			System.out.println(number);
		}
		collector.ack(tuple);
		
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("number"));
	}
	
	private boolean isPrime(int n) {
		
		if (n == 1 || n == 2 || n == 3) {
			return true;
		}
		
		if (n %2 == 0) {
			return false;
		}
		
		for (int i =3; i*i<=n; i+=2) {
			if (n%i == 0) {
				return false;
			}
		}
		
		return true;
	}

}
