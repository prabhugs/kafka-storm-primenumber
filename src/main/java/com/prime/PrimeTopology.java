package com.prime;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class PrimeTopology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout", new NumberSpout(),2);
		builder.setBolt("primebolt", new PrimeBolt(),2).shuffleGrouping("spout");
		
		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("primetopology", config, builder.createTopology());
		Utils.sleep(100000); //100 seconds
		cluster.killTopology("primetopology");
		cluster.shutdown();
	}

}
