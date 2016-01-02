package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class TweetTopology {
	public static void main (String args[]) throws AlreadyAliveException, InvalidTopologyException{
		TopologyBuilder builder = new TopologyBuilder();
		
        // Enter your credentials here!
		TweetSpout tweetSpout = new TweetSpout(
	            "---",
	            "---",
	            "---",
	            "---"
	        );
	
		builder.setSpout("tweet-spout", tweetSpout, 1);
		
		builder.setBolt("sentiment-bolt", new SentimentBolt(), 10).shuffleGrouping("tweet-spout");
		
		builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("sentiment-bolt");
		
	    Config conf = new Config();
	    conf.setDebug(true);

	    if (args != null && args.length > 0) {

	      // run it in a live cluster
	      conf.setNumWorkers(3);
	      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

	    } else {

	      // run it in a simulated local cluster
	      conf.setMaxTaskParallelism(3);

	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("tweet-sentiment-analysis", conf, builder.createTopology());

	      Utils.sleep(300000);

	      cluster.killTopology("tweet-sentiment-analysis");
	      cluster.shutdown();
	    }
	}
}
