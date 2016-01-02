package storm.starter;

import static storm.starter.Constants.DELIMITER;
import static storm.starter.Constants.LATITUDE;
import static storm.starter.Constants.LONGITUDE;
import static storm.starter.Constants.REDIS_CHANNEL;
import static storm.starter.Constants.SENTIMENT;
import static storm.starter.Constants.TWEET;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

public class ReportBolt extends BaseRichBolt{

	private static final long serialVersionUID = -2136925894368796001L;
	private OutputCollector collector;
	RedisClient client;
	transient RedisConnection<String, String> connection;
	
	@Override
	public void prepare(Map paramMap, TopologyContext paramTopologyContext,
			OutputCollector paramOutputCollector) {
		collector = paramOutputCollector;
		
		RedisClient client = new RedisClient("localhost",6379);
		connection = client.connect();
	}

	@Override
	public void execute(Tuple paramTuple) {
		String tweet = paramTuple.getStringByField(TWEET);
		String sentiment = paramTuple.getStringByField(SENTIMENT);
		String latitude = paramTuple.getStringByField(LATITUDE);
		String longitude = paramTuple.getStringByField(LONGITUDE);
		
		StringBuilder output = new StringBuilder();
		output.append(tweet)
		.append(DELIMITER)
		.append(sentiment)
		.append(DELIMITER)
		.append(latitude)
		.append(DELIMITER)
		.append(longitude);
		
		connection.publish(REDIS_CHANNEL, output.toString());
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer paramOutputFieldsDeclarer) {
		
	}

}
