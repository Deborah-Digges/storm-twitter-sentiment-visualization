package storm.starter;

import static storm.starter.Constants.LATITUDE;
import static storm.starter.Constants.LONGITUDE;
import static storm.starter.Constants.TWEET;
import static storm.starter.Constants.SENTIMENT;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SentimentBolt extends BaseRichBolt{

	private static final long serialVersionUID = -7720605027128347660L;
	OutputCollector outputCollector;
	@Override
	public void prepare(Map paramMap, TopologyContext paramTopologyContext,
			OutputCollector paramOutputCollector) {
		outputCollector = paramOutputCollector;
	}

	@Override
	public void execute(Tuple tuple) {
		String tweetText = tuple.getStringByField(TWEET);
		String latitude = tuple.getStringByField(LATITUDE);
		String longitude = tuple.getStringByField(LONGITUDE);
		Sentiment sentiment;
		try{
			 sentiment = SentimentClient.getInstance().getSentiment(tweetText);
		}
		catch(Exception e){
			sentiment = new Sentiment("neutral", 0.5);
		}
		
		outputCollector.emit(new Values(tweetText, sentiment.getSentimentCategory(), latitude, longitude));
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(TWEET, SENTIMENT, LATITUDE, LONGITUDE));
	}

}
