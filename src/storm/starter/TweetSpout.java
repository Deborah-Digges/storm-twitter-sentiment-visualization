package storm.starter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.GeoLocation;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import static storm.starter.Constants.*;

public class TweetSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	
	SpoutOutputCollector collector;
	
	// Data structure onto which the StatusListener puts new tweets
	private LinkedBlockingQueue<Map<String, String>> queue;

	// Twitter API authentication credentials
	String custkey, custsecret;              
	String accesstoken, accesssecret;
	
	public TweetSpout(
			String key,
			String secret,
			String token,
			String tokenSecret
			) {
		this.custkey = key;
		this.custsecret = secret;
		this.accesstoken = token;
		this.accesssecret = tokenSecret;
	}

	class TweetListener implements twitter4j.StatusListener {

		@Override
		public void onException(Exception arg0) {}
		
		@Override
		public void onDeletionNotice(StatusDeletionNotice arg0) {}

		@Override
		public void onScrubGeo(long arg0, long arg1) {}

		@Override
		public void onStallWarning(StallWarning arg0) {}

		@Override
		public void onStatus(Status status) {
			if(status.getGeoLocation() != null && ENGLISH.equalsIgnoreCase(status.getLang())){
				GeoLocation location = status.getGeoLocation();
				Double latitude = location.getLatitude();
				Double longitude = location.getLongitude();
				String tweetText = status.getText();
				
				Map<String, String> tweetFields = new HashMap<String, String>();
				tweetFields.put(TWEET, tweetText);
				tweetFields.put(LATITUDE, latitude.toString());
				tweetFields.put(LONGITUDE, longitude.toString());
				
				queue.offer(tweetFields);
			}
		}

		@Override
		public void onTrackLimitationNotice(int arg0) {}
		
	}
	
	@Override
	public void nextTuple() {
		Map<String, String> tweetFields = queue.poll();
		
		if(tweetFields == null) {
			return;
		}
		String tweetText = tweetFields.get(TWEET);
		String latitude = tweetFields.get(LATITUDE);
		String longitude = tweetFields.get(LONGITUDE);
		
		collector.emit(new Values(tweetText, latitude, longitude));
		
	}

	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Map<String, String>>(1000);
		
		this.collector = collector;
		
		ConfigurationBuilder config = new ConfigurationBuilder()
		.setOAuthConsumerKey(custkey)
		.setOAuthConsumerSecret(custsecret)
		.setOAuthAccessToken(accesstoken)
		.setOAuthAccessTokenSecret(accesssecret);
		
		TwitterStreamFactory factory = new TwitterStreamFactory(config.build());
		TwitterStream stream = factory.getInstance();
		stream.addListener(new TweetListener());
		
		stream.sample();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(TWEET, LATITUDE, LONGITUDE));
	}

}
