package storm.starter;

public class Constants {
	public static String TWEET = "tweet";
	public static String LATITUDE = "latitude";
	public static String LONGITUDE = "longitude";
	public static String SENTIMENT = "sentiment";
	public static String ENGLISH = "EN";
	public static String REDIS_CHANNEL = "tweetSentiment";
	public static String DELIMITER = "|";
	
	public static final int CONNECTION_REQUEST_TIMEOUT = 1000;
	public static final int CONNECT_TIMEOUT = 1000;
	public static final int SOCKET_TIMEOUT = 5000;
	
	public static final String POST_URL = "http://text-processing.com/api/sentiment/";
}
