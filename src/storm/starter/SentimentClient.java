package storm.starter;

import static storm.starter.Constants.CONNECTION_REQUEST_TIMEOUT;
import static storm.starter.Constants.CONNECT_TIMEOUT;
import static storm.starter.Constants.POST_URL;
import static storm.starter.Constants.SOCKET_TIMEOUT;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import twitter4j.JSONException;
import twitter4j.JSONObject;

public class SentimentClient {

	private static SentimentClient instance = null;
	CloseableHttpClient httpClient;
	
	private SentimentClient() {
PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
		
		RequestConfig requestConfig = RequestConfig.custom()
				  .setConnectTimeout(CONNECT_TIMEOUT)
				  .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT)
				  .setSocketTimeout(SOCKET_TIMEOUT).build();
				
		this.httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).setConnectionManager(connManager).build();
	}
	
	public static SentimentClient getInstance() {
		if(instance == null) {
			instance = new SentimentClient();
		}
		return instance;
	}
	
	private Sentiment getSentimentFromResponse(JSONObject response) throws JSONException {
		String sentimentCategory = response.getString("label");
		JSONObject probability = (JSONObject) response.get("probability");
		
		Sentiment sentiment = new Sentiment(sentimentCategory, Double.parseDouble(probability.getString(sentimentCategory)));
		
		return sentiment;
	}
	
	public Sentiment getSentiment(String tweet) throws ClientProtocolException, IOException, JSONException {
		HttpPost postRequest = new HttpPost(POST_URL);
		ArrayList<NameValuePair> postParameters = new ArrayList<NameValuePair>();;
		postParameters.add(new BasicNameValuePair("text", tweet));
		postRequest.setEntity(new UrlEncodedFormEntity(postParameters));
		
		HttpResponse response = httpClient.execute(postRequest);
		
		String stringResponse = EntityUtils.toString(response.getEntity());
		JSONObject jsonResponse = new JSONObject(stringResponse);
		
		return getSentimentFromResponse(jsonResponse);
	}
}
