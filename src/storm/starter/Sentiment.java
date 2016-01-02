package storm.starter;

public class Sentiment {
	private String sentimentCategory;
	private double sentimentProbability;
	
	public String getSentimentCategory() {
		return sentimentCategory;
	}

	public void setSentimentCategory(String sentimentCategory) {
		this.sentimentCategory = sentimentCategory;
	}

	public double getSentimentProbability() {
		return sentimentProbability;
	}

	public void setSentimentProbability(int sentimentProbability) {
		this.sentimentProbability = sentimentProbability;
	}

	public Sentiment(String sentimentCategory, double sentimentProbability) {
		super();
		this.sentimentCategory = sentimentCategory;
		this.sentimentProbability = sentimentProbability;
	}
}
