
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.google.gson.Gson;


public class TweetGet {
	/**
	 * Main entry of this application.
	 *
	 * @param args
	 */

	
	
	static int count = 0;

    private SimpleQueueServiceSample simpleQS;

	private final Connection connection;
	private Statement statement;

	public TweetGet() throws SQLException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		connection = DriverManager
				.getConnection(
						"jdbc:mysql://tweetdb.cqdvxnqn5gyd.us-east-1.rds.amazonaws.com:3306/TweetDB",
						"tweetdb", "tweetdb123");
		statement = connection.createStatement();
        simpleQS = new SimpleQueueServiceSample();
        System.out.println("Queue created\n");
	}

    public SimpleQueueServiceSample getSimpleQS() throws SQLException {
        return simpleQS;
    }

	public Connection getConnection() {
		return connection;
	}

	public Statement getStatement() throws SQLException {
		return statement;
	}

	public void closeStatement() throws SQLException {
		if (statement != null) {
			statement.close();
		}
	}

	public void closeConnection() throws SQLException {
		if (connection != null) {
			connection.close();
		}
	}

	public static void main(String[] args) throws TwitterException,
			ClassNotFoundException, SQLException {
		// just fill this
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
				.setOAuthConsumerKey("Oehalcqvw0Glhvhrz7EGo1rHD")
				.setOAuthConsumerSecret(
						"jKCUXvfO9gIEDxOS5Ta0jK8eEui2cWWYmveJommehN2Id2Tujh")
				.setOAuthAccessToken(
						"2163511526-sE1OGWLB5CYKQcMaGniplMm2ssvt8bs0Y8gqrGC")
				.setOAuthAccessTokenSecret(
						"vz4QK2pHLCXwYxQ7IIJdhEh5aqXj2GRboaWsYzXCFRyZR");

		final TweetGet tweetGet = new TweetGet();

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build())
				.getInstance();
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				if (status.getGeoLocation() != null) {
					long id = status.getId();
					double latitude = status.getGeoLocation().getLatitude();
					double longitude = status.getGeoLocation().getLongitude();
					String userName = status.getUser().getScreenName();
					String tweetText = status.getText();

					try {
						
						/*String sql1 = "SELECT count(*) FROM TweetDB";
						count = tweetGet.getStatement().executeUpdate(sql1);
						*/
						
						if (count <= 20000) {
							String sql = "INSERT INTO tweet_table values ("
									+ id + ",'" + userName + "', '"
									+ tweetText.replace("'", "''") + "',"
									+ latitude + "," + longitude + ")";
							System.out.println(sql);
							tweetGet.getStatement().executeUpdate(sql);
							count++;


                            System.out.println("Check1\n");

                            Tweet tweet_obj = new Tweet(id,tweetText);
                            Gson gson = new Gson();
                            String json = gson.toJson(tweet_obj);

                            tweetGet.getSimpleQS().pushMessage(json);
                            System.out.println("Check2\n");

						} else {
							String sql = "DELETE FROM TweetDB LIMIT 1";
							tweetGet.getStatement().executeUpdate(sql);
							count--;
							sql = "INSERT INTO tweet_table values (" + id
									+ ",'" + userName + "', '"
									+ tweetText.replace("'", "''") + "',"
									+ latitude + "," + longitude + ")";
							tweetGet.getStatement().executeUpdate(sql);
							count++;
						}
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				// ((status.getHashtagEntities())[0])
			}

			@Override
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
				// System.out.println("Got a status deletion notice id:" +
				// statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				// System.out.println("Got track limitation notice:" +
				// numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				// System.out.println("Got scrub_geo event userId:" + userId +
				// " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				// System.out.println("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};

		// twitterStream.sample();
		twitterStream.addListener(listener);
		String[] hashtags = { "home", "work", "game", "movie", "play" };
		twitterStream.filter(new FilterQuery().track(hashtags));
		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				try {
					System.out.println("Java Rocks...!!!");
					tweetGet.closeStatement();
					tweetGet.closeConnection();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		});
	}

}

class Tweet{
    long tweet_id;
    String tweet_text;

    Tweet(long id, String text){
        tweet_id=id;
        tweet_text=text;
    }
}