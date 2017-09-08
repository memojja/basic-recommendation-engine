package main;

//import model.Rating;
import model.User;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;

import com.jasongoodwin.monads.Try;

import model.Movie;
import org.apache.spark.mllib.recommendation.Rating;
import java.io.Serializable;

/**
 * Hello world!
 *
 */


public class App implements Serializable
{
	public static final String moviesPath = "C:\\Users\\memojja\\Desktop\\recomendation-engine\\data\\movies.dat";
	public static final String usersPath = "C:\\Users\\memojja\\Desktop\\recomendation-engine\\data\\users.dat";
	public static final String ratingsPath = "C:\\Users\\memojja\\Desktop\\recomendation-engine\\data\\ratings.dat";
 
	
    public static void main( String[] args )
    {
    	JavaSparkContext jsc = new JavaSparkContext("local","Remondation Engine");
    	SQLContext sqlContext =new SQLContext(jsc);
    	
    	JavaRDD<Movie> movieRDD =  jsc.textFile(moviesPath)
				.map(line -> {
					String[] movieArr = line.split("::");
					Integer movieId =  Integer.parseInt((String) Try.ofFailable(() -> movieArr[0]).orElse("-1"));
					return new Movie(movieId, movieArr[1], movieArr[2]);
				})
				.cache();
		 
		 
		System.out.println(movieRDD.first());



		JavaRDD<User> userRDD = jsc.textFile(usersPath)
				.map(line ->{
					String[] userArr = line.split("::");
					Integer userId = Integer.parseInt(Try.ofFailable(() -> userArr[0]).orElse("-1"));
					Integer age = Integer.parseInt(Try.ofFailable(() -> userArr[2]).orElse("-1"));
					Integer occupation = Integer.parseInt(Try.ofFailable(() -> userArr[3]).orElse("-1"));
					return new User(userId, userArr[1], age, occupation, userArr[4]);
				})
				.cache();


		JavaRDD<Rating> ratingRDD = jsc.textFile(ratingsPath)
				.map(
					line ->{
						String[] ratingArr = line.split("::");
						Integer userId = Integer.parseInt(Try.ofFailable(() -> ratingArr[0]).orElse("-1"));
						Integer movieId = Integer.parseInt(Try.ofFailable(() -> ratingArr[1]).orElse("-1"));
						Double rating = Double.parseDouble(Try.ofFailable(() -> ratingArr[2]).orElse("-1"));
						return new Rating(userId, movieId, rating);
			}).cache();

		System.out.println("Total number of user  : " + userRDD.count());
		System.out.println("Total number of rating  : " + ratingRDD.count());





		JavaPairRDD<Integer, Iterable<Rating>> ratingsGroupByProduct = ratingRDD.groupBy(
			rating -> rating.product()
		);

		System.out.println("Total number of movies rated   : " + ratingsGroupByProduct.count());


		JavaPairRDD<Integer, Iterable<Rating>> ratingsGroupByUser = ratingRDD.groupBy(
				rating -> rating.user()
		);

		System.out.println("Total number of users who rated movies   : " + ratingsGroupByUser.count());
    }
}
