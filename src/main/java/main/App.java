package main;

//import model.Rating;
import model.User;
import model.UserProductTuple;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

import com.jasongoodwin.monads.Try;

import model.Movie;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Hello world!
 *
 */


public class App implements Serializable
{

	public static final String moviesPath = "data\\movies.dat";
	public static final String usersPath = "data\\users.dat";
	public static final String ratingsPath = "data\\ratings.dat";


	
    public static void main( String[] args )
    {

	//Context

    	JavaSparkContext jsc = new JavaSparkContext("local","Remondation Engine");
    	jsc.setLogLevel("OFF");
    	SQLContext sqlContext =new SQLContext(jsc);

	//RDD
		JavaRDD<Movie> movieRDD = getMovieJavaRDD(jsc);
		JavaRDD<User> userRDD = getUserJavaRDD(jsc);
		JavaRDD<Rating> ratingRDD = getRatingJavaRDD(jsc);


	//JavaPairRDD for grouping by value
		JavaPairRDD<Integer, Iterable<Rating>> ratingsGroupByProduct = ratingRDD.groupBy(rating -> rating.product());
		System.out.println("Total number of movies rated   : " + ratingsGroupByProduct.count());

		JavaPairRDD<Integer, Iterable<Rating>> ratingsGroupByUser = ratingRDD.groupBy(rating -> rating.user());
		System.out.println("Total number of users who rated movies   : " + ratingsGroupByUser.count());



	//StructType data structure
		StructType structType = new StructType(new StructField[]{
				DataTypes.createStructField("user", DataTypes.IntegerType, true),
				DataTypes.createStructField("product", DataTypes.IntegerType, true),
				DataTypes.createStructField("rating", DataTypes.DoubleType, true)})
				;


		JavaRDD<Row> ratingRowRdd = ratingRDD.map(
				rating -> new RowFactory().create(rating.user() , rating.product() , rating.rating())

		);



	//DataFrame class for querying sql

		DataFrame usersDF = sqlContext.createDataFrame(userRDD, User.class);
		usersDF.registerTempTable("users");
		usersDF.printSchema();
		System.out.println("Total Number of users df : " + usersDF.count());

		DataFrame filteredUsersDF = sqlContext.sql("select * from users where users.userId in (11,12)");
		Row[] filteredUsers  = filteredUsersDF.collect();
		for(Row row : filteredUsers){
			System.out.print("UserId : " + row.getAs("userId"));
			System.out.print("	Gender : " + row.getAs("gender"));
			System.out.print("	Age : " + row.getAs("age"));
			System.out.print("	Occupation : " + row.getAs("occupation"));
			System.out.println("	Zip : " + row.getAs("zip"));
		}



		DataFrame schemaPeople = sqlContext.createDataFrame(ratingRowRdd, structType);
		schemaPeople.registerTempTable("ratings");
		schemaPeople.printSchema();

		DataFrame teenagers = sqlContext.sql("SELECT * FROM ratings WHERE ratings.user = 1 and product in (938,919)");
		System.out.println("Number of rows : (user = 1 and product = 938 ) : " + teenagers.count());
		Row[] filteredDF = teenagers.collect();
		for(Row row : filteredDF){
			System.out.print("UserId : " + row.getAs("user"));
			System.out.print("	MovieId : " + row.getAs("product"));
			System.out.println("	Rating : " + row.getAs("rating"));
		}


		DataFrame ratingsDF = sqlContext.sql(
				"select movies.title, movierates.maxr, movierates.minr, movierates.cntu  " +
				" from(SELECT ratings.product, max(ratings.rating) as maxr, " +
				" min(ratings.rating) as minr,count(distinct user) as cntu  " +
				" FROM ratings group by ratings.product ) movierates " +
				" join movies on movierates.product=movies.movieId " +
				" order by movierates.cntu desc ");
		ratingsDF.show();


		/**
		 * how the top 10 most-active users and how many times they rated
		 // a movie
		 *
		 */
		DataFrame top10MostActive = sqlContext.sql("SELECT ratings.user, count(*) as ct from ratings group by ratings.user order by ct desc limit 10");

		for(Row row : top10MostActive.collectAsList()){
			System.out.println(row);
		}

		/**
		 * Find the movies that user 4169 rated higher than 4
		 */
		DataFrame idUser4169 = sqlContext.sql("SELECT ratings.user, ratings.product,ratings.rating, movies.title FROM ratings JOIN movies ON movies.movieId=ratings.product "
				+ "where ratings.user=4169 and ratings.rating > 4");

		idUser4169.show();


		//Prediction use cases



		JavaRDD<Rating>[] ratingSplits = ratingRDD.randomSplit(new double[] { 0.8, 0.2 });

		JavaRDD<Rating> trainingRatingRDD = ratingSplits[0].cache();
		JavaRDD<Rating> testRatingRDD = ratingSplits[1].cache();

		long numOfTrainingRating = trainingRatingRDD.count();
		long numOfTestingRating = testRatingRDD.count();

		System.out.println("Number of training Rating : " + numOfTrainingRating);
		System.out.println("Number of training Testing : " + numOfTestingRating);

		//Create prediction model (Using ALS)
		ALS als = new ALS();
		MatrixFactorizationModel model = als.setRank(20).setIterations(10).run(trainingRatingRDD);

// Get the top 5 movie predictions for user 4169

		Rating[] recommendedsFor4169 = model.recommendProducts(4169, 5);
		System.out.println("Recommendations for 4169");
		for (Rating ratings : recommendedsFor4169) {
			System.out.println("Product id : " + ratings.product() + "-- Rating : " + ratings.rating());
		}

		// Get user product pair from testRatings
		JavaPairRDD<Integer, Integer> testUserProductRDD = testRatingRDD.mapToPair(
				rating -> new Tuple2<Integer, Integer>(rating.user(), rating.product())
		);

		JavaRDD<Rating> predictionsForTestRDD = model.predict(testUserProductRDD);

		System.out.println("Test predictions");
		predictionsForTestRDD.take(10).stream().forEach(rating -> {
			System.out.println("Product id : " + rating.product() + "-- Rating : " + rating.rating());
		});

//We will compare the test predictions to the actual test ratings


		JavaPairRDD<UserProductTuple, Double> predictionsKeyedByUserProductRDD = predictionsForTestRDD.mapToPair(
				rating -> new Tuple2<UserProductTuple, Double>(new UserProductTuple(rating.user(), rating.product()),rating.rating())
		);


		JavaPairRDD<UserProductTuple, Double> testKeyedByUserProductRDD = testRatingRDD.mapToPair(
				rating -> new Tuple2<UserProductTuple, Double>(new UserProductTuple(rating.user(), rating.product()),rating.rating())
		);

		JavaPairRDD<UserProductTuple, Tuple2<Double,Double>> testAndPredictionsJoinedRDD  = testKeyedByUserProductRDD.join(predictionsKeyedByUserProductRDD);

		testAndPredictionsJoinedRDD.take(10).forEach(k ->{
			System.out.println(
					"UserID : " + k._1.getUserId() +
					"||ProductId: " + k._1.getProductId() +
					"|| Test Rating : " + k._2._1 +
					"|| Predicted Rating : " + k._2._2);
		});

		//Step 7 ) Find false positives

		JavaPairRDD<UserProductTuple, Tuple2<Double,Double>> falsePositives =  testAndPredictionsJoinedRDD.filter(
				k -> k._2._1 <= 1 && k._2._2 >=4
		);

		System.out.println("testAndPredictionsJoinedRDD  count : " + testAndPredictionsJoinedRDD.count());
		System.out.println("False positives count : " + falsePositives.count());


//Step 8 ) Find absolute differences between the predicted and actual targets.

		JavaDoubleRDD meanAbsoluteError = testAndPredictionsJoinedRDD.mapToDouble(
				v1 -> Math.abs(v1._2._1 - v1._2._2));

		System.out.println("Mean : " + meanAbsoluteError.mean());

	}

	private static JavaRDD<Rating> getRatingJavaRDD(JavaSparkContext jsc) {
		JavaRDD<Rating> ratingRDD = jsc.textFile(ratingsPath)
				.map(
					line ->{
						String[] ratingArr = line.split("::");
						Integer userId = Integer.parseInt(Try.ofFailable(() -> ratingArr[0]).orElse("-1"));
						Integer movieId = Integer.parseInt(Try.ofFailable(() -> ratingArr[1]).orElse("-1"));
						Double rating = Double.parseDouble(Try.ofFailable(() -> ratingArr[2]).orElse("-1"));
						return new Rating(userId, movieId, rating);
			}).cache();

		System.out.println("RatingRDD get count : " + ratingRDD.count());
		return ratingRDD;
	}

	private static JavaRDD<User> getUserJavaRDD(JavaSparkContext jsc) {
		JavaRDD<User> userRDD = jsc.textFile(usersPath)
				.map(line ->{
					String[] userArr = line.split("::");
					Integer userId = Integer.parseInt(Try.ofFailable(() -> userArr[0]).orElse("-1"));
					Integer age = Integer.parseInt(Try.ofFailable(() -> userArr[2]).orElse("-1"));
					Integer occupation = Integer.parseInt(Try.ofFailable(() -> userArr[3]).orElse("-1"));
					return new User(userId, userArr[1], age, occupation, userArr[4]);
				})
				.cache();

		System.out.println("UserRDD get count : " + userRDD.count());
		return userRDD;
	}

	private static JavaRDD<Movie> getMovieJavaRDD(JavaSparkContext jsc) {
		JavaRDD<Movie> movieRDD =  jsc.textFile(moviesPath)
                .map(line -> {
                    String[] movieArr = line.split("::");
                    Integer movieId =  Integer.parseInt((String) Try.ofFailable(() -> movieArr[0]).orElse("-1"));
                    return new Movie(movieId, movieArr[1], movieArr[2]);
                })
                .cache();

		System.out.println("MovieRDD get first  and count :" + movieRDD.first() + "  " + movieRDD.count());
		return movieRDD;
	}
}


//NOTES



//		bir rdd uzerinde islem yapip sonucu baska rdd atmak,
//		JavaRDD<Integer> sayilar=jsc.parallelize(Arrays.asList(1,2,3,4));
//		JavaRDD<Integer> ikiKati=sayilar.map(
//				satir -> satir*2);
//		System.out.println(StringUtils.join(ikiKati.collect(),"-"));
//
//		rdd birlestirme farkli olanlari birlestir,kesisimleri al alma vs..
//		RDD1={“Merhaba”,”dünyalı”,”biz”,”dostuz”}
//		RDD2={“Merhaba”,”dünyalı”,”marsta”,”hayat”,”varmış”}
//		RDD1.union(RDD2) -> Tüm elemanları birleştirir, tekrar vardır.
//			RDD1.intersection(RDD2)->Kesişim Kümesi -> {“Merhaba”,”dünyalı”}
//		RDD1.subtract(RDD2)->Fark Kümesi ->={”biz”,”dostuz” ,”marsta”,”hayat”,”varmış”}
//
//		discint ile de farkli olanlar yeni bir rdd olur.
//		RDD={”to”, “be”, “or”, “not”, “to”, “be”}
//		RDD.distinct()->{”to”, “be”, “or”, “not”}

//		RDD.countByValue() RDD’ nin her elemanın sayısını ayrı ayrı getirir. { (1,1),(2,1),(3,2)}

//		unpersist() ile cache’ e alınmış bilgiyi düğüm hafızalarından silebiliriz. (Persist-> Sevgili Spark Diske Yazabilirsin, Cache->Lütfen Sadece Hafızayı (RAM) kullan)
