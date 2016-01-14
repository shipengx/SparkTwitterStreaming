package com.shipeng;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.api.java.StorageLevels;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.util.Arrays;
import java.util.List;


public class LoadTwitterStream {

    public static void main(String[] args) {
 
        if (args.length < 4) {
             System.err.println("Usage: LoadTwitterStream <consumer key> <consumer secret>" + " <access token> <access token secret> [<filters>]");
             System.exit(1);
         }
 
         String consumerKey        =  args[0];
         String consumerSecret     =  args[1];
         String accessToken        =  args[2];
         String accessTokenSecret  =  args[3];
         String[] filters = Arrays.copyOfRange(args, 4, args.length);
 
 
         //Set the system properties so that Twitter4j library used by Twitter stream can use them to generate OAuth credentials.
         System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
         System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
         System.setProperty("twitter4j.oauth.accessToken", accessToken);
         System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
 
 
         SparkConf  sparkConf = new SparkConf().setAppName("Java spark twitter stream");
         JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
         JavaDStream<Status> tweets = TwitterUtils.createStream(ssc, filters);
 

         JavaDStream<String> statuses = tweets.map(
            new Function<Status, String>() {
                public String call(Status status) { return status.getText(); }
            }
         );
         statuses.print();


         /*

         // Convert RDDs of the statuses DStream to DataFrame and run SQL query
         statuses.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
             @Override
             public Void call(JavaRDD<String> rdd, Time time) {
                 SQLContext sqlContext = JavaSQLContextSingleton.getInstance(rdd.context());

                 // Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
                 JavaRDD<Tweet> rowRDD = rdd.map(new Function<String, Tweet>() {
                     @Override
                     public Tweet call(String value) {
                         Tweet tweet = new Tweet();
                         tweet.setStatus(value);
                         return tweet;
                     }
                 });
                 DataFrame statusesDataFrame = sqlContext.createDataFrame(rowRDD, Tweet.class);

                 // Register as table
                 statusesDataFrame.registerTempTable("statuses");

                 // Do word count on table using SQL and print it
                 DataFrame wordCountsDataFrame =
                     sqlContext.sql("select count(*) as total from statuses ");
                 System.out.println("========= " + time + "=========");
                 wordCountsDataFrame.show();
                 return null;
             }
         });

         */


         ssc.start();
         ssc.awaitTermination();
    
    }//end main

}//end class LoadTwitterStream



/** lazily instantiated singleton instance of SQLContext */
class JavaSQLContextSingleton {
  private static transient SQLContext instance = null;
  public static SQLContext getInstance(SparkContext sparkContext) {
    if (instance == null) {
      instance = new SQLContext(sparkContext);
    }
    return instance;
  }
}


