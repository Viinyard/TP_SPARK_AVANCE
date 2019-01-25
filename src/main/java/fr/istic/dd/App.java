package fr.istic.dd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import org.apache.spark.storage.StorageLevel;
import scala.*;

import java.lang.Double;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {
        SparkConf conf = new SparkConf().setAppName("Workshop").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        Broadcast<Integer[]> broadcast = sc.broadcast(new Integer[] {
            42, // assist
            1,  // dmg
            100, // kill
            999 // top1
        });
        
        String path = App.class.getClassLoader().getResource("agg_match_stats_0.csv").getPath();
        
        JavaRDD<String> lines = sc.textFile(path);
        String header = lines.first();
        JavaRDD<String> linesWithoutHeader = lines.filter(s -> !s.equals(header));
        
        //printLinesCount(linesWithoutHeader);
        
        //printTop10Players(linesWithoutHeader);
    
        //printTop10PlayersByRatio(linesWithoutHeader);
    
        JavaPairRDD<String, Tuple5<Integer, Integer, Integer, Integer, Integer>> data
            = linesWithoutHeader.mapToPair(s -> { String[] raws = s.split(","); return new Tuple2<>(raws[11], new Tuple5(
            Integer.parseInt(raws[5]),    // player_assists
            Integer.parseInt(raws[9]),    // player_dmg
            Integer.parseInt(raws[10]),   // player_kills
            Integer.parseInt(raws[14]),   // player_team_placement
            1                              // player_number_party
        ));
        });
        data.persist(StorageLevel.MEMORY_ONLY());
    
        question10(data, broadcast);
    
        //question12(data);
        
        data.unpersist();
        
        sc.stop();
    }
    
    public static void question12(JavaPairRDD<String, Tuple5<Integer, Integer, Integer, Integer, Integer>> data) {
        
        
        long nbJoueurAssistant = data.filter(t2 -> t2._1.length() > 0) // remove null pseudo
            .mapToPair(t2 -> new Tuple2<>(t2._1, new Tuple2<>(
                t2._2._1(),  // player_assists
                t2._2._3()    // player_kills
            )))
            .reduceByKey((t1, t2) -> new Tuple2<>(
                t1._1() + t2._1(),  // player_assists
                t1._2() + t2._2()  // player_kills
            )).filter(t1 -> t1._2._1 / (double) t1._2._2() >= 5).count();
        
        System.out.println("Nombre d'assistants : " + nbJoueurAssistant);
    }
    
    public static void question10(JavaPairRDD<String, Tuple5<Integer, Integer, Integer, Integer, Integer>> data, Broadcast<Integer[]> broadcast) {
        List<Tuple2<Double, String>> top10Players = data.filter(t2 -> t2._1.length() > 0) // remove null pseudo
            .mapToPair(t2 -> new Tuple2<>(t2._1, new Tuple5<>(
            t2._2._1() * broadcast.getValue()[0],  // player_assists
            t2._2._2() * broadcast.getValue()[1],  // player_dmg
            t2._2._3() * broadcast.getValue()[2],    // player_kills
            t2._2._4() == 1 ? broadcast.getValue()[3] : 0,    // player_team_placement
            t2._2._5()    // player_number_party
        )))
            .reduceByKey((t1, t2) -> new Tuple5<>(
            t1._1() + t2._1(),  // player_assists
            t1._2() + t2._2(),  // player_dmg
            t1._3() + t2._3(),  // player_kills
            t1._4() + t2._4(),  // player_team_placement
            t1._5() + t2._5()  // player_number_party
            )).
                mapToPair(t2 -> new Tuple2<Double, String>(t2._2._1() + t2._2._2() + t2._2._3() + t2._2._4() / (double) t2._2._5(),
                t2._1
            )).sortByKey(false).take(10);
    
        System.out.println("Top 10 Players By Score : ");
        for(Tuple2<Double, String> tuple : top10Players) {
            System.out.println(tuple._2 + " with a score of " + tuple._1 + " !");
        }
    }
    
    public static void printTop10PlayersByRatio(JavaRDD<String> data) {
    
        List<Tuple2<Double, String>> top10Players = data.mapToPair((s) -> new Tuple2<>(s.split(",")[11], new Tuple2<>(Integer.parseInt(s.split(",")[10]), 0))).filter(t -> t._1.length() > 0).reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._1)).filter(t1 -> t1._2._2 >= 4).mapToPair(t -> new Tuple2<>( t._2._1 / (double) t._2._2, t._1)).sortByKey(false).take(10);
        
        System.out.println("Top 10 Players : ");
        for(Tuple2<Double, String> tuple : top10Players) {
            System.out.println(tuple._2 + " with " + tuple._1 + " kills.");
        }
    }
    
    public static void printTop10Players(JavaRDD<String> data) {
        List<Tuple2<Integer, String>> top10Players = data.mapToPair((s) -> new Tuple2<>(s.split(",")[11], Integer.parseInt(s.split(",")[10]))).filter(t -> t._1.length() > 0).reduceByKey((v1, v2) ->  v1 + v2).mapToPair(p -> new Tuple2<>(p._2, p._1)).sortByKey(false).take(10);
    
        System.out.println("Top 10 Players : ");
        for(Tuple2<Integer, String> tuple : top10Players) {
            System.out.println(tuple._2 + " with " + tuple._1 + " kills.");
        }
    }
    
    public static void printLinesCount(JavaRDD<String> data) {
        System.out.println("Lines count : " + data.count());
    }
}
