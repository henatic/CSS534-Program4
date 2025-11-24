import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction; // Import needed for FlatMapFunction
import java.util.Arrays;
import java.util.Iterator;

public class JavaWordCountFlatMap {
 
    public static void main(String[] args) {
        // configure spark
        // Note: For cluster execution, you typically remove .setMaster("local[2]") 
        // and pass the master via command line, but for this lab, we'll leave it 
        // or you can comment it out to use spark-submit arguments.
        SparkConf sparkConf = new SparkConf().setAppName("Java Word Count FlatMap")
            .setMaster("local[2]").set("spark.executor.memory","2g");
            
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        // provide path to input text file
        String path = "sample.txt";
        
        // read text file to RDD
        JavaRDD<String> lines = sc.textFile(path);

        // LAB 4B CHANGE: Replace lambda with FlatMapFunction anonymous class
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        
        // print #words
        System.out.println( "#words = " + words.count( ) );
        
        sc.stop(); // Good practice to stop the context
    }
 
}