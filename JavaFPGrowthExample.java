import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

public class JavaFPGrowthExample {
	public static void main(String[] args) {
   //Specify the input transactional as command line argument 
   String fileName = "input/input.txt"; 
   //Configure a SparkSession as spark by specifying the application name, master URL, Spark config, and Spark warehouse directory
	SparkSession spark = SparkSession
			            .builder()
			            .appName("JavaFPGrowthExample")
			            .master("local[*]")
			            .config("spark.sql.warehouse.dir", "E:/Exp/")
			            .getOrCreate();
	
   //Create an initial RDD by reading the input database 
   RDD<String> data = spark.sparkContext().textFile(fileName, 1);
	
   //Read the transactions by tab delimiter & mapping RDD(data)
   JavaRDD<List<String>> transactions = data.toJavaRDD().map(
                   new Function<String, List<String>>(){
                   public List<String> call(String line) {
				                  String[] parts = line.split(" ");
				                  return Arrays.asList(parts);
			                           }
		                         });

  //Create FPGrowth object by min. support & no. of partition		
  FPGrowth fpg = new  FPGrowth()
                       .setMinSupport(0.2)
                       .setNumPartitions(10);

  //Train and run your FPGrowth model using the transactions
  FPGrowthModel<String> model = fpg.run(transactions);
       
  //Convert and then collect frequent patterns as Java RDD. After that print the frequent patterns along with their support
		for (FPGrowth.FreqItemset<String> itemset :     
          model.freqItemsets().toJavaRDD().collect()) {	
			 System.out.println(itemset.javaItems() 
                             + "==> " + itemset.freq());
			}
		}	
	}