import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

public class JavaFPGrowthExample {
	public static void main(String[] args) {
        String fileName = args[0];
		SparkConf conf = new SparkConf().setAppName("JavaFPGrowthExample").set("spark.acls.enable", "true");
		JavaSparkContext sc = new JavaSparkContext(conf); 
		JavaRDD<String> data = sc.textFile(fileName);	 	
		JavaRDD<List<String>> transactions = data.map(new Function<String, List<String>>() {
			public List<String> call(String line) {
				String[] parts = line.split(" ");
				return Arrays.asList(parts);
			}
		});

		FPGrowth fpg = new FPGrowth().setMinSupport(0.2).setNumPartitions(10);
		FPGrowthModel<String> model = fpg.run(transactions);
		for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {		
				System.out.println(itemset.javaItems() + "==> " + itemset.freq());
			}
		}		
	}