//***************************************************************
//
//  Developer:    Marshal Pfluger
//
//  Project #:    Project Eight
//
//  File Name:    Project8Driver.java
//
//  Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//  Due Date:     11/27/2023
//
//  Instructor:   Prof. Fred Kumi 
//
//  Description:  uses Spark to analyze stock data from a file
//
//***************************************************************
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import scala.Tuple2;


public class Project8Driver {
	
	//***************************************************************
    //
    //  Method:       main
    // 
    //  Description:  The main method of the program
    //
    //  Parameters:   String array
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    public static void main(String[] args) {
    	Project8Driver obj = new Project8Driver();
    	obj.developerInfo();
        obj.stockData();
    }

	//***************************************************************
    //
    //  Method:       StockData
    // 
    //  Description:  runs the Spark program
    //
    //  Parameters:   N/A
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    public void stockData() {
    	
    	// Suppress the warnings to only get important info
    	Logger.getLogger("org.apache").setLevel(Level.WARN);

    	// Create spark Conf
		SparkConf sparkConf = null;
		try {
			sparkConf = new SparkConf().setAppName("Max Closing Price").setMaster("local[*]");
			sparkConf.set("spark.driver.bindAddress", "127.0.0.1");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        // Create the spark Context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Set the input file and create RDD with it
        JavaRDD<String> inputFile = sparkContext.textFile("Project8.txt");

        // Create a PairRDD with stock symbol as the key and Tuple2 as the value
        JavaPairRDD<String, Tuple2<String, Double>> symbolClosingPricePairRDD = inputFile
                .mapToPair(line -> {
                    String[] parts = line.split(",");
                    String stockSymbol = parts[1];
                    String date = parts[2];
                    double closingPrice = Double.parseDouble(parts[4]);
                    return new Tuple2<>(stockSymbol, new Tuple2<>(date, closingPrice));
                });

        // Use reduceByKey to find the maximum closing price for each stock symbol
        JavaPairRDD<String, Tuple2<String, Double>> maxClosingPricePairRDD = symbolClosingPricePairRDD
                .reduceByKey((t1, t2) -> {
                    if (t1._2() >= t2._2()) {
                        return t1;
                    } else {
                        return t2;
                    }
                }).sortByKey(true);


        // Print the results
        maxClosingPricePairRDD.collect().forEach(tuple -> System.out.println(tuple._1 + ": " + tuple._2._2 + "   Date: " + tuple._2._1));

        
        // Uncomment to send output to a file
        //maxClosingPricePairRDD.saveAsTextFile("CountData"); 
        
        // Close sparkContext
        sparkContext.close();
    }

    //***************************************************************
    //
    //  Method:       developerInfo (Non Static)
    // 
    //  Description:  The developer information method of the program
    //                This method must be included in all projects.
    //
    //  Parameters:   None
    //
    //  Returns:      N/A
    //
    //***************************************************************
	public void developerInfo()
	{
		System.out.println("Name:    Marshal Pfluger");
		System.out.println("Course:  COSC 3365 Distributed Databases Using Hadoop");
		System.out.println("Program: Eight\n");
		} // End of the developerInfo method
}