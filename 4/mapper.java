package sales;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
    public void map(LongWritable lineno,Text data,OutputCollector<Text,IntWritable> values,Reporter r) throws IOException{
        String arr[] = data.toString().split(",");
        if(arr[2].equals("Price"))
            return;

        // Doing 2nd Part 

        int price = Integer.parseInt(arr[2]);
        String transaction_type = arr[3];
        String country = arr[7];

        values.collect(new Text("Country_"+country),new IntWritable(price));
        values.collect(new Text("Transaction_"+transaction_type),new IntWritable(1));
    }
}