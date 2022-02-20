package oddeve;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
    public void map(LongWritable lineno,Text data,OutputCollector<Text,IntWritable> values,Reporter r) throws IOException{
        String arr[] = data.toString().split(" ");
        for(String no:arr){
            int num = Integer.parseInt(no);
            if(num%2==0)
                values.collect(new Text("ODD"),new IntWritable(num));
            else
                values.collect(new Text("EVEN"),new IntWritable(num));
        }
    }
}