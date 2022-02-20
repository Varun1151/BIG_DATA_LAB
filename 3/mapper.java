package insurance;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
    public void map(LongWritable lineno,Text data, OutputCollector<Text,IntWritable> values,Reporter r) throws IOException{
        String arr[] = data.toString().split(",");
        if(arr[0].equals("policyID"))
            return;
        // For county frequency use arr[2], construction building name use arr[16]
        values.collect(new  Text(arr[16]),new IntWritable(1));
    }
}