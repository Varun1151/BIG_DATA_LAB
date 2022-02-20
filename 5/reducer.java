package oddeve;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{
    public void reduce(Text text,Iterator<IntWritable> data,OutputCollector<Text,IntWritable> values,Reporter r) throws IOException{
        int sum=0,count=0;
        while(data.hasNext()){
            sum=sum+data.next().get();
            count=count+1;
        }
        values.collect(new Text(text.toString()+" SUM"),new IntWritable(sum));
        values.collect(new Text(text.toString()+" COUNT"),new IntWritable(count));
    }
}