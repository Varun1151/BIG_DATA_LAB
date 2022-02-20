package word;


import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{
    public void reduce(Text word,Iterator<IntWritable> count,OutputCollector<Text,IntWritable> values,Reporter r)throws IOException{
        int ans=0;
        while(count.hasNext()){
            ans+=count.next().get();
        }
        values.collect(word,new IntWritable(ans));
    }
}