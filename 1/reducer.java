package temp;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,Text>{
    public void reduce(Text year,Iterator<IntWritable> temps,OutputCollector<Text,Text> values,Reporter r) throws IOException{
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        values.collect(new Text("\nYear"), new Text("(Max , Min)"));
        while(temps.hasNext()){
            int temp = temps.next().get();
            max = Math.max(temp,max);
            min = Math.min(temp,min);
        }
        values.collect(year,new Text("("+max+" , "+min+")"));
    } 
}