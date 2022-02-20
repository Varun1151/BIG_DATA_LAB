package average;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,FloatWritable,Text,FloatWritable>{
    public void reduce(Text text,Iterator<FloatWritable> data,OutputCollector<Text,FloatWritable> values,Reporter r) throws IOException{
        float sum=0.0f,count=0.0f;
        while(data.hasNext()){
            sum=sum+data.next().get();
            count=count+1.0f;
        }
        
        values.collect(new Text(text.toString()+" COUNT"),new FloatWritable(count));
        values.collect(new Text(text.toString()+" AVERAGE"),new FloatWritable(sum/count));
    }
}