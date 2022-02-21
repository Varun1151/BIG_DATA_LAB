package matmul;

import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
    public void reduce(Text key,Iterator<Text> data,OutputCollector<Text,Text> values,Reporter r) throws IOException{
        int n=5;
        HashMap<Integer,Float> hashA = new HashMap<>();
        HashMap<Integer,Float> hashB = new HashMap<>();
        while(data.hasNext()){
            String arr[] = data.next().toString().split(",");
            if(arr[0].equals("A"))
                hashA.put(Integer.parseInt(arr[1]),Float.parseFloat(arr[2]));
            else
                hashB.put(Integer.parseInt(arr[1]),Float.parseFloat(arr[2]));
        }
        float result=0.0f;
        for(int j=0;j<n;j++){
            float a_ij = hashA.containsKey(j)?hashA.get(j):0.0f;
            float b_jk = hashB.containsKey(j)?hashB.get(j):0.0f;
            result=result+a_ij*b_jk;
        }
        if(result!=0.0f)
            values.collect(null,new Text(key.toString()+","+result));
    }
}