/*
NAME: GUPTA ANANYA VIKAS
MATRICULATION NUMBER: A0226576W
*/

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Comparator;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Scanner;
import java.util.Map;
import java.util.Map.*;
import java.util.*;
import java.io.InputStreamReader;
import java.io.File;
import java.io.BufferedReader;
import java.io.*;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//Tasks to do
//Mapper: 
//tokenize text input + avoid stop words + only words more than 4 length + ignore ""
//Reducer: 
// only common words from both files + keeping lower freq count 
//descending order of freq + ascending alphabetic order for same freq
//only get first k words from sorted list of words

//References for approach: 
// https://courses.cs.duke.edu/spring14/compsci290/lectures/08-mapreduce.pdf
// https://leetcode.com/problems/top-k-frequent-words/solutions/2721906/c-solution-using-hashmap/?page=2
// https://stackoverflow.com/questions/19012482/how-to-get-the-input-file-name-in-the-mapper-in-a-hadoop-program
// https://www.geeksforgeeks.org/java-program-to-sort-a-hashmap-by-keys-and-values/

public class TopkCommonWords {

    private static HashSet<String> stopwords = new HashSet<String>();
    private static Integer k;
    private static String fn1;
    private static String fn2;

    public static class TokenizerMapper extends Mapper<Object,Text,Text, Text> {

        private Text word = new Text();
        private String file = new String();

        //tokenize input files into (word,filename)  
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            
            FileSplit fs = ((FileSplit) context.getInputSplit());
            file = fs.getPath().getName();

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String token = word.toString();
                if (token.length()<=4 || stopwords.contains(token) || token.equals("")) { //checking for stop conditions
                    continue;
                }
                else {
                    Text fn = new Text(file);
                    context.write(word,fn);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,Text,IntWritable,Text> {
        
        private IntWritable result = new IntWritable();
        private HashMap<String,Integer> hp = new HashMap<String,Integer>(); //final hp with min count
        private HashMap<String,Integer> hp1 = new HashMap<String,Integer>(); //count of f1
        private HashMap<String,Integer> hp2 = new HashMap<String,Integer>(); //count of f2

        //individual occurence followed by min common occurences(dictionary approach)
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                if (val.toString().equals(fn1)) { //freq of key in file 1
                    if (hp1.containsKey(key.toString())) {
                        hp1.put(key.toString(),hp1.get(key.toString())+1);
                    }
                    else {
                        hp1.put(key.toString(),1);
                    }
                }
                if (val.toString().equals(fn2)) { //freq of key in file 2
                    if (hp2.containsKey(key.toString())) {
                        hp2.put(key.toString(),hp2.get(key.toString())+1);
                    }
                    else {
                        hp2.put(key.toString(),1);
                    }
                }
            }
            if (hp1.containsKey(key.toString()) && hp2.containsKey(key.toString())) { //only common word keys
                int sum = Math.min(hp1.get(key.toString()),hp2.get(key.toString()));
                result.set(sum);
                hp.put(key.toString(),result.get());
            }
        }

        //custom comparator for sorting and then mapping for first k only
        public void cleanup(Context context) throws IOException, InterruptedException {
            
            Comparator<Map.Entry<String, Integer>> comp = new Comparator<Map.Entry<String, Integer>>() {
            
                public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
                    int val1 = b.getValue().compareTo(a.getValue()); // descending values
                    int val2 = a.getKey().compareTo(b.getKey()); //if equal values, alphabetical keys

                    if (val1 != 0) {
                        return val1;
                    } 
                    else { 
                        return val2; 
                    }
                }
            };

            ArrayList<Map.Entry<String, Integer>> lst = new ArrayList<Map.Entry<String, Integer>>(hp.entrySet());
            Collections.sort(lst, comp); 
            
            HashMap<String, Integer> ordered = new LinkedHashMap<String,Integer>();
            
            for (Map.Entry<String, Integer> ele : lst) {
                ordered.put(ele.getKey(), ele.getValue());
            }

            ArrayList<String> alpha_keys = new ArrayList<String>(ordered.keySet());
            for (int ind = 0; ind < k; ind++) {
                Text word_text = new Text(alpha_keys.get(ind));
                IntWritable num_word = new IntWritable(ordered.get(alpha_keys.get(ind)));
                context.write(num_word, word_text); //(count,word)
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //FileSystem fs = new FileSystem(conf);
        //Path interDirPath = new Path("/home/course/cs4225/cs4225_assign/temp/assign1_inter/A0226576W");

        //similar to CS2040 contest problem approach
        k = Integer.parseInt(args[4]);

        Path p1 = new Path(args[0]);
        Path p2 = new Path(args[1]);

        fn1 = p1.getName();
        fn2 = p2.getName();

        Scanner scanner = new Scanner(new File(args[2])); //stopwords populating
        while (scanner.hasNextLine()) {
            String w = scanner.nextLine();
            stopwords.add(w);
        }

        Job job = Job.getInstance (conf, "TopkCommonWords");
        job.setJarByClass (TopkCommonWords.class);
        job.setMapperClass (TokenizerMapper.class);
        job.setReducerClass (IntSumReducer.class); 
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass (IntWritable.class);
        job.setOutputValueClass (Text.class);
        FileInputFormat.addInputPath(job, p1);
        FileInputFormat.addInputPath(job, p2);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        
        boolean hasCompleted = job.waitForCompletion(true);
        //fs.delete(interDirPath, true); // ONLY call this after your last job has completed to delete your intermediate directory
        
        System.exit(hasCompleted ? 0 : 1); // there should be NO MORE code below this line
    }
}
