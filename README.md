# Big-data-analytics-da--1
question:
Develop a Map Reduce program to read a stud.txt file and display top 3 ranker in every subject in Fall semester of 2024


 PROBLEM DESCRIPTION:
The task is to create a MapReduce program that processes a file called stud.txt, which contains students' scores in various subjects, and finds the top 3 rankers in each subject for the Fall semester of 2024.

CODE:
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class L10Q2_22MIS1046 {

  public static class M
       extends Mapper<Text, Text, Text, Text>{
	
    public void map(Text key, Text value, Context context
                    ) throws IOException, InterruptedException {
      context.write(key,value);
    }
  }

  public static class P extends Partitioner<Text,Text>{
    public int getPartition(Text key,Text val,int num){
      if("Mathematics".equals(key.toString())) return 0;
      else if("Physics".equals(key.toString())) return 1;
      else if("Chemistry".equals(key.toString())) return 2;
      else if("Biology".equals(key.toString())) return 3;
      else if("English".equals(key.toString())) return 4;
      else if("History".equals(key.toString())) return 5;
      else if("Geography".equals(key.toString())) return 6;
      else if("CS".equals(key.toString())) return 7;
      else if("Economics".equals(key.toString())) return 8;
      else return 9;
    }
  }
  
  public static class R extends Reducer<Text,Text,Text,Text>{
    
    public void reduce(Text key,Iterable<Text> val,Context context) throws IOException,InterruptedException{
      String name1="",name2="",name3="";
      int mark1=0,mark2=0,mark3=0;
      for(Text t:val){
        String str = t.toString();
        String s[] = str.split(" ");
        
        if(Integer.parseInt(s[1])>mark1){
            mark3 = mark2;
            mark2 = mark1;
            mark1 = Integer.parseInt(s[1]);
            name3 = name2;
            name2 = name1;
            name1 = s[0];
       }
       else if(Integer.parseInt(s[1])>mark2 && mark2<=mark1){
            mark3 = mark2;
            mark2 = Integer.parseInt(s[1]);
            name3 = name2;
            name2 = s[0];
       }
       else if(Integer.parseInt(s[1])>mark2 && mark3<=mark2){
            mark3 = Integer.parseInt(s[1]);
            name3 = s[0];
       }
    }
    String result1 = name1+" "+mark1;
    context.write(key,new Text(result1));
    String result2 = name2+" "+mark2;
    context.write(key,new Text(result2));
    String result3 = name3+" "+mark3;
    context.write(key,new Text(result3));
    }
  } 
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
    Job job = new Job(conf, "word count");
    job.setJarByClass(L10Q2_22MIS1046.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapperClass(M.class);
    job.setReducerClass(R.class);
    job.setPartitionerClass(P.class);
    job.setNumReduceTasks(10);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
 
CODE EXPLANATION:

This Hadoop MapReduce program processes student score data for various subjects and finds the **top 3 rankers** in each subject. Here's an explanation of the main components:

1. Mapper (M class):
- The **Mapper** takes key-value pairs as input, where the key represents the subject, and the value contains the student name and score.
- It simply passes these key-value pairs to the next stage (partitioner) without modification using the `context.write(key, value)` method.

2. Partitioner (P class):
- The Partitioner is responsible for determining which reducer processes which subject's data.
- Based on the subject name (key), it assigns each subject to a different reducer:
  - For example, `"Mathematics"` goes to partition 0, `"Physics"` to partition 1, and so on.
- This ensures that all data related to a specific subject (e.g., Mathematics) is processed by the same reducer.

3. Reducer (R class):
- The **Reducer** receives the student data for each subject as input.
- It processes the list of students and their scores, identifies the **top 3 students** with the highest scores, and writes them to the output.
- The logic in the reducer sorts and updates the top 3 scores (`mark1`, `mark2`, `mark3`) and corresponding student names (`name1`, `name2`, `name3`).
- It then outputs the top 3 students for each subject as separate lines.

4. Main Method:
- This is where the **MapReduce job** is configured:
  - The input format is set to `KeyValueTextInputFormat`, where the key-value separator is a space `" "`.
  - The **Mapper**, **Reducer**, and **Partitioner** classes are specified.
  - It also sets 10 reducers (`setNumReduceTasks(10)`), each handling a different subject.
- The job reads data from the input path and writes the results (top 3 students per subject) to the output path.

● Program Objective:
- The program reads student scores from an input file.
- It partitions the data by subject.
- For each subject, it finds the top 3 students with the highest scores and outputs them.

- OUTPUT:
- 
![Screenshot (430)](https://github.com/user-attachments/assets/e5e2bcb8-274b-42ee-a9ce-d56ebc4b7918)
![Screenshot (431)](https://github.com/user-attachments/assets/28e05248-b5a9-4022-87e4-f7a32575d96c)



