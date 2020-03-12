import java.util.StringTokenizer;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class adjList {

  public static class adjMapperOne
       extends Mapper<Object, Text, Text, Text>{

      //private final static IntWritable one = new IntWritable(1);
    private Text outVal = new Text();
      private Text outKey = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String inline = value.toString();
        if (!inline.startsWith("#"))
        {
            String [] inVals = inline.split("\t");
            outKey.set(inVals[0]);
            outVal.set(inVals[1]);
            context.write(outKey, outVal);
        }
    }
  }

 public static class adjReducerOne
      extends Reducer<Text,Text,Text, Text> {
    private Text result = new Text();
    private Text longadj=new Text();
    private Text min_key=new Text();
    private Text max_key=new Text();
    private Text min_value=new Text();
    private Text max_value=new Text();
        HashMap<String,Integer> hmap =new HashMap<String,Integer>();
        int max=0;
        int min=Integer.MAX_VALUE;
        public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
                int cntr = 0;
                String adjlst = "";
                for (Text val : values)
                {
                adjlst = adjlst+","+val;
                cntr++;
                }
      adjlst = adjlst.substring(1);
     /* if (!adjlst.contains("#"))
        {
          adjlst = adjlst+"#"+cntr;

        }*/
        String cKey=key.toString();
        hmap.put(cKey,cntr);


                if(cntr>max){
                max=cntr;
                longadj.set(adjlst);
                max_value.set(Integer.toString(max));
                max_key.set(key);
                }


                if(cntr<min){
                min=cntr;
                min_value.set(Integer.toString(min));
                min_key.set(key);
                }

//      String cmax_key=max_key.toString();
//      String cmin_key=max_key.toString();
     // result.set(adjlst);
     // context.write(key, result);
      //context.write(result,key);
    }
protected void cleanup(Context context) throws IOException,InterruptedException{
        context.write(new Text("The node with the longest Adjacency list is:"),new Text(max_key));
        context.write(new Text("Longest Adjacency List is"),longadj);
        context.write(new Text("The node with maximum connectivity is:"),max_key);
        context.write(new Text("The node with maximum connectivity has a node count of:"),new Text(max_value));
        context.write(new Text("The node with minimum connectivity is:"),min_key);
        context.write(new Text("The node with minimum connectivity has a node count of:"),new Text(min_value));
        }
}

  public static class adjMapperTwo
       extends Mapper<Object, Text, Text, Text>{

      //private final static IntWritable one = new IntWritable(1);
    private Text outVal = new Text();
      private Text outKey = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String inline = value.toString();
        if (!inline.startsWith("#"))
        {
            String [] inVals = inline.split("\t");
            outKey.set(inVals[0]);
            outVal.set(inVals[1]);
            context.write(outKey, outVal);
            context.write(outVal, outKey);

        }
    }
  }

 public static class adjReducerTwo
      extends Reducer<Text,Text,Text, Text> {
    private Text result = new Text();
        private Text longadj=new Text();
    private Text min_key=new Text();
    private Text max_key=new Text();
    private Text min_value=new Text();
    private Text max_value=new Text();
        HashMap<String,Integer> hmap =new HashMap<String,Integer>();
        int max=0;
    int min=Integer.MAX_VALUE;

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int cntr = 0;
      String adjlst = "";
      HashSet<String> duplicates = new HashSet<>();
      for (Text val : values)
      {
          if(duplicates.contains(val.toString()))
                continue;
          duplicates.add(val.toString());
          adjlst = adjlst+","+val;
          cntr++;
      }
      adjlst = adjlst.substring(1);
      /*if (!adjlst.contains("#"))
        {
          adjlst = adjlst+"#"+cntr;

        }*/
        String cKey=key.toString();
        hmap.put(cKey,cntr);


        if(cntr>max){
        max=cntr;
        longadj.set(adjlst);
        max_key.set(key);
        max_value.set(Integer.toString(max));
        }


        if(cntr<min){
        min=cntr;
        min_key.set(key);
        min_value.set(Integer.toString(min));
        }


      //result.set(adjlst);
      //context.write(key, result);

    }
        protected void cleanup(Context context) throws IOException,InterruptedException{
        context.write(new Text("The node with the longest Adjacency list is:"),new Text(max_key));
        context.write(new Text("Longest Adjacency List is"),longadj);
        context.write(new Text("The node with maximum connectivity is:"),max_key);
        context.write(new Text("The node with maximum connectivity has a node count of:"),new Text(max_value));
        context.write(new Text("The node with minimum connectivity is:"),min_key);
        context.write(new Text("The node with minimum connectivity has a node count of:"),new Text(min_value));
}
  }





  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Directed Adjacency List");
    job.setJarByClass(adjList.class);
    job.setMapperClass(adjMapperOne.class);
    //job.setCombinerClass(adjReducerOne.class);
    job.setReducerClass(adjReducerOne.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
  job.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Undirected Adjacency List");
    job2.setJarByClass(adjList.class);
    job2.setMapperClass(adjMapperTwo.class);
   // #job2.setCombinerClass(adjReducerTwo.class);
    job2.setReducerClass(adjReducerTwo.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);




    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));

    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}

