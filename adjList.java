import java.io.IOException;

import java.util.StringTokenizer;

import java.util.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class adjList 
{

  public static class adjMapperdirected extends Mapper<Object, Text, Text, Text>
  {
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

  public static class adjMapperundirected extends Mapper<Object, Text, Text, Text>
  {
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
	   
	    context.write(outVal,outKey); 
        }
    }
  }

  public static class adjReducerdirected extends Reducer<Text,Text,Text,Text> 
  {
     int maxvalueinmap;
     int minvalueinmap;
     Text maxkey=new Text();
     Text minkey=new Text();
     int cntr=0;
     int max_cntr=0;
     int min_cntr=Integer.MAX_VALUE;
     String adjlst2="";
		
     @Override
     public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		HashSet<String> hs=new HashSet();
		String adjlst = "";
		for (Text val : values)
		{
		hs.add(val.toString());
        	  adjlst = adjlst+","+val;
		}
		cntr=hs.size();
	
		adjlst=adjlst.substring(1);

		StringTokenizer tokenizer=new StringTokenizer(adjlst,",");
		
		if(cntr>max_cntr)
		{
			max_cntr=cntr;
			maxkey.set(key);
			adjlst2=adjlst;
		}
		if(cntr<min_cntr)
		{
			min_cntr=cntr;
			minkey.set(key);
                }
        }
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("--------------------------------Directed Graphs---------------------------------------"),new Text());
                context.write(new Text("Printing the Longest Adjacency List for: " + maxkey.toString() + "------>" ),new Text(adjlst2.substring(1)));
                context.write(new Text("--------------------------------------------------------------------------------------"),new Text());
                context.write(maxkey,new Text("Max connectivity node count is: " + Integer.toString(max_cntr)));
                context.write(minkey,new Text("Min connectivity node count is: " + Integer.toString(min_cntr)));
		context.write(new Text("--------------------------------------------------------------------------------"),new Text());
	}
  }

  public static class adjReducerundirected extends Reducer<Text,Text,Text,Text>
  {
     int maxvalueinmap;
     int minvalueinmap;
     Text maxkey=new Text();
     Text minkey=new Text();
     int cntr=0;
     int max_cntr=0;
     int min_cntr=Integer.MAX_VALUE;
     String adjlst2="";	

     @Override
     public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
        {
		HashSet<String> hs=new HashSet();
                String adjlst = "";
                for (Text val : values)
                {
	        hs.add(val.toString());
                  adjlst = adjlst+","+val;
                }
		cntr=hs.size();
                adjlst=adjlst.substring(1);

                StringTokenizer tokenizer=new StringTokenizer(adjlst,",");

                if(cntr>max_cntr)
                {
                        max_cntr=cntr;
                        maxkey.set(key);
                        adjlst2=adjlst;
                }
                if(cntr<min_cntr)
                {
                        min_cntr=cntr;
                        minkey.set(key);
                }
        }
	 protected void cleanup(Context context) throws IOException, InterruptedException {
                context.write(new Text("--------------------------------Undirected Graphs----------------------------------------"),new Text());
                context.write(new Text("Printing the Longest Adjacency List for: " + maxkey.toString() + "------>" ),new Text(adjlst2.substring(1)));
                context.write(new Text("----------------------------------------------------------------------------------------"),new Text());
                context.write(maxkey,new Text("Max connectivity node count is: " + Integer.toString(max_cntr)));
                context.write(minkey,new Text("Min connectivity node count is: " + Integer.toString(min_cntr)));
                context.write(new Text("----------------------------------------------------------------------------------------"),new Text());
        }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job1 = Job.getInstance(conf, "Cloud Computing Adj List Directed graph");

    job1.setJarByClass(adjList.class);

    job1.setMapperClass(adjMapperdirected.class);

    job1.setReducerClass(adjReducerdirected.class);

    job1.setOutputKeyClass(Text.class);

    job1.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job1, new Path(args[0]));

    FileOutputFormat.setOutputPath(job1, new Path(args[1]));

    job1.waitForCompletion(true);

    Job job2 = Job.getInstance(conf,"Cloud Computing Adj List Undirected graph");

    job2.setJarByClass(adjList.class);

    job2.setMapperClass(adjMapperundirected.class);

    job2.setReducerClass(adjReducerundirected.class);

    job2.setOutputKeyClass(Text.class);

    job2.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2, new Path(args[0]));

    FileOutputFormat.setOutputPath(job2, new Path(args[2]));

    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
	
