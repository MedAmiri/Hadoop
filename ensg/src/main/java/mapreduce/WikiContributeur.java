package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.text.wikipedia.XmlInputFormat;

public class  WikiContributeur {

	public static class FirstTitleLetterMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private static final String START_DOC = "<text xml:space=\"preserve\">";
		private static final String END_DOC = "</text>";
		private static final Pattern TITLE = Pattern
				.compile("<title>(.*)<\\/title>");
		
		private static final Pattern CONTRIBUTOR = Pattern
				.compile("<contributor>(.*)<\\/contributor>");
		private static final Pattern IP = Pattern
				.compile("<ip>(.*)<\\/ip>");
		private static final Pattern USERNAME = Pattern
				.compile("<username>(.*)<\\/username>");

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String articleXML = value.toString();

			String title = getTitle(articleXML);
			String document = getDocument(articleXML);
			String contributeur = getContributor(articleXML);
			
			if(contributeur != null){
				context.write(new Text(contributeur), new IntWritable(1));
			}
			
		}

		private static String getDocument(String xml) {
			int start = xml.indexOf(START_DOC) + START_DOC.length();
			int end = xml.indexOf(END_DOC, start);
			return start < end ? xml.substring(start, end) : "";
		}

		private static String getTitle(CharSequence xml) {
			Matcher m = TITLE.matcher(xml);
			return m.find() ? m.group(1) : "";
		}
		
		private static String getContributor(CharSequence xml){
			Matcher m = CONTRIBUTOR.matcher(xml);
			Matcher m1 = IP.matcher(xml);
			Matcher m2 = USERNAME.matcher(xml);
			
			if(m.find() && m1.find()){
				m1.group(1);
								
			}else if(m.find() && m2.find()){
				m2.group(1);
			}
						
			return null;	
		}
	}

	public static class DocumentLengthSumReducer extends
			Reducer<Text, IntWritable, Text, LongWritable> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			long nbArticle = 0;
			long nbmax = 0;
			Text contributor = new Text();
			for (Text valeur : values) {
				//contributor = valeur;
				for(Text val : values){
					if(valeur ==  val){
						nbArticle += 1;
					}
				}
				if(nbmax < nbArticle){
					nbmax = nbArticle;
					contributor = valeur;
				}
			 
			}
			context.write(contributor, new LongWritable(nbmax));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		Job job = Job
				.getInstance(conf, "hatim-mohammed");
		job.setJarByClass(WikiFirstTitleLetterDocumentLengthSum.class);

		// Input / Mapper
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapperClass(FirstTitleLetterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Output / Reducer
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setReducerClass(DocumentLengthSumReducer.class);
		job.setNumReduceTasks(12);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

