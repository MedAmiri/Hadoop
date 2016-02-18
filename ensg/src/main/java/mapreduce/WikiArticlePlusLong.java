package mapreduce;

	import java.io.IOException;
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

	public class WikiArticlePlusLong {

		public static class FirstTitleLetterMapper extends
				Mapper<Object, Text, Text, Text> {

			private static final String START_DOC = "<text xml:space=\"preserve\">";
			private static final String END_DOC = "</text>";
			private static final Pattern TITLE = Pattern
					.compile("<title>(.*)<\\/title>");

			public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException {

				String articleXML = value.toString();

				String title = getTitle(articleXML);
				String document = getDocument(articleXML);

				if (title.length() > 0) {
					context.write(new Text(""), new Text (title +"|||"+
							document.length()));
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

		}

		public static class DocumentLengthSumReducer extends
				Reducer<Text, IntWritable, Text, LongWritable> {

			public void reduce(Text key, Iterable<Text> values,
					Context context) throws IOException, InterruptedException {

				long maxLength = 0;
				Text titreActicleMax = new Text("");
				for (Text document : values) {
					String[] chaine = document.toString().split("\\|\\|\\|");
					int maxtest = Integer.parseInt(chaine[1]);
					if(maxLength < maxtest){
						maxLength = maxtest;
						titreActicleMax.set(chaine[0]);
					}
				}
				context.write(titreActicleMax , new LongWritable(maxLength));
			}
		}

		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
			conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

			Job job = Job
					.getInstance(conf, "WikiFirstTitleLetterDocumentLengthSum");
			job.setJarByClass(WikiFirstTitleLetterDocumentLengthSum.class);

			// Input / Mapper
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(XmlInputFormat.class);
			job.setMapperClass(FirstTitleLetterMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

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

