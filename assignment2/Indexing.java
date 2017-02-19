package e0236IR.assignment2;

import java.io.BufferedReader;
//Java Essentials
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;


//Lucene Packages
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
//import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class Indexing 
{
	static int num_Topics = 40;
	public static void main(String[] args) throws IOException
	{	
		
		//String docLocation = "bbc";	//Location of directory which contains the files to be indexed
		String indexLocation = "indexR";
		//Path docDir = Paths.get(docLocation);
		Directory iDir = FSDirectory.open(Paths.get(indexLocation)); 		//Creating Directory for storing index
		Analyzer analyzer  = new MyAnalyzer();						//Selecting a particular type of analyzer to parse documents
		IndexWriterConfig indexConfig = new IndexWriterConfig(analyzer);	//Contains the configuration of IndexWriter object(which is to be created)
		
		
		indexConfig.setOpenMode(OpenMode.CREATE);    						//with this configuration IndexWriter object will create a new Index
		//indexConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);					//--------""-----------------
		//indexConfig.setSimilarity(new ClassicSimilarity());
		
		
		IndexWriter iWriter = new IndexWriter(iDir, indexConfig); 			//Create an IndexWrite object with given configuration "indexConfig"
		//System.out.println(iWriter.getConfig().getSimilarity().toString());															//iDir is the directory object of the location of index
		System.out.println("Indexing");
		indexViaIndexTopicDoc(iWriter);
		System.out.println("Finished Succussfully");
		iWriter.close();
	}
	
			
	
	static void indexViaIndexTopicDoc(IndexWriter writer) throws IOException {
		
				
		InputStream doc_topic_stream = Files.newInputStream(Paths.get("mallet_output/doc_topics.txt"));
		BufferedReader br = new BufferedReader(new InputStreamReader(doc_topic_stream, StandardCharsets.UTF_8));
		
		int num_docs = 2226;    //careful indexing is from zero  
		double[] topic_prob_sum = new double[40];   //Sum of prob. for each topic(i.e sum( p(D_i | c) ))   i:for each document
		String line, docDir;
		
		for(int i=0;i<num_docs;i++){				//generating conditional probability sum( p(D_i | c) ) for each c(topic)
			line = br.readLine();
			String[] lineCompo = line.split("\\s+");			
			for(int j=0;j<num_Topics;j++){
				topic_prob_sum[j] += Double.parseDouble(lineCompo[j+2]);
				}			
		}
		doc_topic_stream.close();
		
		//*** Very Important: why there is a need to initialize the InputStream again?  
		doc_topic_stream = Files.newInputStream(Paths.get("mallet_output/doc_topics.txt"));
		br = new BufferedReader(new InputStreamReader(doc_topic_stream, StandardCharsets.UTF_8));
		for(int i=0;i<num_docs;i++){
			line = br.readLine();
			//System.out.println(line);
			String[] lineCompo = line.split("\\s+");
			docDir = lineCompo[1].substring(5);
			String doc_topic_prob = "";
			for(int j=0;j<num_Topics;j++)
				 doc_topic_prob += " " + String.valueOf( Double.parseDouble(lineCompo[j+2])/topic_prob_sum[j] );
				
			doc_topic_prob = doc_topic_prob.trim();
			
			
			Document doc = new Document();
			
			Field pathField = new StringField("path", docDir, Field.Store.YES );
			doc.add(pathField);
			
			Field probField = new StringField("prob", doc_topic_prob, Field.Store.YES);	// p(D|c_i) Adding String containing probability of doc D given c_i for every i 
			doc.add(probField);
			//System.out.println(docDir);			
			InputStream stream = Files.newInputStream(Paths.get(docDir));
			Field text = new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)));
			doc.add(text);
					
			
			
			if(writer.getConfig().getOpenMode() == OpenMode.CREATE)			//create new index
			{
				//System.out.println("adding " + docDir);
				writer.addDocument(doc);
				
				
			}
			else 															//create or update index
			{
				writer.updateDocument(new Term("path", docDir.toString()), doc);
				//writer.deleteDocuments(new Term("path",docDir.toString()));		
				
			}
			
		}
		
		
	}
	

}



	