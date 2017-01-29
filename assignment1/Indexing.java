package e0236IR.assignment1;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
//Java Essentials
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import org.json.simple.*;
import org.json.simple.parser.*;

//Lucene Packages
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


public class Indexing 
{
	
	public static void main(String[] args) throws IOException
	{	
		
		String docLocation = "/home/rishabh/workspace/E0236/conda root/Text";	//Location of directory which contains the files to be indexed
		String indexLocation = "/home/rishabh/workspace/E0236/indexR/";
		Path docDir = Paths.get(docLocation);
		Directory iDir = FSDirectory.open(Paths.get(indexLocation)); 		//Creating Directory for storing index
		Analyzer analyzer  = new MyAnalyzer();						//Selecting a particular type of analyzer to parse documents
		IndexWriterConfig indexConfig = new IndexWriterConfig(analyzer);	//Contains the configuration of IndexWriter object(which is to be created)
		
		indexConfig.setOpenMode(OpenMode.CREATE);    						//with this configuration IndexWriter object will create a new Index
		//indexConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);					//--------""-----------------
		
		IndexWriter iWriter = new IndexWriter(iDir, indexConfig); 			//Create an IndexWrite object with given configuration "indexConfig"
																			//iDir is the directory object of the location of index
		System.out.println("Started");
		indexDocuments(iWriter, docDir);
		System.out.println("Finished Succussfully");
		iWriter.close();
		
		
	}
	
	
	static void indexDocuments(IndexWriter writer, Path docDir) throws IOException
	{
		if(Files.isDirectory(docDir)){
			Files.walkFileTree(docDir, new SimpleFileVisitor<Path>() {
		        @Override
		        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
		          try {
		            indexDocuments(writer, file, attrs.lastModifiedTime().toMillis());
		          } catch (IOException ignore) {
		            // don't index files that can't be read.
		          }
		          return FileVisitResult.CONTINUE;
		        }
		      });
			
		}
		else indexDocuments(writer, docDir, Files.getLastModifiedTime(docDir).toMillis());		//here docDir is not a directory but it is a path to the json file			
		
	}
	
	
	static void indexDocuments(IndexWriter writer, Path docDir, long lastModified) throws IOException		//Indexes a single document
	{	//Will enter here only when "docDir" refers to a file(not a directory)
		
		
		Document doc = new Document();
		
		String title=null;
		String content=null;
		try {
			//Parsing .json file to retrieve the content and title
			JSONParser jparser = new JSONParser();
			JSONObject jobj = (JSONObject)jparser.parse(new FileReader(docDir.toString()));
			title = ((JSONObject)jobj.get("parse")).get("title").toString();
			content = ((JSONObject)jobj.get("parse")).get("wikitext").toString();
			
		} catch (Exception e) {
			System.out.println("Exception in jsonparser");
		}
		
		
		//System.out.println(docDir.toString());
		
		Field pathField = new StringField("path", docDir.toString(), Field.Store.YES );
		doc.add(pathField);
		
		Field titleField = new StringField("title", title, Field.Store.YES );
		doc.add(titleField);
		
		Field time = new LongPoint("modified",lastModified);
		doc.add(time);
		
		//**IMP** Part Read the comment
		InputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));//*IMP* When i index the content as default string 
		//--the size of index directory was very big because default string uses UTF-16 encoding so i just converted that string to InputStream Object
		//--and created the index by first converting the string to UTF-8 encoding.Also we cant convert string to UTF-8 encoded string preserving give string
		//--as default String.
		Field text = new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)));
		doc.add(text);
		
		
		
		if(writer.getConfig().getOpenMode() == OpenMode.CREATE)			//create new index
		{
			//System.out.println("adding " + docDir);
			writer.addDocument(doc);
			
		}
		else 															//create or update index
			writer.updateDocument(new Term("path", docDir.toString()), doc);
			//writer.deleteDocuments(new Term("path",docDir.toString()));		
	}
	

}



	