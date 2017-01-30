package e0236IR.assignment1;

//Java Essentials
import java.io.FileReader;
import java.io.IOException;
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
//import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class Indexing 
{
	
	public static void main(String[] args) throws IOException
	{	
		
		String docLocation = "/home/rishabh/workspace/E0236/conda root/Text";	//Location of directory which contains the files to be indexed
		String indexLocation = "indexR";
		Path docDir = Paths.get(docLocation);
		Directory iDir = FSDirectory.open(Paths.get(indexLocation)); 		//Creating Directory for storing index
		Analyzer analyzer  = new MyAnalyzer();						//Selecting a particular type of analyzer to parse documents
		IndexWriterConfig indexConfig = new IndexWriterConfig(analyzer);	//Contains the configuration of IndexWriter object(which is to be created)
		
		
		indexConfig.setOpenMode(OpenMode.CREATE);    						//with this configuration IndexWriter object will create a new Index
		//indexConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);					//--------""-----------------
		//indexConfig.setSimilarity(new ClassicSimilarity());
		
		
		IndexWriter iWriter = new IndexWriter(iDir, indexConfig); 			//Create an IndexWrite object with given configuration "indexConfig"
		//System.out.println(iWriter.getConfig().getSimilarity().toString());															//iDir is the directory object of the location of index
		System.out.println("Indexing");
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
		
		
		Field text = new TextField("contents", content, Field.Store.NO);
		//Field text = new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)));
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



	