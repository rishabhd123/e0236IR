package e0236IR.assignment1;

//Java Essentials
import java.io.BufferedReader;
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

//Lucene Packages
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
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

public class Indexing {
	
	public static void main(String[] args) throws IOException
	{
		String docLocation = "/home/rishabh/workspace/E0236/conda root/Text";	//Location of directory which contains the files to be indexed
		String indexLocation = "/home/rishabh/workspace/E0236/indexR/";
		Path docDir = Paths.get(docLocation);
		Directory iDir = FSDirectory.open(Paths.get(indexLocation)); 		//Creating Directory for storing index
		Analyzer analyzer  = new StandardAnalyzer();						//Selecting a particular type of analyzer to parse documents
		IndexWriterConfig indexConfig = new IndexWriterConfig(analyzer);	//Contains the configuration of IndexWriter object(which is to be created)
		
		indexConfig.setOpenMode(OpenMode.CREATE);    						//with this configuration IndexWriter object will create a new Index
		//indexConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);					//--------""-----------------
		
		IndexWriter iWriter = new IndexWriter(iDir, indexConfig); 			//Create an IndexWrite object with given configuration "indexConfig"
																			//iDir is the directory object of the location of index
		indexDocuments(iWriter, docDir);
		System.out.println("Finished Succussfully");
		
		
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
		else indexDocuments(writer, docDir, Files.getLastModifiedTime(docDir).toMillis());
			
		
	}
	
	
	static void indexDocuments(IndexWriter writer, Path docDir, long lastModified) throws IOException		//Indexes a single document
	{	//Will enter here only when "docDir" refers to a file(not a directory)
		InputStream stream = Files.newInputStream(docDir);					//Stream of a Document(file)
		Document doc = new Document();
		Field pathField = new StringField("path", docDir.toString(), Field.Store.YES );
		doc.add(pathField);
		
		Field time = new LongPoint("modified",lastModified);
		doc.add(time);
		
		Field text = new TextField("content", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)));
		doc.add(text);
		
		if(writer.getConfig().getOpenMode() == OpenMode.CREATE)			//create new index
		{
			//System.out.println("adding " + docDir);
			writer.addDocument(doc);
		}
		else 															//create or update index
			writer.updateDocument(new Term("path", docDir.toString()), doc);	
		
		
		
	
	}
	

}
