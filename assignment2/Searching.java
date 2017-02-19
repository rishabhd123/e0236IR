package e0236IR.assignment2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TreeSet;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class Searching {
	static int num_Topics = 40;
  public static void main(String[] args) throws Exception {
    
    String index = "indexR";		 
    String field = "contents";
    int hitsPerPage = 10;
       
    
    IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
    IndexSearcher searcher = new IndexSearcher(reader);
    Analyzer analyzer = new MyAnalyzer();
    
    //searcher.setSimilarity(new ClassicSimilarity());		//Classic Similarity
    //1.Start Retrieving number of Terms in vocabulary
   /* 
    Fields fields= MultiFields.getFields(reader);	//fetches various fields from the index (i.e fields that were indexed during indexing)
    int count=0;
       
    for(String f:fields){							//Iterated over all the fields
    	TermsEnum tIt= fields.terms(f).iterator();	//get all the terms belong to field f 
    	while(tIt.next()!=null) count++;			//count number of terms in field f
    }
    System.out.println(count);
    */
    //2.End
    
        
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    
    QueryParser parser = new QueryParser(field, analyzer);
    System.out.println("Enter query: ");
    String line = in.readLine();
    Query query = parser.parse(line);
    System.out.println("Searching for: " + query.toString(field));
    doPagingSearch(in, searcher, query, hitsPerPage, line);
    
    reader.close();
    
    
    
  }

  
  static void doPagingSearch(BufferedReader in, IndexSearcher searcher, Query query, 
                                     int hitsPerPage, String strQuery) throws IOException {
	
	
	int docsToBeRet = 100;
    // Collect enough docs to show 5 pages
    TopDocs results = searcher.search(query, 100);  //5 * hitsPerPage
    ScoreDoc[] hits = results.scoreDocs;
   //hits[i].score/results.getMaxScore(); //  important
    
    TreeSet<Emp1> treeset = new TreeSet<Emp1>(new MyComparator());
    int numTotalHits = results.totalHits;
    System.out.println(numTotalHits + " total matching documents");

    int start = 0;
    int end = Math.min(numTotalHits, docsToBeRet);   //Math.min(numTotalHits, hitsPerPage);
        
         
      Hashtable<String, String> T = topicGivenTermProb(strQuery);
      Hashtable<Integer, TreeSet<Emp1>> topicWiseRank = new Hashtable<Integer, TreeSet<Emp1>>();
      for(int j=0 ; j<num_Topics ; j++){
       	 topicWiseRank.put(j, new TreeSet<Emp1>(new MyComparator())); 
         }
      
      for (int i = start; i < end; i++) {
        
        Document doc = searcher.doc(hits[i].doc);
        
        String path = doc.get("path");
        if (path != null) {
          System.out.println((i+1) + ". " + path);
          String title = doc.get("title");
          if (title != null) {
            System.out.println("   Title: " + doc.get("title"));
          }
        } else {
          System.out.println((i+1) + ". " + "No path for this document");
        }                
        String[] docTopicsProb = doc.get("prob").split("\\s+");
        float tfidf  = hits[i].score/results.getMaxScore();
        
        Iterator<String> termIt = T.keySet().iterator();
        double t1 = 1.0;
        while(termIt.hasNext()){
        	String term = termIt.next();
        	String[] topicsListForTerm = T.get(term).split("\\s+");
        	int count = Integer.parseInt(topicsListForTerm[0]);
        	double t2=0.0;
        	for(int j=1; j<=count; j++){
        		int topic = Integer.parseInt(topicsListForTerm[j]);
        		t2 += Double.parseDouble(docTopicsProb[topic]);
        		
        	}
        	t1 *= (t2/count);
        
        }
        
        
        
        
        
        double finalScore = t1*tfidf;
        treeset.add( new Emp1(path, finalScore) );        
         
        termIt = T.keySet().iterator();
        t1 = 1.0;
        double[] probTopicGivQue = new double[40];
        for(int k=0 ; k<num_Topics ; k++){
        	probTopicGivQue[k] = 1.0;
        }
        
        while(termIt.hasNext()){
        	String term = termIt.next();
        	String[] topicsListForTerm = T.get(term).split("\\s+");
        	int count = Integer.parseInt(topicsListForTerm[0]);
        	for(int j=1; j<=count; j++){
        		int topic = Integer.parseInt(topicsListForTerm[j]);
        		probTopicGivQue[topic] /= count;         		
        	}
        	//t1 *= (t2/count);
        }
        
        for(int k=0 ;k<num_Topics ; k++){
        	double temp = probTopicGivQue[k];
        	if(temp<1){
        		double score = Double.parseDouble(docTopicsProb[k]) * temp;
        		TreeSet<Emp1> tempTree = topicWiseRank.get(k);
        		tempTree.add(new Emp1(path, score*tfidf));
        		//System.out.println(path);
        		topicWiseRank.put(k, tempTree );        		
        	}
        }
        
        
        
        
      }
      InputStream stream = Files.newInputStream(Paths.get("mallet_output/topic_keys.txt"));
      BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8 ));
      PrintWriter out1 = new PrintWriter("output2/topic_wise_doc.txt");
      String print = "";  // Initializing for storing topic wise ranked docs
      for(int p=0; p< num_Topics ; p++ ){
    	  String[] t = br.readLine().split("\\s+");
    	  String words = t[2]+" "+t[3]+" "+t[4]+" "+t[5]+" "+t[6];
    	  if(!topicWiseRank.get(p).isEmpty()){
    		  TreeSet<Emp1> temp = topicWiseRank.get(p);
    		  int n=5;
    		  Iterator<Emp1> it = temp.iterator();
    		  Emp1 e;
    		  print += p + " "+words+"\n";
    		  //System.out.println(p + " "+words);
    		  while ( n!=0 && it.hasNext() ){
    			  e = it.next();
    			  print += e.docId + "\n";
    			  //System.out.println(e.docId);
    			  n--;
    			  
    		  }
    	  }
      }
      
      out1.write(print);
      out1.close();
      stream.close();
      br.close();
      
      
      print  = ""; //Re initializing for storing docs ranked by new ranking scheme
      
      for(Emp1 e:treeset) {
    	  print += e.docId + "\n";
    	  //System.out.println(e.docId);
      }
      
      PrintWriter out2 = new PrintWriter("output2/Top100doc.txt");  //output top 100 dics acc to new ranking scheme
      out2.print(print);
      out2.close();
    	  
    	     
      
      
  }
  
  
  static Hashtable<String, String> topicGivenTermProb(String query) throws IOException{
	  
	  String[] terms = query.toLowerCase().split("\\s+");
	  InputStream stream;
	  BufferedReader br;
	  Hashtable<String, String> T = new  Hashtable<>();
	  
	  String topicsRelevantToQuery = "";
	  
	  for(String s:terms){
		  int count=0;
		  stream = Files.newInputStream(Paths.get("mallet_output/topic_keys.txt"));
		  br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
		  String topicList = "";
		  for(int i=0; i < num_Topics ; i++ ){
			  String topic = br.readLine();
			  if(topic.contains(s+" ") || topic.contains(" "+s)){
				  count++;				  
				  topicList += " " + i;
				  if(!topicsRelevantToQuery.contains(i + ""))	topicsRelevantToQuery += i + "\n";
			  }
			  
		  }
		  
		  if(count!=0){
			  topicList = count + " " + topicList;
			  T.put(s, topicList);
		  }
		  
		  br.close();
		  stream.close();
		  
	  }
	  
	  
	  PrintWriter out3 = new PrintWriter("output2/top_k_topics.txt");
	  
	  //System.out.println("Topics Relevant to "+ query + "\n" + topicsRelevantToQuery);
	  
	  out3.print(topicsRelevantToQuery);
	  out3.close();
	  System.out.println("Topics Relevant to "+ query + "\n" + topicsRelevantToQuery);
	  return T;
  }
  
     
}

class MyComparator implements Comparator<Emp1>{

	@Override
	public int compare(Emp1 e1, Emp1 e2) {
		if(e1.score < e2.score) return 1;		
		else return -1;
	}
	
}

class Emp1{
	public String docId;
	public double score;
	public Emp1(String doc, double sc) {
		docId = doc;
		score = sc;
	}
}
