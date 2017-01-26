package e0236IR.assignment1;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
//import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;

public class MyAnalyzer extends Analyzer{		//By default class Analyzer doesn't perform stemming.So i override createComponents() method
	@Override							//to implement PorterStemFilter
	protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new LowerCaseTokenizer();
        return new TokenStreamComponents(source, new PorterStemFilter(source));
      }
}
