package edu.ucla.cs.cs144;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.text.SimpleDateFormat;

import edu.ucla.cs.cs144.DbManager;
import edu.ucla.cs.cs144.SearchConstraint;
import edu.ucla.cs.cs144.SearchResult;

public class AuctionSearch implements IAuctionSearch {

	/* 
         * You will probably have to use JDBC to access MySQL data
         * Lucene IndexSearcher class to lookup Lucene index.
         * Read the corresponding tutorial to learn about how to use these.
         *
         * Your code will need to reference the directory which contains your
	 * Lucene index files.  Make sure to read the environment variable 
         * $LUCENE_INDEX with System.getenv() to build the appropriate path.
	 *
	 * You may create helper functions or classes to simplify writing these
	 * methods. Make sure that your helper functions are not public,
         * so that they are not exposed to outside of this class.
         *
         * Any new classes that you create should be part of
         * edu.ucla.cs.cs144 package and their source files should be
         * placed at src/edu/ucla/cs/cs144.
         *
         */
	
	private IndexSearcher searcher = null;
	private QueryParser parser = null;
	
	@SuppressWarnings({"deprecation", "unchecked"})
	public SearchResult[] basicSearch(String query, int numResultsToSkip, 
			int numResultsToReturn) throws CorruptIndexException, IOException, ParseException {
		
		ArrayList<SearchResult> results = new ArrayList<SearchResult>();
		
		searcher = new IndexSearcher("null/index1");
		parser = new QueryParser("Content", new StandardAnalyzer());
	
		Query q = parser.parse(query);
		Hits hits = searcher.search(q);
		
		System.out.println("total items: " + hits.length());
		
		for(int i = 0; i < hits.length(); i++) {
		  Document doc = hits.doc(i);
		  
		  SearchResult s = new SearchResult();
		  s.setItemId(doc.get("ItemId"));
		  s.setName(doc.get("Name"));
		  
		  System.out.println(s.getItemId());
		  
		  results.add(s);
		}
		
		SearchResult[] r = {};
		r = results.toArray(r);
		return r;
	}

	public SearchResult[] advancedSearch(SearchConstraint[] constraints, 
			int numResultsToSkip, int numResultsToReturn) {
    searcher = new IndexSearcher("LUCENE_INDEX");
    for(int i = 0; i < constraints.length; i++)
    {
      if(constraints[i].fieldName.equals("ItemName"))
      {
        parser = new QueryParser("Name", new StandardAnalyzer());
        Query query = parser.parse(constraints[i].value);
        Hits hits = searcher.search(query);
      }
      else if(constraints[i].fieldName.equals("Category"))
      {

      }
      else if(constraints[i].fieldName.equals("Description"))
      {

      }
    }
		// TODO: Your code here!
		return new SearchResult[0];
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		return null;
	}
	
	public String echo(String message) {
		return message;
	}

}
