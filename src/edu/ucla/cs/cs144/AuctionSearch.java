package edu.ucla.cs.cs144;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

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
import java.util.LinkedHashMap;
import java.util.*;

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
		
		searcher = new IndexSearcher(System.getenv("LUCENE_INDEX") + "/index1");
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
			int numResultsToSkip, int numResultsToReturn) throws CorruptIndexException, IOException,
      ParseException, SQLException {
    parser = null;
    searcher = new IndexSearcher(System.getenv("LUCENE_INDEX") + "/index1");
    LinkedHashMap<String, String> results = null;
    for(int i = 0; i < constraints.length; i++)
    {
      if(constraints[i].getFieldName().equals("ItemName"))
      {
        parser = new QueryParser("Name", new StandardAnalyzer());
      }
      else if(constraints[i].getFieldName().equals("Category"))
      {
        parser = new QueryParser("Category", new StandardAnalyzer());
      }
      else if(constraints[i].getFieldName().equals("Description"))
      {
        parser = new QueryParser("Description", new StandardAnalyzer());
      }

      if(parser != null)
      {
        Query query = parser.parse(constraints[i].getValue());
        Hits hits = searcher.search(query);
        if(results == null)
        {
          results = new LinkedHashMap<String, String>(((int)(hits.length()/.75))+1);
          for(int j = 0; j < hits.length(); j++)
          {
            Document doc = hits.doc(j);
            results.put(doc.get("ItemId"), doc.get("Name"));
          }
        }
        else
        {
          LinkedHashMap<String, String> temp = new LinkedHashMap(((int)(hits.length()/.75))+1);
          
          for(int j = 0; j < hits.length(); j++)
          {
            Document doc = hits.doc(j);
            if(results.containsKey(doc.get("ItemId")))
            {
              temp.put(doc.get("ItemId"), doc.get("Name"));
            }
          }
          results = temp;
        }
        parser = null;
      }
    }

    Connection con = DbManager.getConnection(true);

    PreparedStatement stmt = null;
    for(int i = 0; i < constraints.length; i++)
    {
      if(constraints[i].getFieldName().equals("SellerId"))
      {
        stmt = con.prepareStatement("SELECT ItemId, Name FROM Item WHERE Seller=" + constraints[i].getValue());
      }
      else if(constraints[i].getFieldName().equals("BuyPrice"))
      {
        stmt = con.prepareStatement("SELECT ItemId, Name FROM Item WHERE BuyPrice=" + constraints[i].getValue());
      }
      else if(constraints[i].getFieldName().equals("BidderId"))
      {
        stmt = con.prepareStatement("SELECT I.ItemId, I.Name FROM Bid AS B INNER JOIN Item AS I ON I.ItemId=B.ItemId WHERE Bidder=\"" + constraints[i].getValue() + "\"");
      }
      else if(constraints[i].getFieldName().equals("EndTime"))
      {
        stmt = con.prepareStatement("SELECT ItemId, Name FROM Item WHERE Ends=\"" + formatDate(constraints[i].getValue()) + "\"");
      }

      if(stmt != null)
      {
        ResultSet rs = stmt.executeQuery();
        if(results == null)
        {
          rs.last();
          results = new LinkedHashMap<String, String>(((int)(rs.getRow()/.75))+1);
          rs.beforeFirst();
          while(rs.next())
          {
            results.put(rs.getString("ItemId"), rs.getString("Name"));
          }
        }
        else
        {
          rs.last();
          LinkedHashMap<String, String> temp = new LinkedHashMap<String, String>(((int)(rs.getRow()/.75))+1);
          rs.beforeFirst();
          while(rs.next())
          {
            if(results.containsKey(rs.getString("ItemId")))
            {
              temp.put(rs.getString("ItemId"), rs.getString("Name"));
            }
          }
          results = temp;
        }
        stmt = null;
      }
    }

    SearchResult[] r = new SearchResult[results.size()];
    int i = 0;
    for(Map.Entry<String, String> entry : results.entrySet())
    {
      SearchResult temp = new SearchResult(entry.getKey(), entry.getValue());
      r[i] = temp;
      i++;
    }
    
		return r;
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		return null;
	}
	
	public String echo(String message) {
		return message;
	}

  private static String formatDate(String indate)
  {
    SimpleDateFormat informat = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
    SimpleDateFormat outformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try {
      Date started = informat.parse(indate);
      return outformat.format(started);
    } catch(Java.Text.ParseException a) {
      System.err.println("Caught IOException: " + a.getMessage());
    }
    System.exit(1);
    return "SHOULD NEVER DO THIS";
  }


}
