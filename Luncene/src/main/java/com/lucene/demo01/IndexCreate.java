package com.lucene.demo01;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * Created by LDDFY on 2017/1/5.
 */
public class IndexCreate {

    public static void main(String[] args) {
        Analyzer analyzer = new StandardAnalyzer();
        Directory directory = null;
        try {

            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            directory = FSDirectory.open(Paths.get("D://lucene/index/test"));
            IndexWriter iwriter = new IndexWriter(directory, config);


            Document doc = new Document();
            doc.add(new Field("fieldname", "This is the text to be indexed.", TextField.TYPE_STORED));
            doc.add(new Field("id", "this is lucene text test", TextField.TYPE_STORED));

            iwriter.addDocument(doc);
            iwriter.commit();


            // Now search the index:
            DirectoryReader ireader = DirectoryReader.open(directory);
            IndexSearcher isearcher = new IndexSearcher(ireader);

            // Parse a simple query that searches for "text":
            QueryParser parser = new QueryParser("fieldname", analyzer);
            Query query = parser.parse("text");
            ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
            // assertEquals(3, hits.length);

            // Iterate through the results:
            for (int i = 0; i < hits.length; i++) {
                Document hitDoc = isearcher.doc(hits[i].doc);
                assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
                System.out.println(hitDoc.get("fieldname"));
            }
            ireader.close();
            directory.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
