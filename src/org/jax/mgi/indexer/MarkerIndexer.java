/*package org.jax.mgi.Indexer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import mgi.frontend.datamodel.Marker;
import mgi.frontend.datamodel.MarkerID;
import mgi.frontend.datamodel.MarkerLocation;
import mgi.frontend.datamodel.Reference;

import org.apache.solr.common.SolrInputDocument;
import org.hibernate.Query;
import org.hibernate.Transaction;

public class MarkerIndexer extends Indexer {

    public MarkerIndexer (String httpConnection) {
        super(httpConnection);
    }
    
    
    public static void main(String args[]) {
        MarkerIndexer mi = new MarkerIndexer("http://cardolan.informatics.jax.org:8983/solr/solr-marker/");
        mi.doChunks(); 
    }
    
    private void doChunks() {
        
        try {
            Transaction tx = ses.beginTransaction();

            Integer divValue = 20000;
            
            Query q = ses.createQuery("select count(*) from Marker");
            Long foo = (Long) q.uniqueResult();
            int modValue = foo.intValue() / divValue;
            
            System.out.println("Total count:" + foo);
            System.out.println("Modula value:" + modValue);
            
            for (int i=0;i <= foo.intValue() / divValue; i++) {
//            for (int i=0;i <= 1; i++) {                
                List <Marker> results = ses.createQuery("from mgi.frontend.datamodel.Marker").setFirstResult(i*divValue).setMaxResults(divValue).list();
                
                System.out.println("On Pass: " + i);
                
                Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
                
//                System.out.println("Starting at: " + i*10000);
                
                for (Marker m: results) {
                    SolrInputDocument doc = new SolrInputDocument();
                    doc.addField("Marker_key", m.getMarkerKey());
                    doc.addField("mgiid", m.getPrimaryID());
                    doc.addField("Symbol", m.getSymbol());
                    doc.addField("Type", m.getMarkerType());
                    doc.addField("Synonyms", r.get);
                    doc.addField("abstract", r.getAbstract());
                    doc.addField("issue", r.getIssue());
                    doc.addField("volume", r.getVol());
                    
                    List <Reference> refList = m.getReferences();
                    for (Reference ref: refList) {
                        doc.addField("ref_key", ref.getReferenceKey());
                    }
                    
                    try {                    
                            Set <MarkerID> mlist = m.getIds();
                            for (MarkerID mid: mlist) {
                                doc.addField("Secondary_MGI_IDs", mid.getAccID());
                            }
                        }
                    catch (Exception e) {}
                    try {                    
                        List <MarkerLocation> mloclist = m.getLocations();
                        for (MarkerLocation mid: mloclist) {
                            doc.addField("Start_ti", mid.getStartCoordinate());
                            doc.addField("End_ti", mid.getEndCoordinate());
                            doc.addField("Chr", mid.getChromosome());
                        }
                    }
                catch (Exception e) {}

                    
                    docs.add(doc);
                }
                System.out.println("Exporting to Solr");
                server.add(docs);
                System.out.println("Moving on");
            }
            ses.clear();
            server.commit();
            System.out.println("We committed!");
        } catch (Exception e) {e.printStackTrace();}
        
        ses.close();
        
    }
}
*/