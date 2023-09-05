package org.jax.mgi.searchInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * ReferenceSearchInfo
 * @author mhall
 * This object represents the searchable parts of a reference from a given indexes perspective.
 * 
 * For an index that uses this object its expected that every record that uses this reference will need
 * all of the reference information contained in this object.
 * 
 */

public class ReferenceSearchInfo {
	
	private List<String> authors = new ArrayList<String> ();
	private List<String> firstAuthors = new ArrayList<String> ();
	private List<String> lastAuthors = new ArrayList<String> ();
	private List<String> authorsFacet = new ArrayList<String> ();
	private String journal;
	private List<String> journalFacet = new ArrayList<String> ();
	private String year;
	private String titleStemmed;
	private String titleUnstemmed;
	private String abstractStemmed;
	private String abstractUnstemmed;
	private String titleAbstractStemmed;
	private String titleAbstractUnstemmed;
	
	public void addAuthor (String author) {
		this.authors.add(author);
	}
	public void addAuthorFacet (String author) {
		this.authorsFacet.add(author);
	}

	public void addFirstAuthor (String author) {
		this.firstAuthors.add(author);
	}
	
	public void addJournalFacet (String journal) {
		this.journalFacet.add(journal);
	}
	
	public void addLastAuthor (String author) {
		this.lastAuthors.add(author);
	}	
	
	public String getAbstractStemmed() {
		return abstractStemmed;
	}
	public String getAbstractUnstemmed() {
		return abstractUnstemmed;
	}
	public List<String> getAuthors() {
		return authors;
	}
	
	public List<String> getAuthorsFacet() {
		return authorsFacet;
	}
	public List<String> getFirstAuthor() {
		return firstAuthors;
	}
	public String getJournal() {
		return journal;
	}
	public List<String> getJournalFacet() {
		return journalFacet;
	}
	public List<String> getLastAuthor() {
		return lastAuthors;
	}
	public String getTitleAbstractStemmed() {
		return titleAbstractStemmed;
	}
	public String getTitleAbstractUnstemmed() {
		return titleAbstractUnstemmed;
	}
	public String getTitleStemmed() {
		return titleStemmed;
	}
	public String getTitleUnstemmed() {
		return titleUnstemmed;
	}
	public String getYear() {
		return year;
	}
	public void setAbstractStemmed(String abstractStemmed) {
		this.abstractStemmed = abstractStemmed;
	}
	public void setAbstractUnstemmed(String abstractUnstemmed) {
		this.abstractUnstemmed = abstractUnstemmed;
	}
	public void setAuthors(List<String> authors) {
		this.authors = authors;
	}
	public void setAuthorsFacet(List<String> authorsFacet) {
		this.authorsFacet = authorsFacet;
	}
	public void setFirstAuthor(List<String> firstAuthor) {
		this.firstAuthors = firstAuthor;
	}
	public void setJournal(String journal) {
		this.journal = journal;
	}
	public void setJournalFacet(List<String> journalFacet) {
		this.journalFacet = journalFacet;
	}
	public void setLastAuthor(List<String> lastAuthor) {
		this.lastAuthors = lastAuthor;
	}
	public void setTitleAbstractStemmed(String titleAbstractStemmed) {
		this.titleAbstractStemmed = titleAbstractStemmed;
	}
	public void setTitleAbstractUnstemmed(String titleAbstractUnstemmed) {
		this.titleAbstractUnstemmed = titleAbstractUnstemmed;
	}
	public void setTitleStemmed(String titleStemmed) {
		this.titleStemmed = titleStemmed;
	}
	public void setTitleUnstemmed(String titleUnstemmed) {
		this.titleUnstemmed = titleUnstemmed;
	}
	public void setYear(String year) {
		this.year = year;
	}
	@Override
	public String toString() {
		return "ReferenceSearchInfo [authors=" + authors + ", firstAuthor="
				+ firstAuthors + ", lastAuthor=" + lastAuthors + ", journal="
				+ journal + ", journalFacet=" + journalFacet + ", year=" + year
				+ ", titleStemmed=" + titleStemmed + ", titleUnstemmed="
				+ titleUnstemmed + ", abstractStemmed=" + abstractStemmed
				+ ", abstractUnstemmed=" + abstractUnstemmed
				+ ", titleAbstractStemmed=" + titleAbstractStemmed
				+ ", titleAbstractUnstemmed=" + titleAbstractUnstemmed + "]";
	}
	
	
	

}
