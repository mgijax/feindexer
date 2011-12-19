package org.jax.mgi.searchInfo;

/* This used to be an age/assayType pair, but it now has their full-coded
 * equivalents, so it's really more of a quintet.
 */
public class GxdLitAgeAssayTypePair {
	
	private String age = null;
	private String assayType = null;
	private Integer minTheilerStage = null;
	private Integer maxTheilerStage = null;
	private String fullCodedAssayType = null;
	
	public String getAge() {
		return age;
	}
	public String getAssayType() {
		return assayType;
	}
	public String getFullCodedAssayType() {
		return fullCodedAssayType;
	}
	public Integer getMinTheilerStage() {
		return minTheilerStage;
	}
	public Integer getMaxTheilerStage() {
		return maxTheilerStage;
	}
	public void setAge(String age) {
		this.age = age;
	}
	public void setAssayType(String assayType) {
		this.assayType = assayType;
	}
	public void setFullCodedAssayType(String fullCodedAssayType) {
		this.fullCodedAssayType = fullCodedAssayType;
	}
	public void setMinTheilerStage(Integer minTheilerStage) {
		this.minTheilerStage = minTheilerStage;
	}
	public void setMaxTheilerStage(Integer maxTheilerStage) {
		this.maxTheilerStage = maxTheilerStage;
	} 
}
