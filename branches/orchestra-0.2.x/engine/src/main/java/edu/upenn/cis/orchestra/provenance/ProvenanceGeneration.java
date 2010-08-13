package edu.upenn.cis.orchestra.provenance;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.orchestra.datalog.atom.Atom;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleKeysException;
import edu.upenn.cis.orchestra.datamodel.exceptions.IncompatibleTypesException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.exchange.RuleFieldMapping;

public class ProvenanceGeneration {
	/**
	 * Takes a mapping and creates the corresponding provenance table.
	 * 
	 * BUG: fix the logic looking at keys, fkeys, and index creation.
	 * 
	 * @param mapping The input mapping
	 * @param ruleIndex The index used to create the mapping name ("M" + ruleIndex)
	 */
	public static ProvenanceRelation computeProvenanceRelation (final Mapping mapping, 
			final int ruleIndex) throws IncompatibleTypesException, IncompatibleKeysException
	{
		final List<String> indexFields = new ArrayList<String> ();
		List<RuleFieldMapping> rf;
		
		try {
			rf = mapping.getAppropriateRuleFieldMapping();
		} catch (RuntimeException re) {
			throw new RuntimeException("Error in computing mapping " + mapping.getId() + ":\n" + re.getMessage());
		}
		
		final List<RelationField> fields = new ArrayList<RelationField> ();
		for(RuleFieldMapping rfm : rf){
			fields.add(rfm.outputField);
			if(rfm.isIndex || rfm.srcColumns.size() > 1){
				indexFields.add(rfm.outputField.getName());
				StringBuffer srcFieldBuf = new StringBuffer();
				for(RelationField f : rfm.srcColumns){
					srcFieldBuf.append(f.getRelation().getName() + "." + f.getName() + " ");
				}
				
// ZI: BUG: Is this correct?  What we really need is that the mapping has a key in it...
				for(RelationField f : rfm.srcColumns){
					if(!f.getRelation().getPrimaryKey().getFields().contains(f)){
						throw new IncompatibleKeysException("Field: " + rfm.outputField.getName() + " (source: " + srcFieldBuf + ") is a key in the mapping:\n" + mapping.toString() +
								"\nbut not in the source (" + f.getRelation().getName() + "." + f.getName() + ")");
					}
				}
				for(RelationField f : rfm.trgColumns){
					if(!f.getRelation().getPrimaryKey().getFields().contains(f)){
						throw new IncompatibleKeysException("Field: " + rfm.outputField.getName() + " (source: " + srcFieldBuf + ") is a key in the body of the mapping:\n" + mapping.toString() +
								"\nbut not a subset of the key of the target (" + f.getRelation().getName() + "." + f.getName() + ")");
					}
				}
			}
		}
		
		String description = mapping.getDescription();

		try
		{
			final Relation r = mapping.getMappingHead().get(0).getRelation();
			final ProvenanceRelation rel = new ProvenanceRelation(r.getDbCatalog(), 
					r.getDbSchema(), 
					"M"+ruleIndex, "M"+ruleIndex, 
					description,
					true,
					fields,
					"M"+ruleIndex + "_PK",
					indexFields
			);
			boolean noNulls = true;
			
			List<Atom> temp = new ArrayList<Atom>();
			temp.addAll(mapping.getMappingHead());
			temp.addAll(mapping.getBodyWithoutSkolems());
			
			rel.deriveLabeledNullsFromAtoms(temp);
			
			// Copy nullability property from the head atoms
			int inx = 0;
			for (Atom a : mapping.getMappingHead()) {
				for (int inx2 = 0; inx2 < a.getValues().size(); inx2++)
					if (a.isNullable(inx2))
						rel.setIsNullable(inx);
					else
						rel.clearIsNullable(inx2);
				inx++;
			}
			
			List<Mapping> pm = new ArrayList<Mapping>();
			pm.add(mapping);
			rel.setMappings(pm);
        	ProvenanceRelation urel = ProvenanceRelation.createSingleProvRelSchema(rel);
        	
			return urel;
		}catch (final UnknownRefFieldException ex){
			ex.printStackTrace();
		}catch (final UnsupportedTypeException ex){
			ex.printStackTrace();
		}
		return null;
	}

}
