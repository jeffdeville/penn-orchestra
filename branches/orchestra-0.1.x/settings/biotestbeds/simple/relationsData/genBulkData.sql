set serveroutput on size 10000;
set echo off;
set feedback off;
set linesize 600;
    
spool outFiles/pcbi_plasmo_genefeat.txt;


DECLARE

   CURSOR CUR_DATA 
   IS
		SELECT gf.source_id id,
		       gf.product name,
			   dr.primary_identifier AS RefSeqAcc
		  FROM 
		  	   PLASMODB.TEMPEXPORTDB2 tmp,
		  	   dots.GeneFeature gf,
		  	   dots.Transcript t,
			   dots.DbRefNaFeature drnf,
			   sres.DbRef dr,
			   sres.ExternalDatabaseRelease edr,
			   sres.ExternalDatabase ed	
		 WHERE
		       tmp.source_id = gf.source_id 
		   AND gf.na_feature_id = t.parent_id
		   AND gf.na_feature_id = drnf.na_feature_id
		   AND drnf.db_ref_id = dr.db_ref_id
		   AND dr.external_database_release_id
		                        = edr.external_database_release_id
		   AND edr.external_database_id = ed.external_database_id
		   AND ed.name = 'NRDB_ref_dbXRefBySeqIdentity'  
			;   

BEGIN
  FOR i IN CUR_DATA LOOP
  	DBMS_OUTPUT.PUT_LINE (i.Id || '|' ||  i.name || '|' || i.RefSeqAcc ||'|');
  END LOOP;
END;
/

spool off;      



spool outFiles/pcbi_plasmo_genetax.txt;


DECLARE
   CURSOR CUR_DATA IS
			SELECT gf.source_id Id,
				   t.NCBI_TAX_ID taxid
			  FROM dots.GeneFeature gf,
				   dots.NaSequence s, 
				   sres.taxon t,
			  	 PLASMODB.TEMPEXPORTDB2 tmp
			 WHERE
			       tmp.source_id = gf.source_id 
			   AND gf.na_sequence_id = s.na_sequence_id
			   AND s.taxon_id = t.taxon_id;   

BEGIN
  FOR i IN CUR_DATA LOOP
  	DBMS_OUTPUT.PUT_LINE (i.Id || '|' || i.taxid || '|');
  END LOOP;
END;
/

spool off;


spool outFiles/pcbi_plasmo_domainref.txt;

DECLARE
   CURSOR CUR_DATA IS
			SELECT 
			  d.source_id,
			  d.db,    
			  d.dbaccession dbacc
			FROM 
			  domain_references d,
			  PLASMODB.TEMPEXPORTDB2 tmp
			WHERE
			  d.source_id = tmp.source_id
			  AND d.db = 'Interpro';

BEGIN
  FOR i IN CUR_DATA LOOP
  	DBMS_OUTPUT.PUT_LINE (i.source_id || '|' || i.db || '|' || i.dbacc|| '|');
  END LOOP;
END;
/

spool off;



spool outFiles/ncbi_refseq_catalog.txt;

DECLARE
   CURSOR CUR_DATA IS
			SELECT DISTINCT c.gi,
			       c.accession acc,
				   c.tax_id,
				   'SEQ' ||  tmp.source_id seq 
			  FROM
			       PLASMODB.TEMPEXPORTDB2 tmp,
			  	   catalog@refseq c, 
				   dots.GeneFeature gf,
				   dots.DbRefNaFeature drnf,
				   sres.DbRef dr,
				   sres.ExternalDatabaseRelease edr,
				   sres.ExternalDatabase ed
			 WHERE
			       tmp.source_id = gf.source_id 
			   AND gf.na_feature_id = drnf.na_feature_id
			   AND drnf.db_ref_id = dr.db_ref_id
			   AND dr.secondary_identifier = TO_CHAR(c.GI)
			   AND dr.external_database_release_id
			                        = edr.external_database_release_id
			   AND edr.external_database_id = ed.external_database_id
			   AND ed.name = 'NRDB_ref_dbXRefBySeqIdentity';
BEGIN
  FOR i IN CUR_DATA LOOP
  	DBMS_OUTPUT.PUT_LINE (i.gi || '|' || i.acc || '|' || i.tax_id || '|' || i.seq|| '|');
  END LOOP;
END;
/

spool off;			   

spool outFiles/ncbi_refseq_rmvrecords.txt;

DECLARE
   CURSOR CUR_DATA IS
			SELECT DISTINCT c.gi,
			       c.accession acc,
				   c.tax_id,
				   c.species_name species,
				   'SEQ' ||  tmp.source_id seq,
				   replacedByAccession replacedBy
			  FROM
			       PLASMODB.TEMPEXPORTDB2 tmp,
			  	   RemovedRECORDS@refseq c, 
				   dots.GeneFeature gf,
				   dots.DbRefNaFeature drnf,
				   sres.DbRef dr,
				   sres.ExternalDatabaseRelease edr,
				   sres.ExternalDatabase ed
			 WHERE
			       tmp.source_id = gf.source_id 
			   AND gf.na_feature_id = drnf.na_feature_id
			   AND drnf.db_ref_id = dr.db_ref_id
			   AND dr.secondary_identifier = TO_CHAR(c.GI)
			   AND dr.external_database_release_id
			                        = edr.external_database_release_id
			   AND edr.external_database_id = ed.external_database_id
			   AND ed.name = 'NRDB_ref_dbXRefBySeqIdentity';
BEGIN
  FOR i IN CUR_DATA LOOP
  	DBMS_OUTPUT.PUT_LINE (i.gi || '|' || i.acc || '|' || i.tax_id || '|' || i.species || '|' || i.seq|| '|' || i.replacedBy || '|');
  END LOOP;
END;
/

spool off;			   


spool outFiles/ncbi_taxo_names.txt;

DECLARE
   CURSOR CUR_DATA IS
			SELECT DISTINCT
				   n.tax_id,
				   n.name_txt name,
				   n.name_class
			  FROM 
			  	   PLASMODB.TEMPEXPORTDB2 tmp,
				   dots.GeneFeature gf,
				   dots.NaSequence s,
				   sres.Taxon t,
			  	   taxonomy.names@RefSeq n
			  WHERE tmp.source_id = gf.source_id
			    AND gf.na_sequence_id = s.na_sequence_id
				AND s.taxon_id = t.taxon_id
				AND t.NCBI_TAX_ID = n.tax_id
		UNION
			SELECT DISTINCT 
				   n.tax_id,
				   n.name_txt name,
				   n.name_class
			  FROM
			       PLASMODB.TEMPEXPORTDB2 tmp,
			  	   catalog@refseq c, 
				   dots.GeneFeature gf,
				   dots.DbRefNaFeature drnf,
				   sres.DbRef dr,
				   sres.ExternalDatabaseRelease edr,
				   sres.ExternalDatabase ed,
				   taxonomy.names@RefSeq n
			 WHERE
			       tmp.source_id = gf.source_id 
			   AND gf.na_feature_id = drnf.na_feature_id
			   AND drnf.db_ref_id = dr.db_ref_id
			   AND dr.secondary_identifier = TO_CHAR(c.GI)
			   AND dr.external_database_release_id
			                        = edr.external_database_release_id
			   AND edr.external_database_id = ed.external_database_id
			   AND ed.name = 'NRDB_ref_dbXRefBySeqIdentity'
			   AND n.tax_id = c.tax_id
		;
				
BEGIN
  FOR i IN CUR_DATA LOOP
  	DBMS_OUTPUT.PUT_LINE (i.tax_id || '|' || i.name || '|' || i.name_class|| '|');
  END LOOP;
END;
/

spool off;


spool outFiles/ebi_interpro_entry.txt;

DECLARE
   CURSOR CUR_DATA IS
			SELECT  DISTINCT
				 e.entry_ac,
				 e.name
			  FROM 
			  	   PLASMODB.TEMPEXPORTDB2 tmp,
			  	   domain_references dr,
			  	   ENTRY@interpro e
			  WHERE
			     tmp.source_id = dr.source_id
			     AND dr.db='Interpro'	 
				 AND dr.dbaccession = e.entry_ac;
BEGIN
  FOR i IN CUR_DATA LOOP
  	DBMS_OUTPUT.PUT_LINE (i.entry_ac || '|' || i.name|| '|');
  END LOOP;
END;
/

spool off;


spool outFiles/ebi_interpro_entry2meth.txt;

DECLARE
   CURSOR CUR_DATA IS
		SELECT DISTINCT
			 e.entry_ac,
			 e.method_ac,
			 dbname db
		  FROM 
		  	   PLASMODB.TEMPEXPORTDB2 tmp,
		  	   domain_references dr,
		  	   ENTRY2method@interpro e,
			   method@interpro m,
			   cv_database@interpro db
		  WHERE
		     tmp.source_id = dr.source_id
		     AND dr.db='Interpro'	 
			 AND dr.dbaccession = e.entry_ac
			 AND e.method_ac = m.method_ac
			 AND m.dbcode = db.dbcode;
BEGIN
  FOR i IN CUR_DATA LOOP
  	DBMS_OUTPUT.PUT_LINE (i.entry_ac || '|' || i.method_ac || '|' || i.db|| '|');
  END LOOP;
END;
/

spool off;

spool outFiles/ebi_interpro_interpro2go.txt;

DECLARE
   CURSOR CUR_DATA IS
			SELECT DISTINCT
				 e.entry_ac,
				 e.go_ac
			  FROM 
			  	   PLASMODB.TEMPEXPORTDB2 tmp,
			  	   domain_references dr,
			  	   interpro2go@interpro e
			  WHERE
			     tmp.source_id = dr.source_id
			     AND dr.db='Interpro'	 
				 AND dr.dbaccession = e.entry_ac;
BEGIN
  FOR i IN CUR_DATA LOOP
  	DBMS_OUTPUT.PUT_LINE (i.entry_ac || '|' || i.go_ac|| '|');
  END LOOP;
END;
/

spool off;


spool outFiles/go_term.txt;

DECLARE
   CURSOR CUR_DATA IS
			SELECT DISTINCT
			  	g.id,
				g.name,
				g.acc
			  FROM
			     PLASMODB.TEMPEXPORTDB2 tmp,
			     apidb.gotermsummary gs,	 
			     TERM@GOterms g
			  WHERE
			     tmp.source_id = gs.source_id
				 AND gs.go_id = g.acc 
			  UNION	
				SELECT 
				  	g.id,
					g.name,
					g.acc
				  FROM 
				  	   PLASMODB.TEMPEXPORTDB2 tmp,
				  	   domain_references dr,
				  	   interpro2go@interpro e,
					   TERM@GOterms g
				  WHERE
				     tmp.source_id = dr.source_id
				     AND dr.db='Interpro'	 
					 AND dr.dbaccession = e.entry_ac
					 AND e.go_ac = g.acc; 
BEGIN
  FOR i IN CUR_DATA LOOP
  	DBMS_OUTPUT.PUT_LINE (i.id || '|' || i.name || '|' || i.acc|| '|');
  END LOOP;
END;
/

spool off;					 

exit;