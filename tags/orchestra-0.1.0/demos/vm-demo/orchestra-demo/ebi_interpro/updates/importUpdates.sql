CONNECT TO EBI;

IMPORT FROM "/home/db2inst1/orchestra-demo/ebi_interpro/updates/ebi_interpro_entry.txt" OF DEL MODIFIED BY COLDEL| USEDEFAULTS METHOD P (1, 2, 3, 4) MESSAGES "/home/db2inst1/orchestra-demo/ebi_interpro/updates/ebi_interpro_entry_load_results.txt" INSERT INTO INTERPRO.ENTRY (ENTRY_AC, NAME, ENTRY_AC_LN, NAME_LN);

IMPORT FROM "/home/db2inst1/orchestra-demo/ebi_interpro/updates/ebi_interpro_entry2meth.txt" OF DEL MODIFIED BY COLDEL| USEDEFAULTS METHOD P (1, 2, 3, 4, 5, 6) MESSAGES "/home/db2inst1/orchestra-demo/ebi_interpro/updates/ebi_interpro_entry2meth_load_results.txt" INSERT INTO INTERPRO.ENTRY2METH (ENTRY_AC, METHOD_AC, DB, ENTRY_AC_LN, METHOD_AC_LN, DB_LN);

IMPORT FROM "/home/db2inst1/orchestra-demo/ebi_interpro/updates/ebi_interpro_interpro2go.txt" OF DEL MODIFIED BY COLDEL| USEDEFAULTS METHOD P (1, 2, 3, 4) MESSAGES "/home/db2inst1/orchestra-demo/ebi_interpro/updates/ebi_interpro_interpro2go_load_results.txt" INSERT INTO INTERPRO.INTERPRO2GO (ENTRY_AC, GO_AC, ENTRY_AC_LN, GO_AC_LN);

CONNECT RESET;
