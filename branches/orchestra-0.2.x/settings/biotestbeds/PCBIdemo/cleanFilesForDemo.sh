grep -v -e 'IPR002048|SM00054' -e 'IPR002048|PD000012' outFiles/ebi_interpro_entry2meth.txt > outFiles/ebi_interpro_entry2meth.txt2
mv outFiles/ebi_interpro_entry2meth.txt2 outFiles/ebi_interpro_entry2meth.txt 

grep -v -e 'IPR000425|SSF81338' -e 'IPR012269|TIGR00861' outFiles/ebi_interpro_entry2meth.txt > outFiles/ebi_interpro_entry2meth.txt2
mv outFiles/ebi_interpro_entry2meth.txt2 outFiles/ebi_interpro_entry2meth.txt 

echo 'IPR002073|SSF48547|SUPERFAMILY|' >> outFiles/ebi_interpro_entry2meth.txt

