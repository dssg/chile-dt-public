
#!/bin/bash
# Script for preprocessing data from raw


cd mnt/data/projects/direccion_trabajo_inspections/preprocessing



# Inspections data ----
# Meta data
cp ../raw/partner_data/Metadatos.xlsx metadatos.xlsx
in2csv metadatos.xlsx > metadatos.csv

# Sample data
cp ../raw/partner_data/DataSample.csv inspections_sample_raw.csv
cut -d';' f1- inspections_sample_raw.csv > inspections_sample.csv
awk 'NR==1{$0=tolower($0)} 1'  inspections_sample.csv

# Complete data
cp ../raw/partner_data/datafinaluchicago_completa_todoeldataset.csv inspections_complete_raw.csv
cut -d';' f1- inspections_complete_raw.csv > inspections_complete.csv
awk 'NR==1{$0=tolower($0)} 1'  inspections_complete_mod.csv


# Complete data without eliminations
cp ../raw/partner_data/datafinaluchicago_completa_todoeldataset_sineliminaciones.csv inspections_se_raw.csv
cut -d';' -f 1- inspections_se_raw.csv | awk 'NR==1{$0=tolower($0)} 1' > inspections_se_mod.csv
sed 's/;;/;\.;/g' tipificador_mod

ead -n1000 inspections_se_mod.csv | csvsql -i postgresql





# Additional data ----
# Taxes data
cp ../raw/partner_data/datasiiproceso.csv taxes_raw.csv
awk 'sub(/\./,"")+1' taxes_raw.csv > taxes.csv
sed 's/;;/;\.;/g' taxes.csv > taxes_mod.csv 


awk 'sub(/\./,"")+1' taxes_raw.csv | sed 's/;;/;\.;/g' > taxes_mod.csv

wc -l taxes_raw.csv # 3,552,018
wc -l taxes_mod.csv 	# 3,552,018

#rm taxes_raw.csv
head -n1000 taxes_mod.csv | csvsql -i postgresql


# tipificador codes translation
cp ../raw/partner_data/Tipificador\ al\ 27-11-17.xls tipificador.xls
in2csv -d';' --sheet 'Tipificadores' tipificador.xls > tipificador.csv
awk 'NR==1{$0=tolower($0)} 1' tipificador.csv | sed '1 s/ = //g;1 s/máx/max/g;1 s/\.//g;1 s/ //g' > tipificador_mod.csv


wc -l tipificador.csv # 3,385
wc -l tipificador_mod.csv 	# 3,385



#14_06_2018__13_37_15.xls
cp ../raw/partner_data/14_06_2018__13_37_15.xls .
in2csv 14_06_2018__13_37_15.xls > tt.csv

#14_06_2018__13_41_06.xls
#'Data DSSG.xlsx'

#economic_data




#'Cifras empleo 2010 - 2018.xls'
#'Imacec Chile 2013 - 2018.xls'
# 'IPC 2009 - 2018.xlsx'
#'PIB Chile 2013 -2018.xls'

# PDFS
# 'TIPIFICADOR INFRACCIONAL.pdf'
# InformeFiscalizacion.pdf
# IngresoFiscalizacion.pdf
# Multas.pdf
