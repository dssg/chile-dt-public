# Programa para seleccionar los datos para la Universidad de Chicago
# Versión para todos los datos del último data set
# 2018/05/09
# Ezequiel T. Muñoz Krsulovic

# última actualización
Sys.time()
#[1] "2018-05-09 12:50:03 GMT"
# seteo de opciones iniciales ---------------------------
#
getOption("max.print")
options(max.print = 500000)
getOption("max.print")
#
#----

# directorio de trabajo ---------------------------
#
# seteo del directorio por default para cargar datos
setwd("~/Desktop/AsesoriaDT/Data/dataconsii/")
#save.image(file="~/Desktop/AsesoriaDT/Data/dataconsii/DT09RFlow.RData")
#load("~/Desktop/AsesoriaDT/Data/dataconsii/DT09RFlow.RData")
#----

# carga de librerias ---------------------------
#
# función para cargar librerías y dependencias de estas
pkgTest <- function(x) {
	if (!require(x,character.only = TRUE)) {
		install.packages(x,dep=TRUE)
		if(!require(x,character.only = TRUE)) stop("Package not found")
	}
}

pkgTest("tibble")
pkgTest("lubridate")
pkgTest("rattle")
pkgTest("sqldf")
pkgTest("rpart")
pkgTest("tree")
pkgTest("party")
pkgTest("MASS")
pkgTest("partykit")
pkgTest("rpart.plot")
pkgTest("caret")
pkgTest("stringr")
pkgTest("rJava")
pkgTest("ROCR")
pkgTest("DMwR")
pkgTest("foreach")
pkgTest("C50")
pkgTest("randomForest")
pkgTest("ROSE")
pkgTest("e1071")
pkgTest("gmodels")
pkgTest("data.table")
pkgTest("matrixStats")
pkgTest("qicharts2")
pkgTest("plyr")
pkgTest("WriteXLS")
pkgTest("gdata")
pkgTest("xlsx")
pkgTest("caTools")
pkgTest("Amelia")
pkgTest("ggplot2")
pkgTest("RColorBrewer")
pkgTest("scales")
pkgTest("RODBC")
pkgTest("dummies")
pkgTest("RSNNS")
pkgTest("Cubist")
#pkgTest("gbm")
pkgTest("deepnet")
pkgTest("dplyr")
pkgTest("corrplot")
pkgTest("mice")
pkgTest("DT")
pkgTest("splitstackshape")
#carga de librerias
require(tibble)
require(lubridate)
require(rattle)
require(sqldf)
require(rpart)
require(tree)
require(party)
require(MASS)
require(partykit)
require(rpart.plot)
require(caret)
require(stringr)
require(rJava)
require(ROCR)
require(DMwR)
require(rpart)
require(foreach)
require(C50);
require(randomForest)
require(ROSE)
require(e1071)
require(gmodels)
require(data.table)
require(matrixStats)
require(qicharts2)
require(plyr)
require(WriteXLS)
require(gdata)
require(xlsx)
require(caTools)
require(Amelia)
require(ggplot2)
require(scales)
require(grid)
require(RColorBrewer)
require(RODBC)
require(dummies)
require(RSNNS)
require(Cubist)
#require(gbm)
require(deepnet)
require(dplyr)
require(corrplot)
require(mice)
require(DT)
require(splitstackshape)
#
#----

# funciones -----------------------
#
pMiss <- function(x) {
	sum(is.na(x))/length(x)*100
}
#
mergeFull <- function(x, y) {
	merge(x, y, all = TRUE)
}
#
mergeLeft <- function(x, y) {  
	merge(x, y, all.x = TRUE)
}
#
lagpad <- function(x, k) {
	if (!is.vector(x)) 
	stop('x must be a vector')
	if (!is.numeric(x)) 
	stop('x must be numeric')
	if (!is.numeric(k))
	stop('k must be numeric')
	if (1 != length(k))
	stop('k must be a single number')
	c(rep(0, k), x)[1 : length(x)] 
}
#
matrizeIndicadores <- function(Test,ClaseTexto,ClaseReferida,Prediccion,positiveClase,p) {
	#MatrizResultados <-NULL
	MatrizResultados <- data.frame(pos=integer(),Sensitivity=numeric(),Specificity=numeric(),
		Precision=numeric(),Accuracy=numeric(),F1=numeric(),
		NegativePredictiveValue=numeric(),FalsePositiveRate=numeric(),
		FalseDiscoveryRate=numeric(),FalseNegativeRate=numeric(),
		MisclassificationRatio=numeric(),
		stringsAsFactors=FALSE)
	#
	MatrizConfusion<-table(Test[,ClaseTexto],Prediccion) 
	print("Matriz Confusion")
	print(MatrizConfusion)
	# este está correcto, cambiado por lo que sale en confusionMatrix
	tp<-MatrizConfusion[2,2]
	print(c("tp",as.character(tp)))
	fn<-MatrizConfusion[2,1]
	print(c("fn",as.character(fn)))
	fp<-MatrizConfusion[1,2]
	print(c("fp",as.character(fp)))
	tn<-MatrizConfusion[1,1]
	print(c("tn",as.character(tn)))
	Resultados.Sensitivity<-tp/(tp+fn)
	print(c("Resultados.Sensitivity",as.character(Resultados.Sensitivity)))
	Resultados.Specificity<-tn/(fp+tn)
	print(c("Resultados.Specificity",as.character(Resultados.Specificity)))
	Resultados.Precision<-tp/(tp+fp)
	print(c("Resultados.Precision",as.character(Resultados.Precision)))
	Resultados.Accuracy<-(tp+tn)/(tp+fn+fp+tn)
	print(c("Resultados.Accuracy",as.character(Resultados.Accuracy)))
	Resultados.F1<-2*tp/(2*tp+fp+fn)
	print(c("Resultados.F1",as.character(Resultados.F1)))
	Resultados.NegativePredictiveValue<-tn/(tn+fn)
	print(c("Resultados.NegativePredictiveValue",as.character(Resultados.NegativePredictiveValue)))
	Resultados.FalsePositiveRate<-fp/(fp+tn)
	print(c("Resultados.FalsePositiveRate",as.character(Resultados.FalsePositiveRate)))
	Resultados.FalseDiscoveryRate<-fp/(fp+tp)
	print(c("Resultados.FalseDiscoveryRate",as.character(Resultados.FalseDiscoveryRate)))
	Resultados.FalseNegativeRate<-fn/(fn+tp)
	print(c("Resultados.FalseNegativeRate",as.character(Resultados.FalseDiscoveryRate)))
	Resultados.MisclassificationRatio<-(fp+fn)/(tp+fn+fp+tn)
	print(c("Resultados.MisclassificationRatio",as.character(Resultados.MisclassificationRatio)))
	#
	cM<-confusionMatrix(Prediccion,ClaseReferida,positive=positiveClase)
	print(cM)
	#
	nombrescolumnas=c("pos","Sensitivity","Specificity","Precision","Accuracy","F1",
		"NegativePredictiveValue","FalsePositiveRate","FalseDiscoveryRate",
		"FalseNegativeRate","MisclassificationRatio")
	newrow=c(pos=as.integer(p),Sensitivity=as.numeric(Resultados.Sensitivity),
		Specificity=as.numeric(Resultados.Specificity),
		Precision=as.numeric(Resultados.Precision),
		Accuracy=as.numeric(Resultados.Accuracy),
		F1=as.numeric(Resultados.F1),
		NegativePredictiveValue=as.numeric(Resultados.NegativePredictiveValue),
		FalsePositiveRate=as.numeric(Resultados.FalsePositiveRate),
		FalseDiscoveryRate=as.numeric(Resultados.FalseDiscoveryRate),
		FalseNegativeRate=as.numeric(Resultados.FalseNegativeRate),
		MisclassificationRatio=as.numeric(Resultados.MisclassificationRatio))
	#print("newrow:")
	#print(newrow)
	MatrizResultados<-rbind(MatrizResultados,newrow,deparse.level = 2)
	#print("MatrizResultados:")
	#print(MatrizResultados)
	colnames(MatrizResultados)<-nombrescolumnas
	#print(MatrizResultados)
	return(MatrizResultados)
	#
}
#
#----

# lectura datos 20170927 ----
#
sink(str_replace_all(paste(as.character(format(Sys.time(), "%Y-%m-%d-")),
	str_replace_all(as.character(format(Sys.time(), "%X")),c(":"),"-"),
	"-lecturadatos-3-dataset-20170927",".txt"),c(" "),""))
Sys.time()
#
fiscalizaciones20170927 <- read.csv("dataset_27_09_2017.csv", header=TRUE, sep=";")
s_fiscalizaciones20170927<-summary(fiscalizaciones20170927)
s_fiscalizaciones20170927
#
sqldf("select count(*) from fiscalizaciones20170927")
# 1408087
# 
Sys.time()
#
sink()
#----

# pre pro 20170927 ----
#
d <- fiscalizaciones20170927
nrow(d)
# [1] 1408087
#d <- d[!(d$Agno == "2005"),]
# Agno
summary(d$Agno)
'
Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
2006    2007    2010    2010    2013    2016 
'
d[ d == "NULL"]<- NA
d[ d == ""]<- NA

# no eliminaremnos los nulos
#d <- subset(d, !is.na(d$noInfra))
# [1] 1314225
# nrow(d)
apply(d, 2, pMiss)
'
           IDFiscalizacion                 CodOficina                       Agno 
                0.00000000                 0.00000000                 0.00000000 
NroComision             TotalAfectados                   urgencia 
0.00000000                19.03852532                21.13285614 
SolEsAfectado              FechaRegistro                 CodTipoSol 
42.95459016                 0.00127833                 0.00000000 
solicitante            CodUnidadOrigen               unidadOrigen 
0.00000000                 0.00000000                 0.00000000 
CodTipoTermino                tipoTermino             EgresoConMulta 
0.00000000                 0.00000000                 0.00000000 
RutEmpresa             EmpDFCodComuna             EmpDMCodComuna 
0.51687147                 0.51651638                37.12852970 
EmpTrabHombres                     CodCae             CodTipoEmpresa 
2.62838873                 1.47583210                21.64411716 
grupocodtipoMaterias     grupoglosatipoMaterias            grupoglosaInfra 
29.01163067                29.01163067                78.70174215 
grupocodigoInfra        grupoglosaInfra_det       grupocodigoInfra_det 
78.70174215                78.70174215                78.70174215 
grupocodtipoMaterias2    grupoglosatipoMaterias2           grupoglosaInfra2 
78.40119254                78.40119254                90.12695948 
grupoglosaInfra2_det          grupocodigoInfra2      grupocodigoInfra2_det 
91.31204251                90.12695948                91.30820752 
grupoCodigoNormaInfra2_det         actividadeconomica                     bloque 
90.13079448                32.19112171                96.23659618 
calle                     comuna                      depto 
32.19801049                32.19801049                83.35365641 
dvSII                    f22c645                    f22c646 
32.19112171                47.16121944                92.16369443 
fechainicio               fechatermino              ntrabajadores 
32.34182263                97.26188794                36.09223010 
numero                razonsocial                     region 
32.74975197                32.19112171                32.19801049 
rubro                   subrubro       subtipocontribuyente 
32.19112171                32.19112171                32.19112171 
tipocontribuyente             tipoterminoSII                tramoventas 
32.19112171                97.26188794                32.19112171 
villapoblacion                       ccae                       gcae 
84.81187597                 2.27152158                 2.27152158 
crae                       grae                    noInfra 
2.27152158                 2.27152158                 6.66592334 
Infra                derechoFund                  novaInfra 
6.66592334                 6.66592334                 6.66592334 
num_materias                   num_sind 
6.66592334                66.87505815 
'
d$fechareg <- substr(as.character(d$FechaRegistro),1,10)
#d$fechainf <- substr(as.character(d$FechaInforme),1,10)
d$FechaRegistro <- NULL
#d$FechaInforme <- NULL
d$Region <- as.factor(as.integer(d$CodOficina/100))
d$Infra <- as.numeric(as.character(d$Infra))
d$Infractor <- d$Infra
d$Infractor[d$Infractor >= 1] <- 1
d$Exsind <- as.integer(as.character(d$num_sind))
d$Exsind[d$Exsind >= 1] <- 1
d$Agno <- as.factor(d$Agno)
#
names(d)[names(d) == "actividadeconomica"] <- "actividadeconomicaSII"
names(d)[names(d) == "bloque"] <- "bloqueSII"
names(d)[names(d) == "calle"] <- "calleSII"
names(d)[names(d) == "comuna"] <- "comunaSII"
names(d)[names(d) == "depto"] <- "deptoSII"
names(d)[names(d) == "f22c645"] <- "f22c645SII"
names(d)[names(d) == "f22c646"] <- "f22c646SII"
names(d)[names(d) == "fechainicio"] <- "fechainicioSII"
names(d)[names(d) == "fechatermino"] <- "fechaterminoSII"
names(d)[names(d) == "ntrabajadores"] <- "ntrabajadoresSII"
names(d)[names(d) == "numero"] <- "numeroSII"
names(d)[names(d) == "razonsocial"] <- "razonsocialSII"
names(d)[names(d) == "region"] <- "regionSII"
names(d)[names(d) == "rubro"] <- "rubroSII"
names(d)[names(d) == "subrubro"] <- "subrubroSII"
names(d)[names(d) == "subtipocontribuyente"] <- "subtipocontribuyenteSII"
names(d)[names(d) == "tipocontribuyente"] <- "tipocontribuyenteSII"
names(d)[names(d) == "tramoventas"] <- "tramoventasSII"
names(d)[names(d) == "villapoblacion"] <- "villapoblacionSII"
daux<-d
#
d$ntrabajadoresSII <- gsub("\\.", "", d$ntrabajadoresSII)
d$ntrabajadoresSII <- as.double(d$ntrabajadoresSII)
#anno <- substr(d$fecha, star =1, stop =4)
#mes<-  substr(d$fecha, star =6, stop =7)
#dia <-  substr(d$fecha, star =9, stop =10)

data1<-d

# no se eliminan los datos de la DT, salvo los que ya han sido transformado
# se eliminarán los del SII
#
#d$CodTipoEmpresa <- NULL
#d$TotalAfectados <- NULL
#d$urgencia <- NULL
#d$SolEsAfectado <- NULL
#d$EmpDMCodComuna <- NULL
d$actividadeconomicaSII <- NULL
#d$agnoSII <- NULL
d$bloqueSII <- NULL
d$calleSII <- NULL
d$comunaSII <- NULL
d$deptoSII <- NULL
d$dvSII <- NULL
d$f22c645SII <- NULL
d$f22c646SII <- NULL
d$fechainicioSII <- NULL
d$fechaterminoSII <- NULL
d$numeroSII <- NULL
d$razonsocialSII <- NULL
d$regionSII <- NULL
d$rubroSII <- NULL
d$subrubroSII <- NULL
#d$rutSII <- NULL
d$subtipocontribuyenteSII <- NULL
d$tipoterminoSII <- NULL
d$tipocontribuyenteSII <- NULL
d$villapoblacionSII <- NULL

# otras eliminaciones de atributos del SII
d$ntrabajadoresSII <- NULL
d$tramoventasSII <- NULL

# eliminación de novaInfra, no se utilizó
d$novaInfra <- NULL

data2<- d

apply(d, 2, pMiss)
'
           IDFiscalizacion                 CodOficina                       Agno 
                0.00000000                 0.00000000                 0.00000000 
NroComision             TotalAfectados                   urgencia 
0.00000000                19.03852532                21.13285614 
SolEsAfectado                 CodTipoSol                solicitante 
42.95459016                 0.00000000                 0.00000000 
CodUnidadOrigen               unidadOrigen             CodTipoTermino 
0.00000000                 0.00000000                 0.00000000 
tipoTermino             EgresoConMulta                 RutEmpresa 
0.00000000                 0.00000000                 0.51687147 
EmpDFCodComuna             EmpDMCodComuna             EmpTrabHombres 
0.51651638                37.12852970                 2.62838873 
CodCae             CodTipoEmpresa       grupocodtipoMaterias 
1.47583210                21.64411716                29.01163067 
grupoglosatipoMaterias            grupoglosaInfra           grupocodigoInfra 
29.01163067                78.70174215                78.70174215 
grupoglosaInfra_det       grupocodigoInfra_det      grupocodtipoMaterias2 
78.70174215                78.70174215                78.40119254 
grupoglosatipoMaterias2           grupoglosaInfra2       grupoglosaInfra2_det 
78.40119254                90.12695948                91.31204251 
grupocodigoInfra2      grupocodigoInfra2_det grupoCodigoNormaInfra2_det 
90.12695948                91.30820752                90.13079448 
ccae                       gcae                       crae 
2.27152158                 2.27152158                 2.27152158 
grae                    noInfra                      Infra 
2.27152158                 6.66592334                 6.66592334 
derechoFund                  novaInfra               num_materias 
6.66592334                 6.66592334                 6.66592334 
num_sind                   fechareg                     Region 
66.87505815                 0.00127833                 0.00000000 
Infractor                     Exsind 
6.66592334                66.87505815 
'
# se elimina Exsind pues no aporta en mejorar los modelos
# no se eliminan para el caso del envío a la U de Chicago
#d$Exsind <- NULL
#d$num_sind <- NULL

d$datereg <- as.Date(d$fechareg)
d$fechareg <- NULL
#d$dateinf <- as.Date(d$fechainf)
d$mesreg <- as.factor(month(d$datereg))
#d$mesinf <- as.factor(month(d$dateinf))
nrow(d)
# [1] 1408087

# selección de datos
# ahora base completa:
d <- d[which(d$datereg >= as.Date("2006-06-01","%Y-%m-%d")),]
nrow(d)
# [1] 1305507
d <- d[which(d$datereg <= as.Date("2016-06-30","%Y-%m-%d")),]
nrow(d)
# [1] 1254937

data2a<-d

d$RutEmpresa <- as.numeric(as.character(d$RutEmpresa))
#
d <- d[with(d, order(RutEmpresa)), ]
#
sqldf("select RutEmpresa,count(*) from d where RutEmpresa in (0,1,2,10,11,12,76,88,99,109,123,126,235,1002,10009,100000,1000000,1111111,99999999,9999999,999999) group by RutEmpresa order by RutEmpresa")
'
   RutEmpresa count(*)
1           0      143
2           1     1022
3           2        1
4          10        1
5          11        1
6          12        1
7          76        1
8          88        1
9          99        1
10        109        3
11        123       87
12        126        2
13        235        1
14       1002        3
15      10009        8
16     100000       11
17     999999        1
18    1000000       29
19    1111111       86
20    9999999       23
21   99999999       72
'
sqldf("select RutEmpresa,count(*) from d where RutEmpresa is null group by RutEmpresa order by RutEmpresa")
'
  RutEmpresa count(*)
1         NA     6135
'
nrow(d)
# [1] 1254937

# no eliminamos los RutEmpresa NA
# d <- subset(d, !is.na(d$RutEmpresa))

nrow(d)
# [1] 1254937
#
# eliminamos todos los rut raros
d <- d[ which(d$RutEmpresa != "0" ),  ]
d <- d[ which(d$RutEmpresa != "1" ),  ]
d <- d[ which(d$RutEmpresa != "2" ),  ]
d <- d[ which(d$RutEmpresa != "10" ),  ]
d <- d[ which(d$RutEmpresa != "11" ),  ]
d <- d[ which(d$RutEmpresa != "12" ),  ]
d <- d[ which(d$RutEmpresa != "76" ),  ]
d <- d[ which(d$RutEmpresa != "88" ),  ]
d <- d[ which(d$RutEmpresa != "99" ),  ]
d <- d[ which(d$RutEmpresa != "109" ),  ]
d <- d[ which(d$RutEmpresa != "123" ),  ]
d <- d[ which(d$RutEmpresa != "126" ),  ]
d <- d[ which(d$RutEmpresa != "235" ),  ]
d <- d[ which(d$RutEmpresa != "1002" ),  ]
d <- d[ which(d$RutEmpresa != "10009" ),  ]
d <- d[ which(d$RutEmpresa != "100000" ),  ]
d <- d[ which(d$RutEmpresa != "1000000" ),  ]
d <- d[ which(d$RutEmpresa != "1111111" ),  ]
d <- d[ which(d$RutEmpresa != "99999999" ),  ]
d <- d[ which(d$RutEmpresa != "9999999" ),  ]
d <- d[ which(d$RutEmpresa != "999999" ),  ]
nrow(d)
# [1] 1247344

data2b <- d

#
d$CodTipoSol <- as.factor(d$CodTipoSol)
d$CodTipoTermino <- as.factor(d$CodTipoTermino)

# eploraciones de datos para ver si se eliminan
#
sqldf("select CodTipoTermino,count(*) from d group by CodTipoTermino order by CodTipoTermino")
'
  CodTipoTermino count(*)
1               0        3
2              10   276342
3              11     2369
4              15    93745
5              16     1696
6              17   114667
7              18      517
8              22       11
9               8      448
10              9   757546
'
#
sqldf("select CodTipoSol,count(*) from d group by CodTipoSol order by CodTipoSol")
'
   CodTipoSol count(*)
1           0   324885
2           1   401186
3          10    20781
4          11    15823
5          12    18899
6          13      976
7           2    63520
8           3   142478
9           4    96169
10          5    95227
11          6     6063
12          7     5600
13          8    20508
14          9    35229
'
#
sqldf("select solicitante,count(*) from d group by solicitante order by solicitante")
'
                   solicitante count(*)
1  Autoridad                       5600
2  Direccion Nacional             15823
3  Direccion Regional             20781
4  Empleador                     142478
5  Fiscalizador                   18899
6  Inspeccion                     35229
7  Institucion Previsional        95227
8             No se Identifica    96169
9  Organizacion Sindical          63520
10 Otra Institucion                6063
11                Por Programa   324885
12 Tercero                        20508
13                Trabajadores   401186
14 Tribunal                         976
'
#
sqldf("select solicitante,CodTipoSol,count(*) from d group by solicitante,CodTipoSol order by solicitante,CodTipoSol")
'
                   solicitante CodTipoSol count(*)
1  Autoridad                            7     5600
2  Direccion Nacional                  11    15823
3  Direccion Regional                  10    20781
4  Empleador                            3   142478
5  Fiscalizador                        12    18899
6  Inspeccion                           9    35229
7  Institucion Previsional              5    95227
8             No se Identifica          4    96169
9  Organizacion Sindical                2    63520
10 Otra Institucion                     6     6063
11                Por Programa          0   324885
12 Tercero                              8    20508
13                Trabajadores          1   401186
14 Tribunal                            13      976
'
#
sqldf("select CodTipoTermino,tipoTermino,count(*) from d group by CodTipoTermino,tipoTermino order by CodTipoTermino,tipoTermino")
'
  CodTipoTermino          tipoTermino count(*)
1               0                  Sin informacion        3
2              10                        Con Multa   276342
3              11           Sin Revisión con Multa     2369
4              15              Beneficio Sin Multa    93745
5              16              Beneficio Con Multa     1696
6              17            Certificado Sin Multa   114667
7              18            Certificado Con Multa      517
8              22             Derivado a Mediación       11
9               8 Revisión Completa Con Infracción      448
10              9                        Sin Multa   757546
'
#
sqldf("select CodTipoTermino,count(*) from d group by CodTipoTermino order by CodTipoTermino")
'
  CodTipoTermino count(*)
   CodTipoTermino count(*)
1               0        3
2              10   276342
3              11     2369
4              15    93745
5              16     1696
6              17   114667
7              18      517
8              22       11
9               8      448
10              9   757546
'
#
#d$CodTipoSol <- as.factor(d$CodTipoSol)
sqldf("select CodTipoSol,count(*) from d group by CodTipoSol order by CodTipoSol")
'
   CodTipoSol count(*)
1           0   324885
2           1   401186
3          10    20781
4          11    15823
5          12    18899
6          13      976
7           2    63520
8           3   142478
9           4    96169
10          5    95227
11          6     6063
12          7     5600
13          8    20508
14          9    35229
'
#
sqldf("select crae,grae,count(*) from d group by crae,grae order by crae,grae")
'
   crae                                                                             grae count(*)
1  <NA>                                                                             <NA>    11748
2     1 AGRICULTURA, CAZA, SILVICULTURA Y PESCA                                                 2
3   100 ACTIVIDADES NO ESPECIFICADAS Y OTRAS                                                69496
4   101                                      Agricultura, ganadería, caza y silvicultura    87537
5   102                                                                            Pesca    10713
6   103                                                  Explotación de minas y canteras     9741
7   104                                                        Industrias manufactureras    86046
8   105                                           Suministro de electricidad, gas y agua     5183
9   106                                                                     Construcción   147567
10  107                                                                         Comercio   211164
11  108                                                           Hoteles y restaurantes    91984
12  109                                      Transporte, almacenamiento y comunicaciones   141283
13  110                                                        Intermediación financiera    16167
14  111                               Actividades inmobiliarias, empresariales y alquile   160079
15  112                                                 Administración publica y defensa     3947
16  113                                                                        Enseñanza    52547
17  114                                                    Servicios sociales y de salud    18585
18  115                                      Otras actividades de servicios comunitarios    91372
19  116                                          Hogares privados con servicio doméstico    32033
20  117                                      Organizaciones y órganos extraterritoriales      150
'
#
#d <- d[ which(d$solicitante != "Institucion Previsional    " ),  ]
#d <- d[ which(d$solicitante != "Empleador                  " ),  ]
#

# datos hasta el momento
nrow(d)
# [1] 1247344
#

# no eliminaremos estos tipos de solicitud
#
#d <- d[ which(d$CodTipoSol != 13 ),  ]
#d <- d[ which(d$CodTipoSol != 5 ),  ]
#d <- d[ which(d$CodTipoSol != 3 ),  ]

#
sqldf("select solicitante,CodTipoSol,count(*) from d group by solicitante,CodTipoSol order by solicitante,CodTipoSol")
'
                   solicitante CodTipoSol count(*)
1  Autoridad                            7     5600
2  Direccion Nacional                  11    15823
3  Direccion Regional                  10    20781
4  Empleador                            3   142478
5  Fiscalizador                        12    18899
6  Inspeccion                           9    35229
7  Institucion Previsional              5    95227
8             No se Identifica          4    96169
9  Organizacion Sindical                2    63520
10 Otra Institucion                     6     6063
11                Por Programa          0   324885
12 Tercero                              8    20508
13                Trabajadores          1   401186
14 Tribunal                            13      976
'
#
nrow(d)
# [1] 1247344
#

# no eliminaremos los siguientes tipos de término
#
#d <- d[ which(d$CodTipoTermino != 0 ),  ]
#d <- d[ which(d$CodTipoTermino != 11 ),  ]
#d <- d[ which(d$CodTipoTermino != 22 ),  ]
#d <- d[ which(d$CodTipoTermino != 15 ),  ]
#d <- d[ which(d$CodTipoTermino != 16 ),  ]

#
sqldf("select CodTipoTermino,tipoTermino,count(*) from d group by CodTipoTermino,tipoTermino order by CodTipoTermino,tipoTermino")
'
 CodTipoTermino           tipoTermino count(*)
1               0                  Sin informacion        3
2              10                        Con Multa   276342
3              11           Sin Revisión con Multa     2369
4              15              Beneficio Sin Multa    93745
5              16              Beneficio Con Multa     1696
6              17            Certificado Sin Multa   114667
7              18            Certificado Con Multa      517
8              22             Derivado a Mediación       11
9               8 Revisión Completa Con Infracción      448
10              9                        Sin Multa   757546
'
#
sqldf("select crae,grae,count(*) from d group by crae,grae order by crae,grae")
'
   crae                                                                             grae count(*)
1  <NA>                                                                             <NA>    11748
2     1 AGRICULTURA, CAZA, SILVICULTURA Y PESCA                                                 2
3   100 ACTIVIDADES NO ESPECIFICADAS Y OTRAS                                                69496
4   101                                      Agricultura, ganadería, caza y silvicultura    87537
5   102                                                                            Pesca    10713
6   103                                                  Explotación de minas y canteras     9741
7   104                                                        Industrias manufactureras    86046
8   105                                           Suministro de electricidad, gas y agua     5183
9   106                                                                     Construcción   147567
10  107                                                                         Comercio   211164
11  108                                                           Hoteles y restaurantes    91984
12  109                                      Transporte, almacenamiento y comunicaciones   141283
13  110                                                        Intermediación financiera    16167
14  111                               Actividades inmobiliarias, empresariales y alquile   160079
15  112                                                 Administración publica y defensa     3947
16  113                                                                        Enseñanza    52547
17  114                                                    Servicios sociales y de salud    18585
18  115                                      Otras actividades de servicios comunitarios    91372
19  116                                          Hogares privados con servicio doméstico    32033
20  117                                      Organizaciones y órganos extraterritoriales      150
'

# no borramos los NA del rae
#d <- subset(d, !is.na(d$crae))
nrow(d)
# [1] 1247344
#
# no borramos las actividades no especificadas
#d <- d[ which(d$crae != 100 ),  ]
nrow(d)
# [1] 1247344
#

# no borramos el con crae = 1
# d <- d[ which(d$crae != 1 ),  ]
nrow(d)
# 1247344
#
sqldf("select crae,grae,count(*) from d group by crae,grae order by crae,grae")
'
   crae                                               grae count(*)
1  <NA>                                                                             <NA>    11748
2     1 AGRICULTURA, CAZA, SILVICULTURA Y PESCA                                                 2
3   100 ACTIVIDADES NO ESPECIFICADAS Y OTRAS                                                69496
4   101                                      Agricultura, ganadería, caza y silvicultura    87537
5   102                                                                            Pesca    10713
6   103                                                  Explotación de minas y canteras     9741
7   104                                                        Industrias manufactureras    86046
8   105                                           Suministro de electricidad, gas y agua     5183
9   106                                                                     Construcción   147567
10  107                                                                         Comercio   211164
11  108                                                           Hoteles y restaurantes    91984
12  109                                      Transporte, almacenamiento y comunicaciones   141283
13  110                                                        Intermediación financiera    16167
14  111                               Actividades inmobiliarias, empresariales y alquile   160079
15  112                                                 Administración publica y defensa     3947
16  113                                                                        Enseñanza    52547
17  114                                                    Servicios sociales y de salud    18585
18  115                                      Otras actividades de servicios comunitarios    91372
19  116                                          Hogares privados con servicio doméstico    32033
20  117                                      Organizaciones y órganos extraterritoriales      150
'
#

colnames(d)
'
 [1] "IDFiscalizacion"            "CodOficina"                 "Agno"                       "NroComision"               
 [5] "TotalAfectados"             "urgencia"                   "SolEsAfectado"              "CodTipoSol"                
[9] "solicitante"                "CodUnidadOrigen"            "unidadOrigen"               "CodTipoTermino"            
[13] "tipoTermino"                "EgresoConMulta"             "RutEmpresa"                 "EmpDFCodComuna"            
[17] "EmpDMCodComuna"             "EmpTrabHombres"             "CodCae"                     "CodTipoEmpresa"            
[21] "grupocodtipoMaterias"       "grupoglosatipoMaterias"     "grupoglosaInfra"            "grupocodigoInfra"          
[25] "grupoglosaInfra_det"        "grupocodigoInfra_det"       "grupocodtipoMaterias2"      "grupoglosatipoMaterias2"   
[29] "grupoglosaInfra2"           "grupoglosaInfra2_det"       "grupocodigoInfra2"          "grupocodigoInfra2_det"     
[33] "grupoCodigoNormaInfra2_det" "ccae"                       "gcae"                       "crae"                      
[37] "grae"                       "noInfra"                    "Infra"                      "derechoFund"               
[41] "num_materias"               "num_sind"                   "Region"                     "Infractor"                 
[45] "Exsind"                     "datereg"                    "mesreg" 
'
nrow(d)
# [1] 1247344

data2c<-d

# ajustes finales
d$grupocodtipoMaterias <- ifelse(is.na(d$grupocodtipoMaterias), "99", as.character(d$grupocodtipoMaterias))
d$grupoglosatipoMaterias <- ifelse(is.na(d$grupoglosatipoMaterias), "99", as.character(d$grupoglosatipoMaterias))
d$grupoglosaInfra <- ifelse(is.na(d$grupoglosaInfra), "99", as.character(d$grupoglosaInfra))
d$grupocodigoInfra <- ifelse(is.na(d$grupocodigoInfra), "99", as.character(d$grupocodigoInfra))
d$grupoglosaInfra_det <- ifelse(is.na(d$grupoglosaInfra_det), "99", as.character(d$grupoglosaInfra_det))
d$grupocodigoInfra_det <- ifelse(is.na(d$grupocodigoInfra_det), "99", as.character(d$grupocodigoInfra_det))
d$grupocodtipoMaterias2 <- ifelse(is.na(d$grupocodtipoMaterias2), "99", as.character(d$grupocodtipoMaterias2))
d$grupoglosatipoMaterias2 <- ifelse(is.na(d$grupoglosatipoMaterias2), "99", as.character(d$grupoglosatipoMaterias2))
d$grupoglosaInfra2 <- ifelse(is.na(d$grupoglosaInfra2), "99", as.character(d$grupoglosaInfra2))
d$grupoglosaInfra2_det <- ifelse(is.na(d$grupoglosaInfra2_det), "99", as.character(d$grupoglosaInfra2_det))
d$grupocodigoInfra2 <- ifelse(is.na(d$grupocodigoInfra2), "99", as.character(d$grupocodigoInfra2))
d$grupocodigoInfra2_det <- ifelse(is.na(d$grupocodigoInfra2_det), "99", as.character(d$grupocodigoInfra2_det))
d$grupoCodigoNormaInfra2_det <- ifelse(is.na(d$grupoCodigoNormaInfra2_det), "99", as.character(d$grupoCodigoNormaInfra2_det))

data2d<-d
# ----

# enmascarar los RutEmpresa ----

dataparaenmascarar <- d

loadRLibrary <- function(x) {
  if (! (x %in% rownames(installed.packages() ) ) ) { 
    install.packages(x, dependencies = TRUE)
    if (! (x %in% rownames( installed.packages() ) ) ) { stop("Package not found") }
  }
  if ( (x %in% rownames( installed.packages() ) ) ) { require(x, character.only = TRUE) }
}

# carga
loadRLibrary("data.table")
loadRLibrary("digest")
loadRLibrary("knitr")
loadRLibrary("anonymizer")

dataparaenmascarar$RutEmpresaMask <- dataparaenmascarar$RutEmpresa
dataparaenmascarar$RutEmpresaMask <- as.character(dataparaenmascarar$RutEmpresaMask)
head(dataparaenmascarar, n=20L)
'
        IDFiscalizacion CodOficina Agno NroComision TotalAfectados urgencia SolEsAfectado CodTipoSol
1175671         2387019       1311 2014        3639           <NA>        0             1          5
459169          1538817       1309 2008         738           <NA>        0             1          5
748933          1870289       1322 2010        4175           <NA>        0             1          5
398670          1471322       1301 2007        6670             15        0          <NA>          0
476630          1558287       1301 2008        3688             30        1          <NA>          0
582713          1679519       1316 2009         723             33        0             1          4
667819          1776537       1316 2010         222              1        1             1          1
676401          1786322       1316 2010         454              1        0             1          1
683634          1794703       1316 2010         578              1        1          <NA>          2
701210          1814521       1316 2010         855              1        1             1          1
705616          1819506       1316 2010         921             22        1          <NA>          2
708014          1822292       1316 2010         959             22        1          <NA>          2
719225          1835134       1316 2010        1144             22        1          <NA>          2
727980          1845637       1316 2010        1281              1        0             1          1
735361          1854160       1316 2010        1379              1        0             1          1
738320          1857664       1316 2010        1412             22        1          <NA>          2
745948          1866684       1316 2010        1550              1        1             1          1
759496          1882632       1316 2010        1713              1        1             1          1
771352          1897054       1316 2011         177             15        1             1          1
816297          1950155       1316 2011        1051              4        1          <NA>          2
solicitante CodUnidadOrigen        unidadOrigen CodTipoTermino         tipoTermino EgresoConMulta
1175671 Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
459169  Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
748933  Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
398670                 Por Programa               2       Fiscalización              9           Sin Multa              0
476630                 Por Programa               2       Fiscalización              9           Sin Multa              0
582713             No se Identifica               1 Atención de Público             10           Con Multa              1
667819                 Trabajadores               1 Atención de Público              9           Sin Multa              0
676401                 Trabajadores               1 Atención de Público             10           Con Multa              1
683634  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
701210                 Trabajadores               1 Atención de Público             10           Con Multa              1
705616  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
708014  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
719225  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
727980                 Trabajadores               1 Atención de Público             10           Con Multa              1
735361                 Trabajadores               1 Atención de Público             10           Con Multa              1
738320  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
745948                 Trabajadores               1 Atención de Público              9           Sin Multa              0
759496                 Trabajadores               1 Atención de Público             10           Con Multa              1
771352                 Trabajadores               1 Atención de Público             10           Con Multa              1
816297  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
RutEmpresa EmpDFCodComuna EmpDMCodComuna EmpTrabHombres CodCae CodTipoEmpresa
1175671      11780          13101              0              1    738              4
459169       23294          13101           <NA>              2    906              1
748933       28282          13114           <NA>              1      1              4
398670       37229          13101          13101             15   1132              1
476630       37229          13101          13101             14   1132              4
582713       37229          13110           <NA>             34   1132              4
667819       37229          13110          13110             30   1132              4
676401       37229          13110           <NA>             12   1131              4
683634       37229          13110           <NA>             29   1132              1
701210       37229          13110           <NA>             29   1132              4
705616       37229          13110           <NA>             28   1132              4
708014       37229          13110           <NA>             28   1132              4
719225       37229          13110          13110             29   1132              1
727980       37229          13110           <NA>             31   1132              1
735361       37229          13110           <NA>             31   1131              4
738320       37229          13110           <NA>             31   1131              4
745948       37229          13110           <NA>             31   1132              4
759496       37229          13110           <NA>             29   1132              1
771352       37229          13110          13110             28   1132              4
816297       37229          13110           <NA>             27   1132              1
grupocodtipoMaterias
1175671                                                                   1         
459169                                                                    1         
748933                                                                    1         
398670                                   10        ,15        ,3         ,6         
476630                        10        ,15        ,3         ,3         ,6         
582713  15        ,3         ,3         ,7         ,7         ,9         ,9         
667819                                              15        ,3         ,9         
676401                        10        ,15        ,15        ,3         ,9         
683634                                                                    3         
701210                                   10        ,15        ,3         ,9         
705616                        10        ,15        ,15        ,6         ,9         
708014                        10        ,15        ,15        ,15        ,9         
719225                                              15        ,15        ,9         
727980             10        ,15        ,15        ,3         ,8         ,9         
735361             10        ,10        ,15        ,15        ,3         ,9         
738320                                              10        ,15        ,9         
745948                                   10        ,15        ,3         ,9         
759496                                              15        ,15        ,9         
771352                                              15        ,15        ,9         
816297                                                                    15        
grupoglosatipoMaterias
1175671                                                                                                                                                                     BENEFICIOS PREVISIONALES
459169                                                                                                                                                                      BENEFICIOS PREVISIONALES
748933                                                                                                                                                                      BENEFICIOS PREVISIONALES
398670                                                                                                             JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,COTIZACIONES PREVISIONALES
476630                                                                                         JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,CONTRATO DE TRABAJO,COTIZACIONES PREVISIONALES
582713  REMUNERACIONES,CONTRATO DE TRABAJO,CONTRATO DE TRABAJO,ACOSO SEXUAL, SIMULACIÓN, SUBTERFUGIO Y OTROS.,ACOSO SEXUAL, SIMULACIÓN, SUBTERFUGIO Y OTROS.,HIGIENE Y SEGURIDAD,HIGIENE Y SEGURIDAD
667819                                                                                                                                        REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
676401                                                                                                     JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
683634                                                                                                                                                                           CONTRATO DE TRABAJO
701210                                                                                                                    JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
705616                                                                                              JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,COTIZACIONES PREVISIONALES,HIGIENE Y SEGURIDAD
708014                                                                                                          JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
719225                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
727980                                                                                  JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,FERIADO Y PERMISOS,HIGIENE Y SEGURIDAD
735361                                                                                 JORNADA Y DESCANSOS,JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
738320                                                                                                                                        JORNADA Y DESCANSOS,REMUNERACIONES,HIGIENE Y SEGURIDAD
745948                                                                                                                    JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
759496                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
771352                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
816297                                                                                                                                                                                REMUNERACIONES
grupoglosaInfra      grupocodigoInfra
1175671                                      99                    99
459169                                       99                    99
748933                                       99                    99
398670                                       99                    99
476630                                       99                    99
582713  HIGIENE Y SEGURIDAD,HIGIENE Y SEGURIDAD 9         ,9         
667819                                       99                    99
676401                      CONTRATO DE TRABAJO            3         
683634                      CONTRATO DE TRABAJO            3         
701210                           REMUNERACIONES            15        
705616                           REMUNERACIONES            15        
708014                           REMUNERACIONES            15        
719225            REMUNERACIONES,REMUNERACIONES 15        ,15        
727980                           REMUNERACIONES            15        
735361                           REMUNERACIONES            15        
738320                                       99                    99
745948                           REMUNERACIONES            15        
759496                           REMUNERACIONES            15        
771352            REMUNERACIONES,REMUNERACIONES 15        ,15        
816297                           REMUNERACIONES            15        
grupoglosaInfra_det grupocodigoInfra_det
1175671                                                                                               99                   99
459169                                                                                                99                   99
748933                                                                                                99                   99
398670                                                                                                99                   99
476630                                                                                                99                   99
582713  || COMITÉ PARITARIO/NO EFECTUAR REUNIÓN OBLIGATORIA || NO CUMPLIR CONDICIONES SANITARIAS BÁSICAS        || 403 || 407
667819                                                                                                99                   99
676401                                                            || MODIFICACIÓN UNILATERAL CLÁUSULA(S)               || 515
683634                           || Articulo 12 C. del T/Cambio unilateral lugar/naturaleza de servicios               || 338
701210                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
705616                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
708014                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
719225                         || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE || NO PAGAR/INGRESO MÍNIMO        || 462 || 467
727980                                                                     || NO PAGAR/EN FORMA OPORTUNA               || 463
735361                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
738320                                                                                                99                   99
745948                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
759496                                                                     || NO PAGAR/EN FORMA OPORTUNA               || 463
771352                      || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE || NO PAGAR/EN FORMA OPORTUNA        || 462 || 463
816297                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
grupocodtipoMaterias2 grupoglosatipoMaterias2 grupoglosaInfra2 grupoglosaInfra2_det grupocodigoInfra2
1175671                    99                      99               99                   99                99
459169                     99                      99               99                   99                99
748933                     99                      99               99                   99                99
398670                     99                      99               99                   99                99
476630                     99                      99               99                   99                99
582713                     99                      99               99                   99                99
667819                     99                      99               99                   99                99
676401                     99                      99               99                   99                99
683634                     99                      99               99                   99                99
701210                     99                      99               99                   99                99
705616                     99                      99               99                   99                99
708014                     99                      99               99                   99                99
719225                     99                      99               99                   99                99
727980                     99                      99               99                   99                99
735361                     99                      99               99                   99                99
738320                     99                      99               99                   99                99
745948                     99                      99               99                   99                99
759496                     99                      99               99                   99                99
771352                     99                      99               99                   99                99
816297                     99                      99               99                   99                99
grupocodigoInfra2_det grupoCodigoNormaInfra2_det   ccae
1175671                    99                         99 289200
459169                     99                         99 522020
748933                     99                         99      0
398670                     99                         99 802100
476630                     99                         99 802100
582713                     99                         99 802100
667819                     99                         99 802100
676401                     99                         99 801020
683634                     99                         99 802100
701210                     99                         99 802100
705616                     99                         99 802100
708014                     99                         99 802100
719225                     99                         99 802100
727980                     99                         99 802100
735361                     99                         99 801020
738320                     99                         99 801020
745948                     99                         99 802100
759496                     99                         99 802100
771352                     99                         99 802100
816297                     99                         99 802100
gcae
1175671 Tratamientos y revestimientos de metales; obras de ingeniería mecánica en general realizadas a cambio de una retribución o contrata                                                                     
459169  Venta al por menor de carnes (rojas, blancas, otras) productos cárnicos y similares.                                                                                                                    
748933  ACTIVIDADES NO ESPECIFICADAS                                                                                                                                                                            
398670  Enseñanza secundaria de  formación general                                                                                                                                                              
476630  Enseñanza secundaria de  formación general                                                                                                                                                              
582713  Enseñanza secundaria de  formación general                                                                                                                                                              
667819  Enseñanza secundaria de  formación general                                                                                                                                                              
676401  Enseñanza primaria                                                                                                                                                                                      
683634  Enseñanza secundaria de  formación general                                                                                                                                                              
701210  Enseñanza secundaria de  formación general                                                                                                                                                              
705616  Enseñanza secundaria de  formación general                                                                                                                                                              
708014  Enseñanza secundaria de  formación general                                                                                                                                                              
719225  Enseñanza secundaria de  formación general                                                                                                                                                              
727980  Enseñanza secundaria de  formación general                                                                                                                                                              
735361  Enseñanza primaria                                                                                                                                                                                      
738320  Enseñanza primaria                                                                                                                                                                                      
745948  Enseñanza secundaria de  formación general                                                                                                                                                              
759496  Enseñanza secundaria de  formación general                                                                                                                                                              
771352  Enseñanza secundaria de  formación general                                                                                                                                                              
816297  Enseñanza secundaria de  formación general                                                                                                                                                              
crae                                                                             grae noInfra Infra derechoFund
1175671  104                                                        Industrias manufactureras       1     0           0
459169   107                                                                         Comercio       1     0           0
748933   100 ACTIVIDADES NO ESPECIFICADAS Y OTRAS                                                   1     0           0
398670   113                                                                        Enseñanza       4     0           0
476630   113                                                                        Enseñanza       5     0           0
582713   113                                                                        Enseñanza       5     2           0
667819   113                                                                        Enseñanza       3     0           0
676401   113                                                                        Enseñanza       4     1           0
683634   113                                                                        Enseñanza       0     1           0
701210   113                                                                        Enseñanza       3     1           0
705616   113                                                                        Enseñanza       4     1           0
708014   113                                                                        Enseñanza       4     1           0
719225   113                                                                        Enseñanza       1     2           0
727980   113                                                                        Enseñanza       5     1           0
735361   113                                                                        Enseñanza       5     1           0
738320   113                                                                        Enseñanza       3     0           0
745948   113                                                                        Enseñanza       3     1           0
759496   113                                                                        Enseñanza       2     1           0
771352   113                                                                        Enseñanza       1     2           0
816297   113                                                                        Enseñanza       0     1           0
num_materias num_sind Region Infractor Exsind    datereg mesreg RutEmpresaMask
1175671            1     <NA>     13         0     NA 2014-11-20     11          11780
459169             1     <NA>     13         0     NA 2008-04-11      4          23294
748933             1     <NA>     13         0     NA 2010-11-22     11          28282
398670             4        3     13         0      1 2007-10-24     10          37229
476630             5        3     13         0      1 2008-06-10      6          37229
582713             7        3     13         1      1 2009-05-26      5          37229
667819             3        3     13         0      1 2010-02-24      2          37229
676401             5        3     13         1      1 2010-03-29      3          37229
683634             1        3     13         1      1 2010-04-21      4          37229
701210             4        3     13         1      1 2010-06-14      6          37229
705616             5        3     13         1      1 2010-06-30      6          37229
708014             5        3     13         1      1 2010-07-08      7          37229
719225             3        3     13         1      1 2010-08-13      8          37229
727980             6        3     13         1      1 2010-09-13      9          37229
735361             6        3     13         1      1 2010-10-06     10          37229
738320             3        3     13         0      1 2010-10-15     10          37229
745948             4        3     13         1      1 2010-11-11     11          37229
759496             3        3     13         1      1 2010-12-30     12          37229
771352             3        3     13         1      1 2011-02-22      2          37229
816297             1        3     13         1      1 2011-07-26      7          37229
'
# respaldamos los datos
dataparaenmascarar_respaldo <- dataparaenmascarar
# anonymize
dataparaenmascarar$RutEmpresaMask <- anonymizer::anonymize(.x=dataparaenmascarar$RutEmpresaMask, .algo = "sha256", .seed = 0, .n_chars=8L)
head(dataparaenmascarar, n=20L)
'
        IDFiscalizacion CodOficina Agno NroComision TotalAfectados urgencia SolEsAfectado CodTipoSol
1175671         2387019       1311 2014        3639           <NA>        0             1          5
459169          1538817       1309 2008         738           <NA>        0             1          5
748933          1870289       1322 2010        4175           <NA>        0             1          5
398670          1471322       1301 2007        6670             15        0          <NA>          0
476630          1558287       1301 2008        3688             30        1          <NA>          0
582713          1679519       1316 2009         723             33        0             1          4
667819          1776537       1316 2010         222              1        1             1          1
676401          1786322       1316 2010         454              1        0             1          1
683634          1794703       1316 2010         578              1        1          <NA>          2
701210          1814521       1316 2010         855              1        1             1          1
705616          1819506       1316 2010         921             22        1          <NA>          2
708014          1822292       1316 2010         959             22        1          <NA>          2
719225          1835134       1316 2010        1144             22        1          <NA>          2
727980          1845637       1316 2010        1281              1        0             1          1
735361          1854160       1316 2010        1379              1        0             1          1
738320          1857664       1316 2010        1412             22        1          <NA>          2
745948          1866684       1316 2010        1550              1        1             1          1
759496          1882632       1316 2010        1713              1        1             1          1
771352          1897054       1316 2011         177             15        1             1          1
816297          1950155       1316 2011        1051              4        1          <NA>          2
solicitante CodUnidadOrigen        unidadOrigen CodTipoTermino         tipoTermino EgresoConMulta
1175671 Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
459169  Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
748933  Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
398670                 Por Programa               2       Fiscalización              9           Sin Multa              0
476630                 Por Programa               2       Fiscalización              9           Sin Multa              0
582713             No se Identifica               1 Atención de Público             10           Con Multa              1
667819                 Trabajadores               1 Atención de Público              9           Sin Multa              0
676401                 Trabajadores               1 Atención de Público             10           Con Multa              1
683634  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
701210                 Trabajadores               1 Atención de Público             10           Con Multa              1
705616  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
708014  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
719225  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
727980                 Trabajadores               1 Atención de Público             10           Con Multa              1
735361                 Trabajadores               1 Atención de Público             10           Con Multa              1
738320  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
745948                 Trabajadores               1 Atención de Público              9           Sin Multa              0
759496                 Trabajadores               1 Atención de Público             10           Con Multa              1
771352                 Trabajadores               1 Atención de Público             10           Con Multa              1
816297  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
RutEmpresa EmpDFCodComuna EmpDMCodComuna EmpTrabHombres CodCae CodTipoEmpresa
1175671      11780          13101              0              1    738              4
459169       23294          13101           <NA>              2    906              1
748933       28282          13114           <NA>              1      1              4
398670       37229          13101          13101             15   1132              1
476630       37229          13101          13101             14   1132              4
582713       37229          13110           <NA>             34   1132              4
667819       37229          13110          13110             30   1132              4
676401       37229          13110           <NA>             12   1131              4
683634       37229          13110           <NA>             29   1132              1
701210       37229          13110           <NA>             29   1132              4
705616       37229          13110           <NA>             28   1132              4
708014       37229          13110           <NA>             28   1132              4
719225       37229          13110          13110             29   1132              1
727980       37229          13110           <NA>             31   1132              1
735361       37229          13110           <NA>             31   1131              4
738320       37229          13110           <NA>             31   1131              4
745948       37229          13110           <NA>             31   1132              4
759496       37229          13110           <NA>             29   1132              1
771352       37229          13110          13110             28   1132              4
816297       37229          13110           <NA>             27   1132              1
grupocodtipoMaterias
1175671                                                                   1         
459169                                                                    1         
748933                                                                    1         
398670                                   10        ,15        ,3         ,6         
476630                        10        ,15        ,3         ,3         ,6         
582713  15        ,3         ,3         ,7         ,7         ,9         ,9         
667819                                              15        ,3         ,9         
676401                        10        ,15        ,15        ,3         ,9         
683634                                                                    3         
701210                                   10        ,15        ,3         ,9         
705616                        10        ,15        ,15        ,6         ,9         
708014                        10        ,15        ,15        ,15        ,9         
719225                                              15        ,15        ,9         
727980             10        ,15        ,15        ,3         ,8         ,9         
735361             10        ,10        ,15        ,15        ,3         ,9         
738320                                              10        ,15        ,9         
745948                                   10        ,15        ,3         ,9         
759496                                              15        ,15        ,9         
771352                                              15        ,15        ,9         
816297                                                                    15        
grupoglosatipoMaterias
1175671                                                                                                                                                                     BENEFICIOS PREVISIONALES
459169                                                                                                                                                                      BENEFICIOS PREVISIONALES
748933                                                                                                                                                                      BENEFICIOS PREVISIONALES
398670                                                                                                             JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,COTIZACIONES PREVISIONALES
476630                                                                                         JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,CONTRATO DE TRABAJO,COTIZACIONES PREVISIONALES
582713  REMUNERACIONES,CONTRATO DE TRABAJO,CONTRATO DE TRABAJO,ACOSO SEXUAL, SIMULACIÓN, SUBTERFUGIO Y OTROS.,ACOSO SEXUAL, SIMULACIÓN, SUBTERFUGIO Y OTROS.,HIGIENE Y SEGURIDAD,HIGIENE Y SEGURIDAD
667819                                                                                                                                        REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
676401                                                                                                     JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
683634                                                                                                                                                                           CONTRATO DE TRABAJO
701210                                                                                                                    JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
705616                                                                                              JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,COTIZACIONES PREVISIONALES,HIGIENE Y SEGURIDAD
708014                                                                                                          JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
719225                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
727980                                                                                  JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,FERIADO Y PERMISOS,HIGIENE Y SEGURIDAD
735361                                                                                 JORNADA Y DESCANSOS,JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
738320                                                                                                                                        JORNADA Y DESCANSOS,REMUNERACIONES,HIGIENE Y SEGURIDAD
745948                                                                                                                    JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
759496                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
771352                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
816297                                                                                                                                                                                REMUNERACIONES
grupoglosaInfra      grupocodigoInfra
1175671                                      99                    99
459169                                       99                    99
748933                                       99                    99
398670                                       99                    99
476630                                       99                    99
582713  HIGIENE Y SEGURIDAD,HIGIENE Y SEGURIDAD 9         ,9         
667819                                       99                    99
676401                      CONTRATO DE TRABAJO            3         
683634                      CONTRATO DE TRABAJO            3         
701210                           REMUNERACIONES            15        
705616                           REMUNERACIONES            15        
708014                           REMUNERACIONES            15        
719225            REMUNERACIONES,REMUNERACIONES 15        ,15        
727980                           REMUNERACIONES            15        
735361                           REMUNERACIONES            15        
738320                                       99                    99
745948                           REMUNERACIONES            15        
759496                           REMUNERACIONES            15        
771352            REMUNERACIONES,REMUNERACIONES 15        ,15        
816297                           REMUNERACIONES            15        
grupoglosaInfra_det grupocodigoInfra_det
1175671                                                                                               99                   99
459169                                                                                                99                   99
748933                                                                                                99                   99
398670                                                                                                99                   99
476630                                                                                                99                   99
582713  || COMITÉ PARITARIO/NO EFECTUAR REUNIÓN OBLIGATORIA || NO CUMPLIR CONDICIONES SANITARIAS BÁSICAS        || 403 || 407
667819                                                                                                99                   99
676401                                                            || MODIFICACIÓN UNILATERAL CLÁUSULA(S)               || 515
683634                           || Articulo 12 C. del T/Cambio unilateral lugar/naturaleza de servicios               || 338
701210                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
705616                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
708014                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
719225                         || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE || NO PAGAR/INGRESO MÍNIMO        || 462 || 467
727980                                                                     || NO PAGAR/EN FORMA OPORTUNA               || 463
735361                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
738320                                                                                                99                   99
745948                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
759496                                                                     || NO PAGAR/EN FORMA OPORTUNA               || 463
771352                      || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE || NO PAGAR/EN FORMA OPORTUNA        || 462 || 463
816297                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
grupocodtipoMaterias2 grupoglosatipoMaterias2 grupoglosaInfra2 grupoglosaInfra2_det grupocodigoInfra2
1175671                    99                      99               99                   99                99
459169                     99                      99               99                   99                99
748933                     99                      99               99                   99                99
398670                     99                      99               99                   99                99
476630                     99                      99               99                   99                99
582713                     99                      99               99                   99                99
667819                     99                      99               99                   99                99
676401                     99                      99               99                   99                99
683634                     99                      99               99                   99                99
701210                     99                      99               99                   99                99
705616                     99                      99               99                   99                99
708014                     99                      99               99                   99                99
719225                     99                      99               99                   99                99
727980                     99                      99               99                   99                99
735361                     99                      99               99                   99                99
738320                     99                      99               99                   99                99
745948                     99                      99               99                   99                99
759496                     99                      99               99                   99                99
771352                     99                      99               99                   99                99
816297                     99                      99               99                   99                99
grupocodigoInfra2_det grupoCodigoNormaInfra2_det   ccae
1175671                    99                         99 289200
459169                     99                         99 522020
748933                     99                         99      0
398670                     99                         99 802100
476630                     99                         99 802100
582713                     99                         99 802100
667819                     99                         99 802100
676401                     99                         99 801020
683634                     99                         99 802100
701210                     99                         99 802100
705616                     99                         99 802100
708014                     99                         99 802100
719225                     99                         99 802100
727980                     99                         99 802100
735361                     99                         99 801020
738320                     99                         99 801020
745948                     99                         99 802100
759496                     99                         99 802100
771352                     99                         99 802100
816297                     99                         99 802100
gcae
1175671 Tratamientos y revestimientos de metales; obras de ingeniería mecánica en general realizadas a cambio de una retribución o contrata                                                                     
459169  Venta al por menor de carnes (rojas, blancas, otras) productos cárnicos y similares.                                                                                                                    
748933  ACTIVIDADES NO ESPECIFICADAS                                                                                                                                                                            
398670  Enseñanza secundaria de  formación general                                                                                                                                                              
476630  Enseñanza secundaria de  formación general                                                                                                                                                              
582713  Enseñanza secundaria de  formación general                                                                                                                                                              
667819  Enseñanza secundaria de  formación general                                                                                                                                                              
676401  Enseñanza primaria                                                                                                                                                                                      
683634  Enseñanza secundaria de  formación general                                                                                                                                                              
701210  Enseñanza secundaria de  formación general                                                                                                                                                              
705616  Enseñanza secundaria de  formación general                                                                                                                                                              
708014  Enseñanza secundaria de  formación general                                                                                                                                                              
719225  Enseñanza secundaria de  formación general                                                                                                                                                              
727980  Enseñanza secundaria de  formación general                                                                                                                                                              
735361  Enseñanza primaria                                                                                                                                                                                      
738320  Enseñanza primaria                                                                                                                                                                                      
745948  Enseñanza secundaria de  formación general                                                                                                                                                              
759496  Enseñanza secundaria de  formación general                                                                                                                                                              
771352  Enseñanza secundaria de  formación general                                                                                                                                                              
816297  Enseñanza secundaria de  formación general                                                                                                                                                              
crae                                                                             grae noInfra Infra derechoFund
1175671  104                                                        Industrias manufactureras       1     0           0
459169   107                                                                         Comercio       1     0           0
748933   100 ACTIVIDADES NO ESPECIFICADAS Y OTRAS                                                   1     0           0
398670   113                                                                        Enseñanza       4     0           0
476630   113                                                                        Enseñanza       5     0           0
582713   113                                                                        Enseñanza       5     2           0
667819   113                                                                        Enseñanza       3     0           0
676401   113                                                                        Enseñanza       4     1           0
683634   113                                                                        Enseñanza       0     1           0
701210   113                                                                        Enseñanza       3     1           0
705616   113                                                                        Enseñanza       4     1           0
708014   113                                                                        Enseñanza       4     1           0
719225   113                                                                        Enseñanza       1     2           0
727980   113                                                                        Enseñanza       5     1           0
735361   113                                                                        Enseñanza       5     1           0
738320   113                                                                        Enseñanza       3     0           0
745948   113                                                                        Enseñanza       3     1           0
759496   113                                                                        Enseñanza       2     1           0
771352   113                                                                        Enseñanza       1     2           0
816297   113                                                                        Enseñanza       0     1           0
num_materias num_sind Region Infractor Exsind    datereg mesreg
1175671            1     <NA>     13         0     NA 2014-11-20     11
459169             1     <NA>     13         0     NA 2008-04-11      4
748933             1     <NA>     13         0     NA 2010-11-22     11
398670             4        3     13         0      1 2007-10-24     10
476630             5        3     13         0      1 2008-06-10      6
582713             7        3     13         1      1 2009-05-26      5
667819             3        3     13         0      1 2010-02-24      2
676401             5        3     13         1      1 2010-03-29      3
683634             1        3     13         1      1 2010-04-21      4
701210             4        3     13         1      1 2010-06-14      6
705616             5        3     13         1      1 2010-06-30      6
708014             5        3     13         1      1 2010-07-08      7
719225             3        3     13         1      1 2010-08-13      8
727980             6        3     13         1      1 2010-09-13      9
735361             6        3     13         1      1 2010-10-06     10
738320             3        3     13         0      1 2010-10-15     10
745948             4        3     13         1      1 2010-11-11     11
759496             3        3     13         1      1 2010-12-30     12
771352             3        3     13         1      1 2011-02-22      2
816297             1        3     13         1      1 2011-07-26      7
RutEmpresaMask
1175671 8111fdc7862f3d71561f07a2953b394e32d684aec901aab92fee3d2fc1fe91f1
459169  452428eb95d3f66d85ea302684e237f95e46f73069f057f14b950dc27cb6a833
748933  5579eb56fc21504bc4717231e5e0447cd41226c2036624e38f9f0583cdd62daf
398670  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
476630  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
582713  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
667819  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
676401  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
683634  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
701210  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
705616  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
708014  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
719225  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
727980  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
735361  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
738320  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
745948  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
759496  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
771352  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
816297  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
'

# forma alternativa, datos quedan en salida
dataparaenmascarar$RutEmpresaAnonymized<-dataparaenmascarar$RutEmpresa
dataparaenmascarar$RutEmpresaAnonymized <- as.character(dataparaenmascarar$RutEmpresaAnonymized)
salida<-plyr::mutate(.data=dataparaenmascarar, RutEmpresaAnonymized = anonymizer::anonymize(.x=RutEmpresa, .algo = "sha256", .seed = 0, .n_chars=8L))
head(dataparaenmascarar, n=20L)
'
        IDFiscalizacion CodOficina Agno NroComision TotalAfectados urgencia SolEsAfectado CodTipoSol
1175671         2387019       1311 2014        3639           <NA>        0             1          5
459169          1538817       1309 2008         738           <NA>        0             1          5
748933          1870289       1322 2010        4175           <NA>        0             1          5
398670          1471322       1301 2007        6670             15        0          <NA>          0
476630          1558287       1301 2008        3688             30        1          <NA>          0
582713          1679519       1316 2009         723             33        0             1          4
667819          1776537       1316 2010         222              1        1             1          1
676401          1786322       1316 2010         454              1        0             1          1
683634          1794703       1316 2010         578              1        1          <NA>          2
701210          1814521       1316 2010         855              1        1             1          1
705616          1819506       1316 2010         921             22        1          <NA>          2
708014          1822292       1316 2010         959             22        1          <NA>          2
719225          1835134       1316 2010        1144             22        1          <NA>          2
727980          1845637       1316 2010        1281              1        0             1          1
735361          1854160       1316 2010        1379              1        0             1          1
738320          1857664       1316 2010        1412             22        1          <NA>          2
745948          1866684       1316 2010        1550              1        1             1          1
759496          1882632       1316 2010        1713              1        1             1          1
771352          1897054       1316 2011         177             15        1             1          1
816297          1950155       1316 2011        1051              4        1          <NA>          2
solicitante CodUnidadOrigen        unidadOrigen CodTipoTermino         tipoTermino EgresoConMulta
1175671 Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
459169  Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
748933  Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
398670                 Por Programa               2       Fiscalización              9           Sin Multa              0
476630                 Por Programa               2       Fiscalización              9           Sin Multa              0
582713             No se Identifica               1 Atención de Público             10           Con Multa              1
667819                 Trabajadores               1 Atención de Público              9           Sin Multa              0
676401                 Trabajadores               1 Atención de Público             10           Con Multa              1
683634  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
701210                 Trabajadores               1 Atención de Público             10           Con Multa              1
705616  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
708014  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
719225  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
727980                 Trabajadores               1 Atención de Público             10           Con Multa              1
735361                 Trabajadores               1 Atención de Público             10           Con Multa              1
738320  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
745948                 Trabajadores               1 Atención de Público              9           Sin Multa              0
759496                 Trabajadores               1 Atención de Público             10           Con Multa              1
771352                 Trabajadores               1 Atención de Público             10           Con Multa              1
816297  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
RutEmpresa EmpDFCodComuna EmpDMCodComuna EmpTrabHombres CodCae CodTipoEmpresa
1175671      11780          13101              0              1    738              4
459169       23294          13101           <NA>              2    906              1
748933       28282          13114           <NA>              1      1              4
398670       37229          13101          13101             15   1132              1
476630       37229          13101          13101             14   1132              4
582713       37229          13110           <NA>             34   1132              4
667819       37229          13110          13110             30   1132              4
676401       37229          13110           <NA>             12   1131              4
683634       37229          13110           <NA>             29   1132              1
701210       37229          13110           <NA>             29   1132              4
705616       37229          13110           <NA>             28   1132              4
708014       37229          13110           <NA>             28   1132              4
719225       37229          13110          13110             29   1132              1
727980       37229          13110           <NA>             31   1132              1
735361       37229          13110           <NA>             31   1131              4
738320       37229          13110           <NA>             31   1131              4
745948       37229          13110           <NA>             31   1132              4
759496       37229          13110           <NA>             29   1132              1
771352       37229          13110          13110             28   1132              4
816297       37229          13110           <NA>             27   1132              1
grupocodtipoMaterias
1175671                                                                   1         
459169                                                                    1         
748933                                                                    1         
398670                                   10        ,15        ,3         ,6         
476630                        10        ,15        ,3         ,3         ,6         
582713  15        ,3         ,3         ,7         ,7         ,9         ,9         
667819                                              15        ,3         ,9         
676401                        10        ,15        ,15        ,3         ,9         
683634                                                                    3         
701210                                   10        ,15        ,3         ,9         
705616                        10        ,15        ,15        ,6         ,9         
708014                        10        ,15        ,15        ,15        ,9         
719225                                              15        ,15        ,9         
727980             10        ,15        ,15        ,3         ,8         ,9         
735361             10        ,10        ,15        ,15        ,3         ,9         
738320                                              10        ,15        ,9         
745948                                   10        ,15        ,3         ,9         
759496                                              15        ,15        ,9         
771352                                              15        ,15        ,9         
816297                                                                    15        
grupoglosatipoMaterias
1175671                                                                                                                                                                     BENEFICIOS PREVISIONALES
459169                                                                                                                                                                      BENEFICIOS PREVISIONALES
748933                                                                                                                                                                      BENEFICIOS PREVISIONALES
398670                                                                                                             JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,COTIZACIONES PREVISIONALES
476630                                                                                         JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,CONTRATO DE TRABAJO,COTIZACIONES PREVISIONALES
582713  REMUNERACIONES,CONTRATO DE TRABAJO,CONTRATO DE TRABAJO,ACOSO SEXUAL, SIMULACIÓN, SUBTERFUGIO Y OTROS.,ACOSO SEXUAL, SIMULACIÓN, SUBTERFUGIO Y OTROS.,HIGIENE Y SEGURIDAD,HIGIENE Y SEGURIDAD
667819                                                                                                                                        REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
676401                                                                                                     JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
683634                                                                                                                                                                           CONTRATO DE TRABAJO
701210                                                                                                                    JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
705616                                                                                              JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,COTIZACIONES PREVISIONALES,HIGIENE Y SEGURIDAD
708014                                                                                                          JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
719225                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
727980                                                                                  JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,FERIADO Y PERMISOS,HIGIENE Y SEGURIDAD
735361                                                                                 JORNADA Y DESCANSOS,JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
738320                                                                                                                                        JORNADA Y DESCANSOS,REMUNERACIONES,HIGIENE Y SEGURIDAD
745948                                                                                                                    JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
759496                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
771352                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
816297                                                                                                                                                                                REMUNERACIONES
grupoglosaInfra      grupocodigoInfra
1175671                                      99                    99
459169                                       99                    99
748933                                       99                    99
398670                                       99                    99
476630                                       99                    99
582713  HIGIENE Y SEGURIDAD,HIGIENE Y SEGURIDAD 9         ,9         
667819                                       99                    99
676401                      CONTRATO DE TRABAJO            3         
683634                      CONTRATO DE TRABAJO            3         
701210                           REMUNERACIONES            15        
705616                           REMUNERACIONES            15        
708014                           REMUNERACIONES            15        
719225            REMUNERACIONES,REMUNERACIONES 15        ,15        
727980                           REMUNERACIONES            15        
735361                           REMUNERACIONES            15        
738320                                       99                    99
745948                           REMUNERACIONES            15        
759496                           REMUNERACIONES            15        
771352            REMUNERACIONES,REMUNERACIONES 15        ,15        
816297                           REMUNERACIONES            15        
grupoglosaInfra_det grupocodigoInfra_det
1175671                                                                                               99                   99
459169                                                                                                99                   99
748933                                                                                                99                   99
398670                                                                                                99                   99
476630                                                                                                99                   99
582713  || COMITÉ PARITARIO/NO EFECTUAR REUNIÓN OBLIGATORIA || NO CUMPLIR CONDICIONES SANITARIAS BÁSICAS        || 403 || 407
667819                                                                                                99                   99
676401                                                            || MODIFICACIÓN UNILATERAL CLÁUSULA(S)               || 515
683634                           || Articulo 12 C. del T/Cambio unilateral lugar/naturaleza de servicios               || 338
701210                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
705616                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
708014                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
719225                         || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE || NO PAGAR/INGRESO MÍNIMO        || 462 || 467
727980                                                                     || NO PAGAR/EN FORMA OPORTUNA               || 463
735361                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
738320                                                                                                99                   99
745948                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
759496                                                                     || NO PAGAR/EN FORMA OPORTUNA               || 463
771352                      || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE || NO PAGAR/EN FORMA OPORTUNA        || 462 || 463
816297                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
grupocodtipoMaterias2 grupoglosatipoMaterias2 grupoglosaInfra2 grupoglosaInfra2_det grupocodigoInfra2
1175671                    99                      99               99                   99                99
459169                     99                      99               99                   99                99
748933                     99                      99               99                   99                99
398670                     99                      99               99                   99                99
476630                     99                      99               99                   99                99
582713                     99                      99               99                   99                99
667819                     99                      99               99                   99                99
676401                     99                      99               99                   99                99
683634                     99                      99               99                   99                99
701210                     99                      99               99                   99                99
705616                     99                      99               99                   99                99
708014                     99                      99               99                   99                99
719225                     99                      99               99                   99                99
727980                     99                      99               99                   99                99
735361                     99                      99               99                   99                99
738320                     99                      99               99                   99                99
745948                     99                      99               99                   99                99
759496                     99                      99               99                   99                99
771352                     99                      99               99                   99                99
816297                     99                      99               99                   99                99
grupocodigoInfra2_det grupoCodigoNormaInfra2_det   ccae
1175671                    99                         99 289200
459169                     99                         99 522020
748933                     99                         99      0
398670                     99                         99 802100
476630                     99                         99 802100
582713                     99                         99 802100
667819                     99                         99 802100
676401                     99                         99 801020
683634                     99                         99 802100
701210                     99                         99 802100
705616                     99                         99 802100
708014                     99                         99 802100
719225                     99                         99 802100
727980                     99                         99 802100
735361                     99                         99 801020
738320                     99                         99 801020
745948                     99                         99 802100
759496                     99                         99 802100
771352                     99                         99 802100
816297                     99                         99 802100
gcae
1175671 Tratamientos y revestimientos de metales; obras de ingeniería mecánica en general realizadas a cambio de una retribución o contrata                                                                     
459169  Venta al por menor de carnes (rojas, blancas, otras) productos cárnicos y similares.                                                                                                                    
748933  ACTIVIDADES NO ESPECIFICADAS                                                                                                                                                                            
398670  Enseñanza secundaria de  formación general                                                                                                                                                              
476630  Enseñanza secundaria de  formación general                                                                                                                                                              
582713  Enseñanza secundaria de  formación general                                                                                                                                                              
667819  Enseñanza secundaria de  formación general                                                                                                                                                              
676401  Enseñanza primaria                                                                                                                                                                                      
683634  Enseñanza secundaria de  formación general                                                                                                                                                              
701210  Enseñanza secundaria de  formación general                                                                                                                                                              
705616  Enseñanza secundaria de  formación general                                                                                                                                                              
708014  Enseñanza secundaria de  formación general                                                                                                                                                              
719225  Enseñanza secundaria de  formación general                                                                                                                                                              
727980  Enseñanza secundaria de  formación general                                                                                                                                                              
735361  Enseñanza primaria                                                                                                                                                                                      
738320  Enseñanza primaria                                                                                                                                                                                      
745948  Enseñanza secundaria de  formación general                                                                                                                                                              
759496  Enseñanza secundaria de  formación general                                                                                                                                                              
771352  Enseñanza secundaria de  formación general                                                                                                                                                              
816297  Enseñanza secundaria de  formación general                                                                                                                                                              
crae                                                                             grae noInfra Infra derechoFund
1175671  104                                                        Industrias manufactureras       1     0           0
459169   107                                                                         Comercio       1     0           0
748933   100 ACTIVIDADES NO ESPECIFICADAS Y OTRAS                                                   1     0           0
398670   113                                                                        Enseñanza       4     0           0
476630   113                                                                        Enseñanza       5     0           0
582713   113                                                                        Enseñanza       5     2           0
667819   113                                                                        Enseñanza       3     0           0
676401   113                                                                        Enseñanza       4     1           0
683634   113                                                                        Enseñanza       0     1           0
701210   113                                                                        Enseñanza       3     1           0
705616   113                                                                        Enseñanza       4     1           0
708014   113                                                                        Enseñanza       4     1           0
719225   113                                                                        Enseñanza       1     2           0
727980   113                                                                        Enseñanza       5     1           0
735361   113                                                                        Enseñanza       5     1           0
738320   113                                                                        Enseñanza       3     0           0
745948   113                                                                        Enseñanza       3     1           0
759496   113                                                                        Enseñanza       2     1           0
771352   113                                                                        Enseñanza       1     2           0
816297   113                                                                        Enseñanza       0     1           0
num_materias num_sind Region Infractor Exsind    datereg mesreg
1175671            1     <NA>     13         0     NA 2014-11-20     11
459169             1     <NA>     13         0     NA 2008-04-11      4
748933             1     <NA>     13         0     NA 2010-11-22     11
398670             4        3     13         0      1 2007-10-24     10
476630             5        3     13         0      1 2008-06-10      6
582713             7        3     13         1      1 2009-05-26      5
667819             3        3     13         0      1 2010-02-24      2
676401             5        3     13         1      1 2010-03-29      3
683634             1        3     13         1      1 2010-04-21      4
701210             4        3     13         1      1 2010-06-14      6
705616             5        3     13         1      1 2010-06-30      6
708014             5        3     13         1      1 2010-07-08      7
719225             3        3     13         1      1 2010-08-13      8
727980             6        3     13         1      1 2010-09-13      9
735361             6        3     13         1      1 2010-10-06     10
738320             3        3     13         0      1 2010-10-15     10
745948             4        3     13         1      1 2010-11-11     11
759496             3        3     13         1      1 2010-12-30     12
771352             3        3     13         1      1 2011-02-22      2
816297             1        3     13         1      1 2011-07-26      7
RutEmpresaMask RutEmpresaAnonymized
1175671 8111fdc7862f3d71561f07a2953b394e32d684aec901aab92fee3d2fc1fe91f1                11780
459169  452428eb95d3f66d85ea302684e237f95e46f73069f057f14b950dc27cb6a833                23294
748933  5579eb56fc21504bc4717231e5e0447cd41226c2036624e38f9f0583cdd62daf                28282
398670  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
476630  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
582713  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
667819  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
676401  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
683634  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
701210  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
705616  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
708014  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
719225  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
727980  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
735361  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
738320  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
745948  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
759496  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
771352  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
816297  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce                37229
'
head(salida, n=20L)
'
        IDFiscalizacion CodOficina Agno NroComision TotalAfectados urgencia SolEsAfectado CodTipoSol
1175671         2387019       1311 2014        3639           <NA>        0             1          5
459169          1538817       1309 2008         738           <NA>        0             1          5
748933          1870289       1322 2010        4175           <NA>        0             1          5
398670          1471322       1301 2007        6670             15        0          <NA>          0
476630          1558287       1301 2008        3688             30        1          <NA>          0
582713          1679519       1316 2009         723             33        0             1          4
667819          1776537       1316 2010         222              1        1             1          1
676401          1786322       1316 2010         454              1        0             1          1
683634          1794703       1316 2010         578              1        1          <NA>          2
701210          1814521       1316 2010         855              1        1             1          1
705616          1819506       1316 2010         921             22        1          <NA>          2
708014          1822292       1316 2010         959             22        1          <NA>          2
719225          1835134       1316 2010        1144             22        1          <NA>          2
727980          1845637       1316 2010        1281              1        0             1          1
735361          1854160       1316 2010        1379              1        0             1          1
738320          1857664       1316 2010        1412             22        1          <NA>          2
745948          1866684       1316 2010        1550              1        1             1          1
759496          1882632       1316 2010        1713              1        1             1          1
771352          1897054       1316 2011         177             15        1             1          1
816297          1950155       1316 2011        1051              4        1          <NA>          2
solicitante CodUnidadOrigen        unidadOrigen CodTipoTermino         tipoTermino EgresoConMulta
1175671 Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
459169  Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
748933  Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
398670                 Por Programa               2       Fiscalización              9           Sin Multa              0
476630                 Por Programa               2       Fiscalización              9           Sin Multa              0
582713             No se Identifica               1 Atención de Público             10           Con Multa              1
667819                 Trabajadores               1 Atención de Público              9           Sin Multa              0
676401                 Trabajadores               1 Atención de Público             10           Con Multa              1
683634  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
701210                 Trabajadores               1 Atención de Público             10           Con Multa              1
705616  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
708014  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
719225  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
727980                 Trabajadores               1 Atención de Público             10           Con Multa              1
735361                 Trabajadores               1 Atención de Público             10           Con Multa              1
738320  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
745948                 Trabajadores               1 Atención de Público              9           Sin Multa              0
759496                 Trabajadores               1 Atención de Público             10           Con Multa              1
771352                 Trabajadores               1 Atención de Público             10           Con Multa              1
816297  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
RutEmpresa EmpDFCodComuna EmpDMCodComuna EmpTrabHombres CodCae CodTipoEmpresa
1175671      11780          13101              0              1    738              4
459169       23294          13101           <NA>              2    906              1
748933       28282          13114           <NA>              1      1              4
398670       37229          13101          13101             15   1132              1
476630       37229          13101          13101             14   1132              4
582713       37229          13110           <NA>             34   1132              4
667819       37229          13110          13110             30   1132              4
676401       37229          13110           <NA>             12   1131              4
683634       37229          13110           <NA>             29   1132              1
701210       37229          13110           <NA>             29   1132              4
705616       37229          13110           <NA>             28   1132              4
708014       37229          13110           <NA>             28   1132              4
719225       37229          13110          13110             29   1132              1
727980       37229          13110           <NA>             31   1132              1
735361       37229          13110           <NA>             31   1131              4
738320       37229          13110           <NA>             31   1131              4
745948       37229          13110           <NA>             31   1132              4
759496       37229          13110           <NA>             29   1132              1
771352       37229          13110          13110             28   1132              4
816297       37229          13110           <NA>             27   1132              1
grupocodtipoMaterias
1175671                                                                   1         
459169                                                                    1         
748933                                                                    1         
398670                                   10        ,15        ,3         ,6         
476630                        10        ,15        ,3         ,3         ,6         
582713  15        ,3         ,3         ,7         ,7         ,9         ,9         
667819                                              15        ,3         ,9         
676401                        10        ,15        ,15        ,3         ,9         
683634                                                                    3         
701210                                   10        ,15        ,3         ,9         
705616                        10        ,15        ,15        ,6         ,9         
708014                        10        ,15        ,15        ,15        ,9         
719225                                              15        ,15        ,9         
727980             10        ,15        ,15        ,3         ,8         ,9         
735361             10        ,10        ,15        ,15        ,3         ,9         
738320                                              10        ,15        ,9         
745948                                   10        ,15        ,3         ,9         
759496                                              15        ,15        ,9         
771352                                              15        ,15        ,9         
816297                                                                    15        
grupoglosatipoMaterias
1175671                                                                                                                                                                     BENEFICIOS PREVISIONALES
459169                                                                                                                                                                      BENEFICIOS PREVISIONALES
748933                                                                                                                                                                      BENEFICIOS PREVISIONALES
398670                                                                                                             JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,COTIZACIONES PREVISIONALES
476630                                                                                         JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,CONTRATO DE TRABAJO,COTIZACIONES PREVISIONALES
582713  REMUNERACIONES,CONTRATO DE TRABAJO,CONTRATO DE TRABAJO,ACOSO SEXUAL, SIMULACIÓN, SUBTERFUGIO Y OTROS.,ACOSO SEXUAL, SIMULACIÓN, SUBTERFUGIO Y OTROS.,HIGIENE Y SEGURIDAD,HIGIENE Y SEGURIDAD
667819                                                                                                                                        REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
676401                                                                                                     JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
683634                                                                                                                                                                           CONTRATO DE TRABAJO
701210                                                                                                                    JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
705616                                                                                              JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,COTIZACIONES PREVISIONALES,HIGIENE Y SEGURIDAD
708014                                                                                                          JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
719225                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
727980                                                                                  JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,FERIADO Y PERMISOS,HIGIENE Y SEGURIDAD
735361                                                                                 JORNADA Y DESCANSOS,JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
738320                                                                                                                                        JORNADA Y DESCANSOS,REMUNERACIONES,HIGIENE Y SEGURIDAD
745948                                                                                                                    JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
759496                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
771352                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
816297                                                                                                                                                                                REMUNERACIONES
grupoglosaInfra      grupocodigoInfra
1175671                                      99                    99
459169                                       99                    99
748933                                       99                    99
398670                                       99                    99
476630                                       99                    99
582713  HIGIENE Y SEGURIDAD,HIGIENE Y SEGURIDAD 9         ,9         
667819                                       99                    99
676401                      CONTRATO DE TRABAJO            3         
683634                      CONTRATO DE TRABAJO            3         
701210                           REMUNERACIONES            15        
705616                           REMUNERACIONES            15        
708014                           REMUNERACIONES            15        
719225            REMUNERACIONES,REMUNERACIONES 15        ,15        
727980                           REMUNERACIONES            15        
735361                           REMUNERACIONES            15        
738320                                       99                    99
745948                           REMUNERACIONES            15        
759496                           REMUNERACIONES            15        
771352            REMUNERACIONES,REMUNERACIONES 15        ,15        
816297                           REMUNERACIONES            15        
grupoglosaInfra_det grupocodigoInfra_det
1175671                                                                                               99                   99
459169                                                                                                99                   99
748933                                                                                                99                   99
398670                                                                                                99                   99
476630                                                                                                99                   99
582713  || COMITÉ PARITARIO/NO EFECTUAR REUNIÓN OBLIGATORIA || NO CUMPLIR CONDICIONES SANITARIAS BÁSICAS        || 403 || 407
667819                                                                                                99                   99
676401                                                            || MODIFICACIÓN UNILATERAL CLÁUSULA(S)               || 515
683634                           || Articulo 12 C. del T/Cambio unilateral lugar/naturaleza de servicios               || 338
701210                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
705616                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
708014                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
719225                         || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE || NO PAGAR/INGRESO MÍNIMO        || 462 || 467
727980                                                                     || NO PAGAR/EN FORMA OPORTUNA               || 463
735361                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
738320                                                                                                99                   99
745948                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
759496                                                                     || NO PAGAR/EN FORMA OPORTUNA               || 463
771352                      || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE || NO PAGAR/EN FORMA OPORTUNA        || 462 || 463
816297                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
grupocodtipoMaterias2 grupoglosatipoMaterias2 grupoglosaInfra2 grupoglosaInfra2_det grupocodigoInfra2
1175671                    99                      99               99                   99                99
459169                     99                      99               99                   99                99
748933                     99                      99               99                   99                99
398670                     99                      99               99                   99                99
476630                     99                      99               99                   99                99
582713                     99                      99               99                   99                99
667819                     99                      99               99                   99                99
676401                     99                      99               99                   99                99
683634                     99                      99               99                   99                99
701210                     99                      99               99                   99                99
705616                     99                      99               99                   99                99
708014                     99                      99               99                   99                99
719225                     99                      99               99                   99                99
727980                     99                      99               99                   99                99
735361                     99                      99               99                   99                99
738320                     99                      99               99                   99                99
745948                     99                      99               99                   99                99
759496                     99                      99               99                   99                99
771352                     99                      99               99                   99                99
816297                     99                      99               99                   99                99
grupocodigoInfra2_det grupoCodigoNormaInfra2_det   ccae
1175671                    99                         99 289200
459169                     99                         99 522020
748933                     99                         99      0
398670                     99                         99 802100
476630                     99                         99 802100
582713                     99                         99 802100
667819                     99                         99 802100
676401                     99                         99 801020
683634                     99                         99 802100
701210                     99                         99 802100
705616                     99                         99 802100
708014                     99                         99 802100
719225                     99                         99 802100
727980                     99                         99 802100
735361                     99                         99 801020
738320                     99                         99 801020
745948                     99                         99 802100
759496                     99                         99 802100
771352                     99                         99 802100
816297                     99                         99 802100
gcae
1175671 Tratamientos y revestimientos de metales; obras de ingeniería mecánica en general realizadas a cambio de una retribución o contrata                                                                     
459169  Venta al por menor de carnes (rojas, blancas, otras) productos cárnicos y similares.                                                                                                                    
748933  ACTIVIDADES NO ESPECIFICADAS                                                                                                                                                                            
398670  Enseñanza secundaria de  formación general                                                                                                                                                              
476630  Enseñanza secundaria de  formación general                                                                                                                                                              
582713  Enseñanza secundaria de  formación general                                                                                                                                                              
667819  Enseñanza secundaria de  formación general                                                                                                                                                              
676401  Enseñanza primaria                                                                                                                                                                                      
683634  Enseñanza secundaria de  formación general                                                                                                                                                              
701210  Enseñanza secundaria de  formación general                                                                                                                                                              
705616  Enseñanza secundaria de  formación general                                                                                                                                                              
708014  Enseñanza secundaria de  formación general                                                                                                                                                              
719225  Enseñanza secundaria de  formación general                                                                                                                                                              
727980  Enseñanza secundaria de  formación general                                                                                                                                                              
735361  Enseñanza primaria                                                                                                                                                                                      
738320  Enseñanza primaria                                                                                                                                                                                      
745948  Enseñanza secundaria de  formación general                                                                                                                                                              
759496  Enseñanza secundaria de  formación general                                                                                                                                                              
771352  Enseñanza secundaria de  formación general                                                                                                                                                              
816297  Enseñanza secundaria de  formación general                                                                                                                                                              
crae                                                                             grae noInfra Infra derechoFund
1175671  104                                                        Industrias manufactureras       1     0           0
459169   107                                                                         Comercio       1     0           0
748933   100 ACTIVIDADES NO ESPECIFICADAS Y OTRAS                                                   1     0           0
398670   113                                                                        Enseñanza       4     0           0
476630   113                                                                        Enseñanza       5     0           0
582713   113                                                                        Enseñanza       5     2           0
667819   113                                                                        Enseñanza       3     0           0
676401   113                                                                        Enseñanza       4     1           0
683634   113                                                                        Enseñanza       0     1           0
701210   113                                                                        Enseñanza       3     1           0
705616   113                                                                        Enseñanza       4     1           0
708014   113                                                                        Enseñanza       4     1           0
719225   113                                                                        Enseñanza       1     2           0
727980   113                                                                        Enseñanza       5     1           0
735361   113                                                                        Enseñanza       5     1           0
738320   113                                                                        Enseñanza       3     0           0
745948   113                                                                        Enseñanza       3     1           0
759496   113                                                                        Enseñanza       2     1           0
771352   113                                                                        Enseñanza       1     2           0
816297   113                                                                        Enseñanza       0     1           0
num_materias num_sind Region Infractor Exsind    datereg mesreg
1175671            1     <NA>     13         0     NA 2014-11-20     11
459169             1     <NA>     13         0     NA 2008-04-11      4
748933             1     <NA>     13         0     NA 2010-11-22     11
398670             4        3     13         0      1 2007-10-24     10
476630             5        3     13         0      1 2008-06-10      6
582713             7        3     13         1      1 2009-05-26      5
667819             3        3     13         0      1 2010-02-24      2
676401             5        3     13         1      1 2010-03-29      3
683634             1        3     13         1      1 2010-04-21      4
701210             4        3     13         1      1 2010-06-14      6
705616             5        3     13         1      1 2010-06-30      6
708014             5        3     13         1      1 2010-07-08      7
719225             3        3     13         1      1 2010-08-13      8
727980             6        3     13         1      1 2010-09-13      9
735361             6        3     13         1      1 2010-10-06     10
738320             3        3     13         0      1 2010-10-15     10
745948             4        3     13         1      1 2010-11-11     11
759496             3        3     13         1      1 2010-12-30     12
771352             3        3     13         1      1 2011-02-22      2
816297             1        3     13         1      1 2011-07-26      7
RutEmpresaMask
1175671 8111fdc7862f3d71561f07a2953b394e32d684aec901aab92fee3d2fc1fe91f1
459169  452428eb95d3f66d85ea302684e237f95e46f73069f057f14b950dc27cb6a833
748933  5579eb56fc21504bc4717231e5e0447cd41226c2036624e38f9f0583cdd62daf
398670  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
476630  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
582713  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
667819  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
676401  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
683634  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
701210  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
705616  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
708014  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
719225  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
727980  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
735361  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
738320  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
745948  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
759496  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
771352  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
816297  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
RutEmpresaAnonymized
1175671 8111fdc7862f3d71561f07a2953b394e32d684aec901aab92fee3d2fc1fe91f1
459169  452428eb95d3f66d85ea302684e237f95e46f73069f057f14b950dc27cb6a833
748933  5579eb56fc21504bc4717231e5e0447cd41226c2036624e38f9f0583cdd62daf
398670  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
476630  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
582713  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
667819  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
676401  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
683634  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
701210  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
705616  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
708014  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
719225  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
727980  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
735361  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
738320  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
745948  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
759496  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
771352  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
816297  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
'
# ----

# grabado de datos a csv para entrega ----
datafinal <- dataparaenmascarar
datafinal$RutEmpresa <- NULL
datafinal$RutEmpresaAnonymized <- NULL
head(datafinal, n=20L)
'
        IDFiscalizacion CodOficina Agno NroComision TotalAfectados urgencia SolEsAfectado CodTipoSol
1175671         2387019       1311 2014        3639           <NA>        0             1          5
459169          1538817       1309 2008         738           <NA>        0             1          5
748933          1870289       1322 2010        4175           <NA>        0             1          5
398670          1471322       1301 2007        6670             15        0          <NA>          0
476630          1558287       1301 2008        3688             30        1          <NA>          0
582713          1679519       1316 2009         723             33        0             1          4
667819          1776537       1316 2010         222              1        1             1          1
676401          1786322       1316 2010         454              1        0             1          1
683634          1794703       1316 2010         578              1        1          <NA>          2
701210          1814521       1316 2010         855              1        1             1          1
705616          1819506       1316 2010         921             22        1          <NA>          2
708014          1822292       1316 2010         959             22        1          <NA>          2
719225          1835134       1316 2010        1144             22        1          <NA>          2
727980          1845637       1316 2010        1281              1        0             1          1
735361          1854160       1316 2010        1379              1        0             1          1
738320          1857664       1316 2010        1412             22        1          <NA>          2
745948          1866684       1316 2010        1550              1        1             1          1
759496          1882632       1316 2010        1713              1        1             1          1
771352          1897054       1316 2011         177             15        1             1          1
816297          1950155       1316 2011        1051              4        1          <NA>          2
solicitante CodUnidadOrigen        unidadOrigen CodTipoTermino         tipoTermino EgresoConMulta
1175671 Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
459169  Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
748933  Institucion Previsional                   2       Fiscalización             15 Beneficio Sin Multa              0
398670                 Por Programa               2       Fiscalización              9           Sin Multa              0
476630                 Por Programa               2       Fiscalización              9           Sin Multa              0
582713             No se Identifica               1 Atención de Público             10           Con Multa              1
667819                 Trabajadores               1 Atención de Público              9           Sin Multa              0
676401                 Trabajadores               1 Atención de Público             10           Con Multa              1
683634  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
701210                 Trabajadores               1 Atención de Público             10           Con Multa              1
705616  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
708014  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
719225  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
727980                 Trabajadores               1 Atención de Público             10           Con Multa              1
735361                 Trabajadores               1 Atención de Público             10           Con Multa              1
738320  Organizacion Sindical                     1 Atención de Público              9           Sin Multa              0
745948                 Trabajadores               1 Atención de Público              9           Sin Multa              0
759496                 Trabajadores               1 Atención de Público             10           Con Multa              1
771352                 Trabajadores               1 Atención de Público             10           Con Multa              1
816297  Organizacion Sindical                     1 Atención de Público             10           Con Multa              1
EmpDFCodComuna EmpDMCodComuna EmpTrabHombres CodCae CodTipoEmpresa
1175671          13101              0              1    738              4
459169           13101           <NA>              2    906              1
748933           13114           <NA>              1      1              4
398670           13101          13101             15   1132              1
476630           13101          13101             14   1132              4
582713           13110           <NA>             34   1132              4
667819           13110          13110             30   1132              4
676401           13110           <NA>             12   1131              4
683634           13110           <NA>             29   1132              1
701210           13110           <NA>             29   1132              4
705616           13110           <NA>             28   1132              4
708014           13110           <NA>             28   1132              4
719225           13110          13110             29   1132              1
727980           13110           <NA>             31   1132              1
735361           13110           <NA>             31   1131              4
738320           13110           <NA>             31   1131              4
745948           13110           <NA>             31   1132              4
759496           13110           <NA>             29   1132              1
771352           13110          13110             28   1132              4
816297           13110           <NA>             27   1132              1
grupocodtipoMaterias
1175671                                                                   1         
459169                                                                    1         
748933                                                                    1         
398670                                   10        ,15        ,3         ,6         
476630                        10        ,15        ,3         ,3         ,6         
582713  15        ,3         ,3         ,7         ,7         ,9         ,9         
667819                                              15        ,3         ,9         
676401                        10        ,15        ,15        ,3         ,9         
683634                                                                    3         
701210                                   10        ,15        ,3         ,9         
705616                        10        ,15        ,15        ,6         ,9         
708014                        10        ,15        ,15        ,15        ,9         
719225                                              15        ,15        ,9         
727980             10        ,15        ,15        ,3         ,8         ,9         
735361             10        ,10        ,15        ,15        ,3         ,9         
738320                                              10        ,15        ,9         
745948                                   10        ,15        ,3         ,9         
759496                                              15        ,15        ,9         
771352                                              15        ,15        ,9         
816297                                                                    15        
grupoglosatipoMaterias
1175671                                                                                                                                                                     BENEFICIOS PREVISIONALES
459169                                                                                                                                                                      BENEFICIOS PREVISIONALES
748933                                                                                                                                                                      BENEFICIOS PREVISIONALES
398670                                                                                                             JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,COTIZACIONES PREVISIONALES
476630                                                                                         JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,CONTRATO DE TRABAJO,COTIZACIONES PREVISIONALES
582713  REMUNERACIONES,CONTRATO DE TRABAJO,CONTRATO DE TRABAJO,ACOSO SEXUAL, SIMULACIÓN, SUBTERFUGIO Y OTROS.,ACOSO SEXUAL, SIMULACIÓN, SUBTERFUGIO Y OTROS.,HIGIENE Y SEGURIDAD,HIGIENE Y SEGURIDAD
667819                                                                                                                                        REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
676401                                                                                                     JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
683634                                                                                                                                                                           CONTRATO DE TRABAJO
701210                                                                                                                    JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
705616                                                                                              JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,COTIZACIONES PREVISIONALES,HIGIENE Y SEGURIDAD
708014                                                                                                          JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
719225                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
727980                                                                                  JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,FERIADO Y PERMISOS,HIGIENE Y SEGURIDAD
735361                                                                                 JORNADA Y DESCANSOS,JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
738320                                                                                                                                        JORNADA Y DESCANSOS,REMUNERACIONES,HIGIENE Y SEGURIDAD
745948                                                                                                                    JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
759496                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
771352                                                                                                                                             REMUNERACIONES,REMUNERACIONES,HIGIENE Y SEGURIDAD
816297                                                                                                                                                                                REMUNERACIONES
grupoglosaInfra      grupocodigoInfra
1175671                                      99                    99
459169                                       99                    99
748933                                       99                    99
398670                                       99                    99
476630                                       99                    99
582713  HIGIENE Y SEGURIDAD,HIGIENE Y SEGURIDAD 9         ,9         
667819                                       99                    99
676401                      CONTRATO DE TRABAJO            3         
683634                      CONTRATO DE TRABAJO            3         
701210                           REMUNERACIONES            15        
705616                           REMUNERACIONES            15        
708014                           REMUNERACIONES            15        
719225            REMUNERACIONES,REMUNERACIONES 15        ,15        
727980                           REMUNERACIONES            15        
735361                           REMUNERACIONES            15        
738320                                       99                    99
745948                           REMUNERACIONES            15        
759496                           REMUNERACIONES            15        
771352            REMUNERACIONES,REMUNERACIONES 15        ,15        
816297                           REMUNERACIONES            15        
grupoglosaInfra_det grupocodigoInfra_det
1175671                                                                                               99                   99
459169                                                                                                99                   99
748933                                                                                                99                   99
398670                                                                                                99                   99
476630                                                                                                99                   99
582713  || COMITÉ PARITARIO/NO EFECTUAR REUNIÓN OBLIGATORIA || NO CUMPLIR CONDICIONES SANITARIAS BÁSICAS        || 403 || 407
667819                                                                                                99                   99
676401                                                            || MODIFICACIÓN UNILATERAL CLÁUSULA(S)               || 515
683634                           || Articulo 12 C. del T/Cambio unilateral lugar/naturaleza de servicios               || 338
701210                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
705616                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
708014                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
719225                         || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE || NO PAGAR/INGRESO MÍNIMO        || 462 || 467
727980                                                                     || NO PAGAR/EN FORMA OPORTUNA               || 463
735361                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
738320                                                                                                99                   99
745948                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
759496                                                                     || NO PAGAR/EN FORMA OPORTUNA               || 463
771352                      || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE || NO PAGAR/EN FORMA OPORTUNA        || 462 || 463
816297                                                    || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE               || 462
grupocodtipoMaterias2 grupoglosatipoMaterias2 grupoglosaInfra2 grupoglosaInfra2_det grupocodigoInfra2
1175671                    99                      99               99                   99                99
459169                     99                      99               99                   99                99
748933                     99                      99               99                   99                99
398670                     99                      99               99                   99                99
476630                     99                      99               99                   99                99
582713                     99                      99               99                   99                99
667819                     99                      99               99                   99                99
676401                     99                      99               99                   99                99
683634                     99                      99               99                   99                99
701210                     99                      99               99                   99                99
705616                     99                      99               99                   99                99
708014                     99                      99               99                   99                99
719225                     99                      99               99                   99                99
727980                     99                      99               99                   99                99
735361                     99                      99               99                   99                99
738320                     99                      99               99                   99                99
745948                     99                      99               99                   99                99
759496                     99                      99               99                   99                99
771352                     99                      99               99                   99                99
816297                     99                      99               99                   99                99
grupocodigoInfra2_det grupoCodigoNormaInfra2_det   ccae
1175671                    99                         99 289200
459169                     99                         99 522020
748933                     99                         99      0
398670                     99                         99 802100
476630                     99                         99 802100
582713                     99                         99 802100
667819                     99                         99 802100
676401                     99                         99 801020
683634                     99                         99 802100
701210                     99                         99 802100
705616                     99                         99 802100
708014                     99                         99 802100
719225                     99                         99 802100
727980                     99                         99 802100
735361                     99                         99 801020
738320                     99                         99 801020
745948                     99                         99 802100
759496                     99                         99 802100
771352                     99                         99 802100
816297                     99                         99 802100
gcae
1175671 Tratamientos y revestimientos de metales; obras de ingeniería mecánica en general realizadas a cambio de una retribución o contrata                                                                     
459169  Venta al por menor de carnes (rojas, blancas, otras) productos cárnicos y similares.                                                                                                                    
748933  ACTIVIDADES NO ESPECIFICADAS                                                                                                                                                                            
398670  Enseñanza secundaria de  formación general                                                                                                                                                              
476630  Enseñanza secundaria de  formación general                                                                                                                                                              
582713  Enseñanza secundaria de  formación general                                                                                                                                                              
667819  Enseñanza secundaria de  formación general                                                                                                                                                              
676401  Enseñanza primaria                                                                                                                                                                                      
683634  Enseñanza secundaria de  formación general                                                                                                                                                              
701210  Enseñanza secundaria de  formación general                                                                                                                                                              
705616  Enseñanza secundaria de  formación general                                                                                                                                                              
708014  Enseñanza secundaria de  formación general                                                                                                                                                              
719225  Enseñanza secundaria de  formación general                                                                                                                                                              
727980  Enseñanza secundaria de  formación general                                                                                                                                                              
735361  Enseñanza primaria                                                                                                                                                                                      
738320  Enseñanza primaria                                                                                                                                                                                      
745948  Enseñanza secundaria de  formación general                                                                                                                                                              
759496  Enseñanza secundaria de  formación general                                                                                                                                                              
771352  Enseñanza secundaria de  formación general                                                                                                                                                              
816297  Enseñanza secundaria de  formación general                                                                                                                                                              
crae                                                                             grae noInfra Infra derechoFund
1175671  104                                                        Industrias manufactureras       1     0           0
459169   107                                                                         Comercio       1     0           0
748933   100 ACTIVIDADES NO ESPECIFICADAS Y OTRAS                                                   1     0           0
398670   113                                                                        Enseñanza       4     0           0
476630   113                                                                        Enseñanza       5     0           0
582713   113                                                                        Enseñanza       5     2           0
667819   113                                                                        Enseñanza       3     0           0
676401   113                                                                        Enseñanza       4     1           0
683634   113                                                                        Enseñanza       0     1           0
701210   113                                                                        Enseñanza       3     1           0
705616   113                                                                        Enseñanza       4     1           0
708014   113                                                                        Enseñanza       4     1           0
719225   113                                                                        Enseñanza       1     2           0
727980   113                                                                        Enseñanza       5     1           0
735361   113                                                                        Enseñanza       5     1           0
738320   113                                                                        Enseñanza       3     0           0
745948   113                                                                        Enseñanza       3     1           0
759496   113                                                                        Enseñanza       2     1           0
771352   113                                                                        Enseñanza       1     2           0
816297   113                                                                        Enseñanza       0     1           0
num_materias num_sind Region Infractor Exsind    datereg mesreg
1175671            1     <NA>     13         0     NA 2014-11-20     11
459169             1     <NA>     13         0     NA 2008-04-11      4
748933             1     <NA>     13         0     NA 2010-11-22     11
398670             4        3     13         0      1 2007-10-24     10
476630             5        3     13         0      1 2008-06-10      6
582713             7        3     13         1      1 2009-05-26      5
667819             3        3     13         0      1 2010-02-24      2
676401             5        3     13         1      1 2010-03-29      3
683634             1        3     13         1      1 2010-04-21      4
701210             4        3     13         1      1 2010-06-14      6
705616             5        3     13         1      1 2010-06-30      6
708014             5        3     13         1      1 2010-07-08      7
719225             3        3     13         1      1 2010-08-13      8
727980             6        3     13         1      1 2010-09-13      9
735361             6        3     13         1      1 2010-10-06     10
738320             3        3     13         0      1 2010-10-15     10
745948             4        3     13         1      1 2010-11-11     11
759496             3        3     13         1      1 2010-12-30     12
771352             3        3     13         1      1 2011-02-22      2
816297             1        3     13         1      1 2011-07-26      7
RutEmpresaMask
1175671 8111fdc7862f3d71561f07a2953b394e32d684aec901aab92fee3d2fc1fe91f1
459169  452428eb95d3f66d85ea302684e237f95e46f73069f057f14b950dc27cb6a833
748933  5579eb56fc21504bc4717231e5e0447cd41226c2036624e38f9f0583cdd62daf
398670  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
476630  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
582713  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
667819  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
676401  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
683634  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
701210  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
705616  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
708014  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
719225  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
727980  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
735361  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
738320  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
745948  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
759496  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
771352  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
816297  56c95c99ded7373344f1fa0576ef26ac7a429e8907d8b9f3833e2da7e9501fce
'
write.csv2(datafinal,file="datafinaluchicago_completa_todoeldataset.csv",na=".", row.names= TRUE)
# ----

colnames(datafinal)

'
 [1] "IDFiscalizacion"            "CodOficina"                 "Agno"                       "NroComision"               
 [5] "TotalAfectados"             "urgencia"                   "SolEsAfectado"              "CodTipoSol"                
[9] "solicitante"                "CodUnidadOrigen"            "unidadOrigen"               "CodTipoTermino"            
[13] "tipoTermino"                "EgresoConMulta"             "EmpDFCodComuna"             "EmpDMCodComuna"            
[17] "EmpTrabHombres"             "CodCae"                     "CodTipoEmpresa"             "grupocodtipoMaterias"      
[21] "grupoglosatipoMaterias"     "grupoglosaInfra"            "grupocodigoInfra"           "grupoglosaInfra_det"       
[25] "grupocodigoInfra_det"       "grupocodtipoMaterias2"      "grupoglosatipoMaterias2"    "grupoglosaInfra2"          
[29] "grupoglosaInfra2_det"       "grupocodigoInfra2"          "grupocodigoInfra2_det"      "grupoCodigoNormaInfra2_det"
[33] "ccae"                       "gcae"                       "crae"                       "grae"                      
[37] "noInfra"                    "Infra"                      "derechoFund"                "num_materias"              
[41] "num_sind"                   "Region"                     "Infractor"                  "Exsind"                    
[45] "datereg"                    "mesreg"                     "RutEmpresaMask" 
'
