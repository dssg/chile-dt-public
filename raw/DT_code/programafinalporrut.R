# inicio
# última actualización
Sys.time()
#[1] "2017-11-06 09:14:28 CLST"
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

# lectura datos 20170927
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

# pre pro 20170927
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
d <- subset(d, !is.na(d$noInfra))
nrow(d)
# [1] 1314225
apply(d, 2, pMiss)
'
           IDFiscalizacion                 CodOficina                       Agno 
              0.000000e+00               0.000000e+00               0.000000e+00 
               NroComision             TotalAfectados                   urgencia 
              0.000000e+00               1.740273e+01               2.071297e+01 
             SolEsAfectado              FechaRegistro                 CodTipoSol 
              4.106085e+01               1.369628e-03               0.000000e+00 
               solicitante            CodUnidadOrigen               unidadOrigen 
              0.000000e+00               0.000000e+00               0.000000e+00 
            CodTipoTermino                tipoTermino             EgresoConMulta 
              0.000000e+00               0.000000e+00               0.000000e+00 
                RutEmpresa             EmpDFCodComuna             EmpDMCodComuna 
              4.845441e-01               7.609047e-05               3.356784e+01 
            EmpTrabHombres                     CodCae             CodTipoEmpresa 
              2.679830e+00               1.341475e-01               1.648812e+01 
      grupocodtipoMaterias     grupoglosatipoMaterias            grupoglosaInfra 
              2.394164e+01               2.394164e+01               7.718062e+01 
          grupocodigoInfra        grupoglosaInfra_det       grupocodigoInfra_det 
              7.718062e+01               7.718062e+01               7.718062e+01 
     grupocodtipoMaterias2    grupoglosatipoMaterias2           grupoglosaInfra2 
              7.685860e+01               7.685860e+01               8.942183e+01 
      grupoglosaInfra2_det          grupocodigoInfra2      grupocodigoInfra2_det 
              9.069155e+01               8.942183e+01               9.068744e+01 
grupoCodigoNormaInfra2_det         actividadeconomica                     bloque 
              8.942594e+01               3.188001e+01               9.619144e+01 
                     calle                     comuna                      depto 
              3.188701e+01               3.188701e+01               8.324069e+01 
                     dvSII                    f22c645                    f22c646 
              3.188001e+01               4.705267e+01               9.197261e+01 
               fechainicio               fechatermino              ntrabajadores 
              3.203561e+01               9.730008e+01               3.580787e+01 
                    numero                razonsocial                     region 
              3.243486e+01               3.188001e+01               3.188701e+01 
                     rubro                   subrubro       subtipocontribuyente 
              3.188001e+01               3.188001e+01               3.188001e+01 
         tipocontribuyente             tipoterminoSII                tramoventas 
              3.188001e+01               9.730008e+01               3.188001e+01 
            villapoblacion                       ccae                       gcae 
              8.484225e+01               9.758603e-01               9.758603e-01 
                      crae                       grae                    noInfra 
              9.758603e-01               9.758603e-01               0.000000e+00 
                     Infra                derechoFund                  novaInfra 
              0.000000e+00               0.000000e+00               0.000000e+00 
              num_materias                   num_sind 
              0.000000e+00               6.632007e+01 
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

d$CodTipoEmpresa <- NULL
d$TotalAfectados <- NULL
d$urgencia <- NULL
d$SolEsAfectado <- NULL
d$EmpDMCodComuna <- NULL
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

data2<- d

apply(d, 2, pMiss)
'
           IDFiscalizacion                 CodOficina                       Agno 
              0.000000e+00               0.000000e+00               0.000000e+00 
               NroComision                 CodTipoSol                solicitante 
              0.000000e+00               0.000000e+00               0.000000e+00 
           CodUnidadOrigen               unidadOrigen             CodTipoTermino 
              0.000000e+00               0.000000e+00               0.000000e+00 
               tipoTermino             EgresoConMulta                 RutEmpresa 
              0.000000e+00               0.000000e+00               4.845441e-01 
            EmpDFCodComuna             EmpTrabHombres                     CodCae 
              7.609047e-05               2.679830e+00               1.341475e-01 
      grupocodtipoMaterias     grupoglosatipoMaterias            grupoglosaInfra 
              2.394164e+01               2.394164e+01               7.718062e+01 
          grupocodigoInfra        grupoglosaInfra_det       grupocodigoInfra_det 
              7.718062e+01               7.718062e+01               7.718062e+01 
     grupocodtipoMaterias2    grupoglosatipoMaterias2           grupoglosaInfra2 
              7.685860e+01               7.685860e+01               8.942183e+01 
      grupoglosaInfra2_det          grupocodigoInfra2      grupocodigoInfra2_det 
              9.069155e+01               8.942183e+01               9.068744e+01 
grupoCodigoNormaInfra2_det           ntrabajadoresSII             tramoventasSII 
              8.942594e+01               3.580787e+01               3.188001e+01 
                      ccae                       gcae                       crae 
              9.758603e-01               9.758603e-01               9.758603e-01 
                      grae                    noInfra                      Infra 
              9.758603e-01               0.000000e+00               0.000000e+00 
               derechoFund                  novaInfra               num_materias 
              0.000000e+00               0.000000e+00               0.000000e+00 
                  num_sind                   fechareg                     Region 
              6.632007e+01               1.369628e-03               0.000000e+00 
                 Infractor                     Exsind 
              0.000000e+00               6.632007e+01
'
# se elimina Exsind pues no aporta en mejorar los modelos
d$Exsind <- NULL
d$num_sind <- NULL
d$datereg <- as.Date(d$fechareg)
#d$dateinf <- as.Date(d$fechainf)
d$mesreg <- as.factor(month(d$datereg))
#d$mesinf <- as.factor(month(d$dateinf))
nrow(d)
# [1] 1314225
d <- d[which(d$datereg >= as.Date("2006-06-01","%Y-%m-%d")),]
nrow(d)
# [1] 1297376
d <- d[which(d$datereg <= as.Date("2016-06-30","%Y-%m-%d")),]
nrow(d)
# [1] 1247041

data2a<-d

d$RutEmpresa <- as.numeric(as.character(d$RutEmpresa))
#
d <- d[with(d, order(RutEmpresa)), ]
#
sqldf("select RutEmpresa,count(*) from d where RutEmpresa in (0,1,2,10,11,12,76,88,99,109,123,126,235,1002,10009,100000,1000000,1111111,99999999,9999999,999999) group by RutEmpresa order by RutEmpresa")
'
   RutEmpresa count(*)
1           0      137
2           1     1020
3           2        1
4          10        1
5          11        1
6          12        1
7          76        1
8          88        1
9          99        1
10        109        3
11        123       86
12        126        2
13        235        1
14       1002        3
15      10009        8
16     100000       11
17     999999        1
18    1000000       29
19    1111111       85
20    9999999       23
21   99999999       72
'
sqldf("select RutEmpresa,count(*) from d where RutEmpresa is null group by RutEmpresa order by RutEmpresa")
'
  RutEmpresa count(*)
1         NA     6132
'
nrow(d)
# [1] 1247041
d <- subset(d, !is.na(d$RutEmpresa))
nrow(d)
# [1] 1240909
#
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
# 1239461

#
d$CodTipoSol <- as.factor(d$CodTipoSol)
d$CodTipoTermino <- as.factor(d$CodTipoTermino)
sqldf("select CodTipoTermino,count(*) from d group by CodTipoTermino order by CodTipoTermino")
'
  CodTipoTermino count(*)
1              0        2
2             10   275933
3             11      587
4             15    93735
5             16     1696
6             17   110113
7             18      481
8              8      448
9              9   756466
'
#
sqldf("select CodTipoSol,count(*) from d group by CodTipoSol order by CodTipoSol")
'
   CodTipoSol count(*)
1           0   324354
2           1   399288
3          10    20746
4          11    15817
5          12    18828
6          13      974
7           2    63301
8           3   137937
9           4    95843
10          5    95222
11          6     6034
12          7     5589
13          8    20430
14          9    35098
'
#
sqldf("select solicitante,count(*) from d group by solicitante order by solicitante")
'
                   solicitante count(*)
1  Autoridad                       5589
2  Direccion Nacional             15817
3  Direccion Regional             20746
4  Empleador                     137937
5  Fiscalizador                   18828
6  Inspeccion                     35098
7  Institucion Previsional        95222
8             No se Identifica    95843
9  Organizacion Sindical          63301
10 Otra Institucion                6034
11                Por Programa   324354
12 Tercero                        20430
13                Trabajadores   399288
14 Tribunal                         974
'
#
sqldf("select solicitante,CodTipoSol,count(*) from d group by solicitante,CodTipoSol order by solicitante,CodTipoSol")
'
                   solicitante CodTipoSol count(*)
1  Autoridad                            7     5589
2  Direccion Nacional                  11    15817
3  Direccion Regional                  10    20746
4  Empleador                            3   137937
5  Fiscalizador                        12    18828
6  Inspeccion                           9    35098
7  Institucion Previsional              5    95222
8             No se Identifica          4    95843
9  Organizacion Sindical                2    63301
10 Otra Institucion                     6     6034
11                Por Programa          0   324354
12 Tercero                              8    20430
13                Trabajadores          1   399288
14 Tribunal                            13      974
'
#
sqldf("select CodTipoTermino,tipoTermino,count(*) from d group by CodTipoTermino,tipoTermino order by CodTipoTermino,tipoTermino")
'
  CodTipoTermino                      tipoTermino count(*)
1              0                  Sin informacion        2
2             10                        Con Multa   275933
3             11           Sin Revisión con Multa      587
4             15              Beneficio Sin Multa    93735
5             16              Beneficio Con Multa     1696
6             17            Certificado Sin Multa   110113
7             18            Certificado Con Multa      481
8              8 Revisión Completa Con Infracción      448
9              9                        Sin Multa   756466
'
#
sqldf("select CodTipoTermino,count(*) from d group by CodTipoTermino order by CodTipoTermino")
'
  CodTipoTermino count(*)
1              0        2
2             10   275933
3             11      587
4             15    93735
5             16     1696
6             17   110113
7             18      481
8              8      448
9              9   756466
'
#
#d$CodTipoSol <- as.factor(d$CodTipoSol)
sqldf("select CodTipoSol,count(*) from d group by CodTipoSol order by CodTipoSol")
'
   CodTipoSol count(*)
1           0   324354
2           1   399288
3          10    20746
4          11    15817
5          12    18828
6          13      974
7           2    63301
8           3   137937
9           4    95843
10          5    95222
11          6     6034
12          7     5589
13          8    20430
14          9    35098
'
#
sqldf("select crae,grae,count(*) from d group by crae,grae order by crae,grae")
'
   crae                                                                             grae count(*)
1  <NA>                                                                             <NA>    11620
2     1 AGRICULTURA, CAZA, SILVICULTURA Y PESCA                                                 2
3   100 ACTIVIDADES NO ESPECIFICADAS Y OTRAS                                                69092
4   101                                      Agricultura, ganadería, caza y silvicultura    87326
5   102                                                                            Pesca    10688
6   103                                                  Explotación de minas y canteras     9714
7   104                                                        Industrias manufactureras    85747
8   105                                           Suministro de electricidad, gas y agua     5173
9   106                                                                     Construcción   145402
10  107                                                                         Comercio   210672
11  108                                                           Hoteles y restaurantes    91717
12  109                                      Transporte, almacenamiento y comunicaciones   139324
13  110                                                        Intermediación financiera    16116
14  111                               Actividades inmobiliarias, empresariales y alquile   159083
15  112                                                 Administración publica y defensa     3938
16  113                                                                        Enseñanza    52341
17  114                                                    Servicios sociales y de salud    18535
18  115                                      Otras actividades de servicios comunitarios    90934
19  116                                          Hogares privados con servicio doméstico    31888
20  117                                      Organizaciones y órganos extraterritoriales      149
'
#
#d <- d[ which(d$solicitante != "Institucion Previsional    " ),  ]
#nrow(d)
# 407632
#d <- d[ which(d$solicitante != "Empleador                  " ),  ]
#
nrow(d)
# 1239461
#
d <- d[ which(d$CodTipoSol != 13 ),  ]
nrow(d)
# 1238487
d <- d[ which(d$CodTipoSol != 5 ),  ]
nrow(d)
# 1143265
d <- d[ which(d$CodTipoSol != 3 ),  ]
nrow(d)
# 1005328
#
sqldf("select solicitante,CodTipoSol,count(*) from d group by solicitante,CodTipoSol order by solicitante,CodTipoSol")
'
                   solicitante CodTipoSol count(*)
1  Autoridad                            7     5589
2  Direccion Nacional                  11    15817
3  Direccion Regional                  10    20746
4  Fiscalizador                        12    18828
5  Inspeccion                           9    35098
6             No se Identifica          4    95843
7  Organizacion Sindical                2    63301
8  Otra Institucion                     6     6034
9                 Por Programa          0   324354
10 Tercero                              8    20430
11                Trabajadores          1   399288
'
#
nrow(d)
# [1] 1005328
#
d <- d[ which(d$CodTipoTermino != 0 ),  ]
nrow(d)
# 1005326
d <- d[ which(d$CodTipoTermino != 11 ),  ]
nrow(d)
# 1004739
d <- d[ which(d$CodTipoTermino != 22 ),  ]
nrow(d)
# 1004739
d <- d[ which(d$CodTipoTermino != 15 ),  ]
nrow(d)
# 1004537
d <- d[ which(d$CodTipoTermino != 16 ),  ]
nrow(d)
# 1004530
#
sqldf("select CodTipoTermino,tipoTermino,count(*) from d group by CodTipoTermino,tipoTermino order by CodTipoTermino,tipoTermino")
'
 CodTipoTermino                      tipoTermino count(*)
1             10                        Con Multa   264467
2              8 Revisión Completa Con Infracción      448
3              9                        Sin Multa   739615
'
#
sqldf("select crae,grae,count(*) from d group by crae,grae order by crae,grae")
'
   crae                                                                             grae count(*)
1  <NA>                                                                             <NA>     1533
2   100 ACTIVIDADES NO ESPECIFICADAS Y OTRAS                                                56974
3   101                                      Agricultura, ganadería, caza y silvicultura    59213
4   102                                                                            Pesca     6507
5   103                                                  Explotación de minas y canteras     5612
6   104                                                        Industrias manufactureras    49916
7   105                                           Suministro de electricidad, gas y agua     3648
8   106                                                                     Construcción   101976
9   107                                                                         Comercio   118681
10  108                                                           Hoteles y restaurantes    48920
11  109                                      Transporte, almacenamiento y comunicaciones    86727
12  110                                                        Intermediación financiera     9694
13  111                               Actividades inmobiliarias, empresariales y alquile    83506
14  112                                                 Administración publica y defensa     2401
15  113                                                                        Enseñanza    34135
16  114                                                    Servicios sociales y de salud    10481
17  115                                      Otras actividades de servicios comunitarios    59316
18  116                                          Hogares privados con servicio doméstico    17633
19  117                                      Organizaciones y órganos extraterritoriales      104
'
#
d <- subset(d, !is.na(d$crae))
nrow(d)
# 994394
#
d <- d[ which(d$crae != 100 ),  ]
nrow(d)
# 962910
#
#
sqldf("select solicitante,CodTipoSol,count(*) from d group by solicitante,CodTipoSol order by solicitante,CodTipoSol")
'
                   solicitante CodTipoSol count(*)
1  Autoridad                            7     5435
2  Direccion Nacional                  11     6276
3  Direccion Regional                  10    20262
4  Fiscalizador                        12    17693
5  Inspeccion                           9    34198
6             No se Identifica          4    92517
7  Organizacion Sindical                2    60562
8  Otra Institucion                     6     5612
9                 Por Programa          0   318465
10 Tercero                              8    19799
11                Trabajadores          1   382091
'
#
sqldf("select CodTipoTermino,tipoTermino,count(*) from d group by CodTipoTermino,tipoTermino order by CodTipoTermino,tipoTermino")
'
  CodTipoTermino                      tipoTermino count(*)
1             10                        Con Multa   251849
2              8 Revisión Completa Con Infracción      442
3              9                        Sin Multa   710619
'
#
sqldf("select crae,grae,count(*) from d group by crae,grae order by crae,grae")
'
   crae                                                                             grae count(*)
1     1 AGRICULTURA, CAZA, SILVICULTURA Y PESCA                                                 2
2   101                                      Agricultura, ganadería, caza y silvicultura    67002
3   102                                                                            Pesca     9359
4   103                                                  Explotación de minas y canteras     8067
5   104                                                        Industrias manufactureras    73042
6   105                                           Suministro de electricidad, gas y agua     3205
7   106                                                                     Construcción    90315
8   107                                                                         Comercio   193775
9   108                                                           Hoteles y restaurantes    86255
10  109                                      Transporte, almacenamiento y comunicaciones   122792
11  110                                                        Intermediación financiera    15375
12  111                               Actividades inmobiliarias, empresariales y alquile   139191
13  112                                                 Administración publica y defensa     2880
14  113                                                                        Enseñanza    41364
15  114                                                    Servicios sociales y de salud    16865
16  115                                      Otras actividades de servicios comunitarios    68805
17  116                                          Hogares privados con servicio doméstico    24498
18  117                                      Organizaciones y órganos extraterritoriales      118
'
#
#
d <- d[ which(d$crae != 1 ),  ]
nrow(d)
# 962908
#
sqldf("select crae,grae,count(*) from d group by crae,grae order by crae,grae")
'
   crae                                               grae count(*)
1   101        Agricultura, ganadería, caza y silvicultura    67002
2   102                                              Pesca     9359
3   103                    Explotación de minas y canteras     8067
4   104                          Industrias manufactureras    73042
5   105             Suministro de electricidad, gas y agua     3205
6   106                                       Construcción    90315
7   107                                           Comercio   193775
8   108                             Hoteles y restaurantes    86255
9   109        Transporte, almacenamiento y comunicaciones   122792
10  110                          Intermediación financiera    15375
11  111 Actividades inmobiliarias, empresariales y alquile   139191
12  112                   Administración publica y defensa     2880
13  113                                          Enseñanza    41364
14  114                      Servicios sociales y de salud    16865
15  115        Otras actividades de servicios comunitarios    68805
16  116            Hogares privados con servicio doméstico    24498
17  117        Organizaciones y órganos extraterritoriales      118
'
#

data2b<-d

# ajustes finales
d$grupocodigoInfra <- ifelse(is.na(d$grupocodigoInfra), "99", as.character(d$grupocodigoInfra))
d$grupoglosaInfra <- ifelse(is.na(d$grupoglosaInfra), "99", as.character(d$grupoglosaInfra))
d <- d[order(d$RutEmpresa, d$datereg),]

# Tabla cruce Region Rae , infraccionalidad
a1 <- with(d , tapply(Infractor, list(Region = Region , Actividad = grae), mean))
a2<- as.data.frame(a1)
d$count <- 1
p1 <- with(d , tapply(count, list(Region = Region , Actividad = grae), sum))
p2<- as.data.frame(p1)
#

data3<-d

nrow(fiscalizaciones20170927)
# [1] 1408087
nrow(data1)
# [1] 1314225
nrow(data2)
# [1] 1314225
nrow(data2a)
# [1] 1247041
nrow(data2b)
# [1] 962908
nrow(data3)
# [1] 962908
ncol(data3)
# [1] 45
colnames(data3)
'
 [1] "IDFiscalizacion"            "CodOficina"                
 [3] "Agno"                       "NroComision"               
 [5] "CodTipoSol"                 "solicitante"               
 [7] "CodUnidadOrigen"            "unidadOrigen"              
 [9] "CodTipoTermino"             "tipoTermino"               
[11] "EgresoConMulta"             "RutEmpresa"                
[13] "EmpDFCodComuna"             "EmpTrabHombres"            
[15] "CodCae"                     "grupocodtipoMaterias"      
[17] "grupoglosatipoMaterias"     "grupoglosaInfra"           
[19] "grupocodigoInfra"           "grupoglosaInfra_det"       
[21] "grupocodigoInfra_det"       "grupocodtipoMaterias2"     
[23] "grupoglosatipoMaterias2"    "grupoglosaInfra2"          
[25] "grupoglosaInfra2_det"       "grupocodigoInfra2"         
[27] "grupocodigoInfra2_det"      "grupoCodigoNormaInfra2_det"
[29] "ntrabajadoresSII"           "tramoventasSII"            
[31] "ccae"                       "gcae"                      
[33] "crae"                       "grae"                      
[35] "noInfra"                    "Infra"                     
[37] "derechoFund"                "novaInfra"                 
[39] "num_materias"               "fechareg"                  
[41] "Region"                     "Infractor"                 
[43] "datereg"                    "mesreg"                    
[45] "count"  
'

# re pro reac 20170927

nrow(data3)
# [1] 962908

data3a<-data3
data3a$solicitante<-trimws(data3a$solicitante)

baseproactiva <- subset(x = data3a, subset = solicitante == "Por Programa")
nrow(baseproactiva)
# [1] 318465
basereactiva <- subset(x = data3a, subset = solicitante != "Por Programa")
nrow(basereactiva)
# [1] 644443


# new vars 20170927

data4 <- basereactiva
nrow(data4)
# [1] 644443

# fecha formato Date: datereg
# mes de la fecha: mesreg


data4$RutEmpresa<-as.numeric(as.character(data4$RutEmpresa))
# ordenar
data4 <- data4[with(data4, order(RutEmpresa, datereg)), ]
#data4 <- data4[order(data4$RutEmpresa, data4$datereg),]
nrow(data4)
# [1] 644443
ncol(data4)
# [1] 45
data4$numfisc <- 0
ncol(data4)
# [1] 46

# cuenta la cantidad de filas por RutEmpresa

femp <- aggregate(formula = cbind(n = 1:nrow(data4)) ~ RutEmpresa, data = data4, FUN = length)
#femp <- count(data4 , c('RutEmpresa'))
nrow(femp)
# [1] 158143
ncol(femp)
# [1] 2
head(femp)
'
  RutEmpresa  n
1      37229 17
2      65753  1
3      73051  1
4      96731  1
5     100000  9
6     111111  1
'
#base nueva con comportamiento
basetest1 <- merge(data4, femp, by="RutEmpresa")
nrow(basetest1)
# [1] 644443
ncol(basetest1)
# [1] 47
head(basetest1)
'
  RutEmpresa IDFiscalizacion CodOficina Agno NroComision CodTipoSol                 solicitante
1      37229         1679519       1316 2009         723          4            No se Identifica
2      37229         1776537       1316 2010         222          1                Trabajadores
3      37229         1786322       1316 2010         454          1                Trabajadores
4      37229         1794703       1316 2010         578          2 Organizacion Sindical      
5      37229         1814521       1316 2010         855          1                Trabajadores
6      37229         1819506       1316 2010         921          2 Organizacion Sindical      
  CodUnidadOrigen        unidadOrigen CodTipoTermino tipoTermino EgresoConMulta EmpDFCodComuna
1               1 Atención de Público             10   Con Multa              1          13110
2               1 Atención de Público              9   Sin Multa              0          13110
3               1 Atención de Público             10   Con Multa              1          13110
4               1 Atención de Público              9   Sin Multa              0          13110
5               1 Atención de Público             10   Con Multa              1          13110
6               1 Atención de Público             10   Con Multa              1          13110
  EmpTrabHombres CodCae
1             34   1132
2             30   1132
3             12   1131
4             29   1132
5             29   1132
6             28   1132
                                                          grupocodtipoMaterias
1 15        ,3         ,3         ,7         ,7         ,9         ,9         
2                                             15        ,3         ,9         
3                       10        ,15        ,15        ,3         ,9         
4                                                                   3         
5                                  10        ,15        ,3         ,9         
6                       10        ,15        ,15        ,6         ,9         
                                                                                                                                                                        grupoglosatipoMaterias
1 REMUNERACIONES,CONTRATO DE TRABAJO,CONTRATO DE TRABAJO,ACOSO SEXUAL, SIMULACIÓN, SUBTERFUGIO Y OTROS.,ACOSO SEXUAL, SIMULACIÓN, SUBTERFUGIO Y OTROS.,HIGIENE Y SEGURIDAD,HIGIENE Y SEGURIDAD
2                                                                                                                                       REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
3                                                                                                    JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
4                                                                                                                                                                          CONTRATO DE TRABAJO
5                                                                                                                   JORNADA Y DESCANSOS,REMUNERACIONES,CONTRATO DE TRABAJO,HIGIENE Y SEGURIDAD
6                                                                                             JORNADA Y DESCANSOS,REMUNERACIONES,REMUNERACIONES,COTIZACIONES PREVISIONALES,HIGIENE Y SEGURIDAD
                          grupoglosaInfra      grupocodigoInfra
1 HIGIENE Y SEGURIDAD,HIGIENE Y SEGURIDAD 9         ,9         
2                                      99                    99
3                     CONTRATO DE TRABAJO            3         
4                     CONTRATO DE TRABAJO            3         
5                          REMUNERACIONES            15        
6                          REMUNERACIONES            15        
                                                                               grupoglosaInfra_det
1 || COMITÉ PARITARIO/NO EFECTUAR REUNIÓN OBLIGATORIA || NO CUMPLIR CONDICIONES SANITARIAS BÁSICAS
2                                                                                             <NA>
3                                                           || MODIFICACIÓN UNILATERAL CLÁUSULA(S)
4                          || Articulo 12 C. del T/Cambio unilateral lugar/naturaleza de servicios
5                                                   || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE
6                                                   || NO PAGAR/EN FORMA ÍNTEGRA Y/O CORRECTAMENTE
  grupocodigoInfra_det grupocodtipoMaterias2 grupoglosatipoMaterias2 grupoglosaInfra2
1        || 403 || 407                  <NA>                    <NA>             <NA>
2                 <NA>                  <NA>                    <NA>             <NA>
3               || 515                  <NA>                    <NA>             <NA>
4               || 338                  <NA>                    <NA>             <NA>
5               || 462                  <NA>                    <NA>             <NA>
6               || 462                  <NA>                    <NA>             <NA>
  grupoglosaInfra2_det grupocodigoInfra2 grupocodigoInfra2_det grupoCodigoNormaInfra2_det
1                 <NA>              <NA>                  <NA>                       <NA>
2                 <NA>              <NA>                  <NA>                       <NA>
3                 <NA>              <NA>                  <NA>                       <NA>
4                 <NA>              <NA>                  <NA>                       <NA>
5                 <NA>              <NA>                  <NA>                       <NA>
6                 <NA>              <NA>                  <NA>                       <NA>
  ntrabajadoresSII tramoventasSII   ccae
1               NA           <NA> 802100
2               NA           <NA> 802100
3               NA           <NA> 801020
4               NA           <NA> 802100
5               NA           <NA> 802100
6               NA           <NA> 802100
                                                                                                                                                                                                      gcae
1 Enseñanza secundaria de  formación general                                                                                                                                                              
2 Enseñanza secundaria de  formación general                                                                                                                                                              
3 Enseñanza primaria                                                                                                                                                                                      
4 Enseñanza secundaria de  formación general                                                                                                                                                              
5 Enseñanza secundaria de  formación general                                                                                                                                                              
6 Enseñanza secundaria de  formación general                                                                                                                                                              
  crae      grae noInfra Infra derechoFund novaInfra num_materias   fechareg Region Infractor
1  113 Enseñanza       5     2           0         0            7 2009-05-26     13         1
2  113 Enseñanza       3     0           0         0            3 2010-02-24     13         0
3  113 Enseñanza       4     1           0         0            5 2010-03-29     13         1
4  113 Enseñanza       0     1           0         0            1 2010-04-21     13         1
5  113 Enseñanza       3     1           0         0            4 2010-06-14     13         1
6  113 Enseñanza       4     1           0         0            5 2010-06-30     13         1
     datereg mesreg count numfisc  n
1 2009-05-26      5     1       0 17
2 2010-02-24      2     1       0 17
3 2010-03-29      3     1       0 17
4 2010-04-21      4     1       0 17
5 2010-06-14      6     1       0 17
6 2010-06-30      6     1       0 17
'
#
basetest1$numfisc <- ave( 1:nrow(basetest1), basetest1$RutEmpresa, FUN=function(x) 1:length(x)-1 )
basetest1$numinf <-  ave( basetest1$Infractor, basetest1$RutEmpresa, FUN=function(x)  lagpad(cumsum(x),1))
basetest1$recency <- ave( as.numeric(basetest1$datereg) , basetest1$RutEmpresa, FUN=function(x) c(0, diff(x) ))
#
d1 <- basetest1
#
summary(d1$numfisc)
"
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
   0.00    1.00    4.00   81.22   32.00 3595.00 
"
summary(d1$numinf)
"
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
   0.00    0.00    2.00   34.52   14.00 1534.00 
"
summary(d1$recency)
"
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
    0.0     0.0    11.0   131.8    90.0  3618.0 
"
nrow(d1)
# [1] 644443
ncol(d1)
# [1] 49
colnames(d1)
'
 [1] "RutEmpresa"                 "IDFiscalizacion"            "CodOficina"                
 [4] "Agno"                       "NroComision"                "CodTipoSol"                
 [7] "solicitante"                "CodUnidadOrigen"            "unidadOrigen"              
[10] "CodTipoTermino"             "tipoTermino"                "EgresoConMulta"            
[13] "EmpDFCodComuna"             "EmpTrabHombres"             "CodCae"                    
[16] "grupocodtipoMaterias"       "grupoglosatipoMaterias"     "grupoglosaInfra"           
[19] "grupocodigoInfra"           "grupoglosaInfra_det"        "grupocodigoInfra_det"      
[22] "grupocodtipoMaterias2"      "grupoglosatipoMaterias2"    "grupoglosaInfra2"          
[25] "grupoglosaInfra2_det"       "grupocodigoInfra2"          "grupocodigoInfra2_det"     
[28] "grupoCodigoNormaInfra2_det" "ntrabajadoresSII"           "tramoventasSII"            
[31] "ccae"                       "gcae"                       "crae"                      
[34] "grae"                       "noInfra"                    "Infra"                     
[37] "derechoFund"                "novaInfra"                  "num_materias"              
[40] "fechareg"                   "Region"                     "Infractor"                 
[43] "datereg"                    "mesreg"                     "count"                     
[46] "numfisc"                    "n"                          "numinf"                    
[49] "recency"
'

basereactivaplus <- d1

# base x rut 20170927

basefr <- basereactivaplus
nrow(basefr)
# [1] 644443
ncol(basefr)
# [1] 49
colnames(basefr)
'
 [1] "RutEmpresa"                 "IDFiscalizacion"           
 [3] "CodOficina"                 "Agno"                      
 [5] "NroComision"                "CodTipoSol"                
 [7] "solicitante"                "CodUnidadOrigen"           
 [9] "unidadOrigen"               "CodTipoTermino"            
[11] "tipoTermino"                "EgresoConMulta"            
[13] "EmpDFCodComuna"             "EmpTrabHombres"            
[15] "CodCae"                     "grupocodtipoMaterias"      
[17] "grupoglosatipoMaterias"     "grupoglosaInfra"           
[19] "grupocodigoInfra"           "grupoglosaInfra_det"       
[21] "grupocodigoInfra_det"       "grupocodtipoMaterias2"     
[23] "grupoglosatipoMaterias2"    "grupoglosaInfra2"          
[25] "grupoglosaInfra2_det"       "grupocodigoInfra2"         
[27] "grupocodigoInfra2_det"      "grupoCodigoNormaInfra2_det"
[29] "ntrabajadoresSII"           "tramoventasSII"            
[31] "ccae"                       "gcae"                      
[33] "crae"                       "grae"                      
[35] "noInfra"                    "Infra"                     
[37] "derechoFund"                "novaInfra"                 
[39] "num_materias"               "fechareg"                  
[41] "Region"                     "Infractor"                 
[43] "datereg"                    "mesreg"                    
[45] "count"                      "numfisc"                   
[47] "n"                          "numinf"                    
[49] "recency" 
'

#crear base agregada por rut
bd <- basefr[c("Infractor","RutEmpresa", "Agno","datereg", "Region" ,"crae", "grae", "grupocodtipoMaterias" , "grupocodigoInfra", "numfisc", "numinf", "recency" )]
#bd <- bd[order(bd$RutEmpresa, bd$datereg),]
bd.1 <- concat.split.expanded(bd , "Agno", fill = 0, drop = TRUE )
bd.1$inf_06 <- bd.1$Agno_01
bd.1$inf_07 <- bd.1$Agno_02
bd.1$inf_08 <- bd.1$Agno_03
bd.1$inf_09 <- bd.1$Agno_04
bd.1$inf_10 <- bd.1$Agno_05
bd.1$inf_11 <- bd.1$Agno_06
#base hasta 2016
bd.1$inf_12 <- bd.1$Agno_07
bd.1$inf_13 <- bd.1$Agno_08
bd.1$inf_14 <- bd.1$Agno_09
bd.1$inf_15 <- bd.1$Agno_10
bd.1$inf_16 <- bd.1$Agno_11
#

bd.1$inf_06 <- ifelse(bd.1$Infractor == bd.1$inf_06,bd.1$Infractor  , 0 )
bd.1$inf_07 <- ifelse(bd.1$Infractor == bd.1$inf_07,bd.1$Infractor  , 0 )
bd.1$inf_08 <- ifelse(bd.1$Infractor == bd.1$inf_08,bd.1$Infractor  , 0 )
bd.1$inf_09 <- ifelse(bd.1$Infractor == bd.1$inf_09,bd.1$Infractor  , 0 )
bd.1$inf_10 <- ifelse(bd.1$Infractor == bd.1$inf_10,bd.1$Infractor  , 0 )
bd.1$inf_11 <- ifelse(bd.1$Infractor == bd.1$inf_11,bd.1$Infractor  , 0 )
#base hasta 2016
bd.1$inf_12 <- ifelse(bd.1$Infractor == bd.1$inf_12,bd.1$Infractor  , 0 )
bd.1$inf_13 <- ifelse(bd.1$Infractor == bd.1$inf_13,bd.1$Infractor  , 0 )
bd.1$inf_14 <- ifelse(bd.1$Infractor == bd.1$inf_14,bd.1$Infractor  , 0 )
bd.1$inf_15 <- ifelse(bd.1$Infractor == bd.1$inf_15,bd.1$Infractor  , 0 )
bd.1$inf_16 <- ifelse(bd.1$Infractor == bd.1$inf_16,bd.1$Infractor  , 0 )

nrow(bd.1)
# [1] 644443
ncol(bd.1)
#
colnames(bd.1)
'

'

bd.2 <- aggregate(cbind(Agno_01,inf_06,Agno_02,inf_07,Agno_03,inf_08, Agno_04,inf_09, Agno_05,inf_10, Agno_06,inf_11, Agno_07,inf_12,Agno_08,inf_13,Agno_09,inf_14, Agno_10,inf_15,Agno_11,inf_16) ~ RutEmpresa + crae + Region , data=bd.1 , sum)

nrow(bd.2)
# [1] 219726

# construccion variables comportamiento
bd.2$fiscmean <- rowMeans(bd.2[c(4,6,8,10,12,14,16,18,20,22,24)])
bd.2$infmean <- rowMeans(bd.2[c(5,7,9,11,13,15,17,19,21,23,25)])
bd.2$qfisc <- rowSums(bd.2[c(4,6,8,10,12,14,16,18,20,22,24)])
bd.2$qinf <- rowSums(bd.2[c(5,7,9,11,13,15,17,19,21,23,25)])
bd.2$fiscsd <- apply(bd.2[c(4,6,8,10,12,14,16,18,20,22,24)], 1,sd)
bd.2$infsd <- apply(bd.2[c(5,7,9,11,13,15,17,19,21,23,25)],1,sd)

bd.2$recency <- ifelse(bd.2$Agno_11 >=1, 0,ifelse(bd.2$Agno_10 >=1, 1 ,
	ifelse(bd.2$Agno_09 >=1, 2, 
		ifelse(bd.2$Agno_08>=1, 3,
			ifelse(bd.2$Agno_07>=1, 4,
				ifelse(bd.2$Agno_06>=1, 5,
					ifelse(bd.2$Agno_05>=1, 6,
						ifelse(bd.2$Agno_04>=1, 7,
							ifelse(bd.2$Agno_03>=1, 8,
								ifelse(bd.2$Agno_02>=1, 9,
									ifelse(bd.2$Agno_01>=1, 10, 11))))))))) ) )


bd.2 <- subset(bd.2, bd.2$qfisc != 0)
bd.2$infrac <- bd.2$qinf/bd.2$qfisc

#bd.2$Infractor <- ifelse(  bd.2$inf_16 >= 1, 1, 0)
#bd.2$Infractor <- ifelse(  bd.2$inf_16 >= 1, 1, ifelse(  bd.2$inf_15 >= 1, 1, 0))
bd.2$Infractor <- ifelse(  bd.2$inf_15 >= 1, 1, 0)
#bd.2$Infractor<-as.factor(bd.2$Infractor)

bd2<-bd.2
sqldf("select Infractor, count(*) from bd2 group by Infractor order by Infractor")
'
  Infractor count(*)
1         0   199915
2         1    19811
'

nrow(bd2)
# [1] 219726

ncol(bd2)
# [1] 34

colnames(bd2)
'
 [1] "RutEmpresa" "crae"       "Region"     "Agno_01"    "inf_06"    
 [6] "Agno_02"    "inf_07"     "Agno_03"    "inf_08"     "Agno_04"   
[11] "inf_09"     "Agno_05"    "inf_10"     "Agno_06"    "inf_11"    
[16] "Agno_07"    "inf_12"     "Agno_08"    "inf_13"     "Agno_09"   
[21] "inf_14"     "Agno_10"    "inf_15"     "Agno_11"    "inf_16"    
[26] "fiscmean"   "infmean"    "qfisc"      "qinf"       "fiscsd"    
[31] "infsd"      "recency"    "infrac"     "Infractor" 
'

# sel atrib bd x rut
#
# Atributos que nos permiten predecir
#
nd.xrut <- bd2
#
colnames(nd.xrut)
'
 [1] "RutEmpresa" "crae"       "Region"     "Agno_01"    "inf_06"    
 [6] "Agno_02"    "inf_07"     "Agno_03"    "inf_08"     "Agno_04"   
[11] "inf_09"     "Agno_05"    "inf_10"     "Agno_06"    "inf_11"    
[16] "Agno_07"    "inf_12"     "Agno_08"    "inf_13"     "Agno_09"   
[21] "inf_14"     "Agno_10"    "inf_15"     "Agno_11"    "inf_16"    
[26] "fiscmean"   "infmean"    "qfisc"      "qinf"       "fiscsd"    
[31] "infsd"      "recency"    "infrac"     "Infractor" 
'
#
nrow(nd.xrut)
# [1] 219726
#
apply(nd.xrut, 2, pMiss)
'
RutEmpresa       crae     Region    Agno_01     inf_06    Agno_02     inf_07 
         0          0          0          0          0          0          0 
   Agno_03     inf_08    Agno_04     inf_09    Agno_05     inf_10    Agno_06 
         0          0          0          0          0          0          0 
    inf_11    Agno_07     inf_12    Agno_08     inf_13    Agno_09     inf_14 
         0          0          0          0          0          0          0 
   Agno_10     inf_15    Agno_11     inf_16   fiscmean    infmean      qfisc 
         0          0          0          0          0          0          0 
      qinf     fiscsd      infsd    recency     infrac  Infractor 
         0          0          0          0          0          0 
'
summary(nd.xrut)
'
   RutEmpresa            crae           Region         Agno_01       
 Min.   :   37229   107    :38053   13     :76986   Min.   : 0.0000  
 1st Qu.:11111686   111    :30165   5      :24851   1st Qu.: 0.0000  
 Median :76094291   115    :24558   8      :22007   Median : 0.0000  
 Mean   :54162601   106    :24400   10     :13938   Mean   : 0.1556  
 3rd Qu.:77705075   109    :19563   7      :13841   3rd Qu.: 0.0000  
 Max.   :99979260   104    :19458   6      :13333   Max.   :64.0000  
                    (Other):63529   (Other):54770                    
     inf_06            Agno_02             inf_07           Agno_03        
 Min.   : 0.00000   Min.   :  0.0000   Min.   : 0.0000   Min.   :  0.0000  
 1st Qu.: 0.00000   1st Qu.:  0.0000   1st Qu.: 0.0000   1st Qu.:  0.0000  
 Median : 0.00000   Median :  0.0000   Median : 0.0000   Median :  0.0000  
 Mean   : 0.07537   Mean   :  0.2996   Mean   : 0.1429   Mean   :  0.2982  
 3rd Qu.: 0.00000   3rd Qu.:  0.0000   3rd Qu.: 0.0000   3rd Qu.:  0.0000  
 Max.   :46.00000   Max.   :105.0000   Max.   :59.0000   Max.   :145.0000  
                                                                           
     inf_08           Agno_04            inf_09           Agno_05        
 Min.   : 0.0000   Min.   :  0.000   Min.   : 0.0000   Min.   :  0.0000  
 1st Qu.: 0.0000   1st Qu.:  0.000   1st Qu.: 0.0000   1st Qu.:  0.0000  
 Median : 0.0000   Median :  0.000   Median : 0.0000   Median :  0.0000  
 Mean   : 0.1431   Mean   :  0.295   Mean   : 0.1411   Mean   :  0.2815  
 3rd Qu.: 0.0000   3rd Qu.:  0.000   3rd Qu.: 0.0000   3rd Qu.:  0.0000  
 Max.   :70.0000   Max.   :136.000   Max.   :63.0000   Max.   :204.0000  
                                                                         
     inf_10           Agno_06             inf_11           Agno_07        
 Min.   : 0.0000   Min.   :  0.0000   Min.   : 0.0000   Min.   :  0.0000  
 1st Qu.: 0.0000   1st Qu.:  0.0000   1st Qu.: 0.0000   1st Qu.:  0.0000  
 Median : 0.0000   Median :  0.0000   Median : 0.0000   Median :  0.0000  
 Mean   : 0.1334   Mean   :  0.3002   Mean   : 0.1382   Mean   :  0.2869  
 3rd Qu.: 0.0000   3rd Qu.:  0.0000   3rd Qu.: 0.0000   3rd Qu.:  0.0000  
 Max.   :84.0000   Max.   :282.0000   Max.   :80.0000   Max.   :222.0000  
                                                                          
     inf_12           Agno_08             inf_13            Agno_09       
 Min.   : 0.0000   Min.   :  0.0000   Min.   :  0.0000   Min.   :  0.000  
 1st Qu.: 0.0000   1st Qu.:  0.0000   1st Qu.:  0.0000   1st Qu.:  0.000  
 Median : 0.0000   Median :  0.0000   Median :  0.0000   Median :  0.000  
 Mean   : 0.1335   Mean   :  0.2695   Mean   :  0.1233   Mean   :  0.275  
 3rd Qu.: 0.0000   3rd Qu.:  0.0000   3rd Qu.:  0.0000   3rd Qu.:  0.000  
 Max.   :93.0000   Max.   :221.0000   Max.   :141.0000   Max.   :254.000  
                                                                          
     inf_14           Agno_10             inf_15            Agno_11        
 Min.   : 0.0000   Min.   :  0.0000   Min.   :  0.0000   Min.   :  0.0000  
 1st Qu.: 0.0000   1st Qu.:  0.0000   1st Qu.:  0.0000   1st Qu.:  0.0000  
 Median : 0.0000   Median :  0.0000   Median :  0.0000   Median :  0.0000  
 Mean   : 0.1236   Mean   :  0.3091   Mean   :  0.1319   Mean   :  0.1624  
 3rd Qu.: 0.0000   3rd Qu.:  0.0000   3rd Qu.:  0.0000   3rd Qu.:  0.0000  
 Max.   :86.0000   Max.   :308.0000   Max.   :111.0000   Max.   :219.0000  
                                                                           
     inf_16            fiscmean            infmean             qfisc         
 Min.   : 0.00000   Min.   :  0.09091   Min.   : 0.00000   Min.   :   1.000  
 1st Qu.: 0.00000   1st Qu.:  0.09091   1st Qu.: 0.00000   1st Qu.:   1.000  
 Median : 0.00000   Median :  0.09091   Median : 0.09091   Median :   1.000  
 Mean   : 0.07192   Mean   :  0.26663   Mean   : 0.12348   Mean   :   2.933  
 3rd Qu.: 0.00000   3rd Qu.:  0.18182   3rd Qu.: 0.09091   3rd Qu.:   2.000  
 Max.   :79.00000   Max.   :149.27273   Max.   :68.63636   Max.   :1642.000  
                                                                             
      qinf             fiscsd            infsd            recency      
 Min.   :  0.000   Min.   : 0.3015   Min.   : 0.0000   Min.   : 0.000  
 1st Qu.:  0.000   1st Qu.: 0.3015   1st Qu.: 0.0000   1st Qu.: 2.000  
 Median :  1.000   Median : 0.3015   Median : 0.3015   Median : 4.000  
 Mean   :  1.358   Mean   : 0.5336   Mean   : 0.2808   Mean   : 4.565  
 3rd Qu.:  1.000   3rd Qu.: 0.6030   3rd Qu.: 0.3015   3rd Qu.: 7.000  
 Max.   :755.000   Max.   :84.7727   Max.   :34.9092   Max.   :10.000  
                                                                       
     infrac         Infractor      
 Min.   :0.0000   Min.   :0.00000  
 1st Qu.:0.0000   1st Qu.:0.00000  
 Median :0.5000   Median :0.00000  
 Mean   :0.4864   Mean   :0.09016  
 3rd Qu.:1.0000   3rd Qu.:0.00000  
 Max.   :1.0000   Max.   :1.00000  
'

# correlaciones entre variables
# realizado con rattle version 5.1.3

"
Correlation summary using the 'Spearman' covariance.

Note that only correlations between numeric variables are reported.

              Agno_10       Agno_09      Agno_11        inf_15       Agno_08
Agno_10   1.000000000  0.1746277943  0.187819774  0.7160462387  0.1134700177
Agno_09   0.174627794  1.0000000000  0.127829715  0.1552200765  0.1840869584
Agno_11   0.187819774  0.1278297153  1.000000000  0.1745121100  0.0985238685
inf_15    0.716046239  0.1552200765  0.174512110  1.0000000000  0.1085287148
Agno_08   0.113470018  0.1840869584  0.098523869  0.1085287148  1.0000000000
inf_16    0.154288087  0.1080743385  0.727744299  0.1564053994  0.0825282410
inf_14    0.156547595  0.7416579790  0.115270470  0.1487665174  0.1617853185
Agno_07   0.072463929  0.1151273178  0.064845829  0.0730833654  0.1783446559
infrac   -0.016892550  0.0031114329 -0.004781823  0.1760697311  0.0061597241
inf_13    0.107574090  0.1623688454  0.092505748  0.1094594608  0.7416827642
inf_12    0.070002289  0.1070339120  0.064249525  0.0792442553  0.1655215127
Agno_06   0.040102753  0.0729595105  0.040397559  0.0467432426  0.1038959492
inf_11    0.050386534  0.0748305104  0.049862903  0.0594689315  0.1035021648
Agno_05   0.016673890  0.0392854019  0.023280514  0.0278022704  0.0680298045
inf_10    0.027730098  0.0454900627  0.030229560  0.0392127538  0.0712838771
Agno_04  -0.008457059  0.0149263327  0.005204377  0.0095728630  0.0352250811
inf_09    0.008324138  0.0269614213  0.017502032  0.0234523267  0.0440331856
Agno_03  -0.023646391  0.0005115566 -0.007560760 -0.0041584347  0.0154718985
inf_08   -0.004811516  0.0134436938  0.003779203  0.0110446509  0.0257018803
infsd     0.179603435  0.2137988323  0.151984801  0.3063031310  0.2256531303
infmean   0.185247020  0.2198394146  0.157022759  0.3120987399  0.2327317059
qinf      0.185247020  0.2198394146  0.157022759  0.3120987399  0.2327317059
inf_07   -0.015794592 -0.0026850721 -0.005955077  0.0010693760  0.0067111494
Agno_02  -0.041367943 -0.0226802815 -0.020961138 -0.0169830121 -0.0096554965
fiscsd    0.254761735  0.2755258277  0.195633822  0.2204945820  0.2853662688
fiscmean  0.269610547  0.2931557509  0.209533246  0.2279547568  0.3057462622
qfisc     0.269610547  0.2931557509  0.209533246  0.2279547568  0.3057462622
recency  -0.559924000 -0.4119898419 -0.518120358 -0.3985350559 -0.3194977726
inf_06   -0.016510007 -0.0058649534 -0.005531609  0.0002152439  0.0005547368
Agno_01  -0.033040546 -0.0200146129 -0.017648705 -0.0120146925 -0.0118358295
                inf_16       inf_14      Agno_07        infrac       inf_13
Agno_10   0.1542880868  0.156547595  0.072463929 -0.0168925500  0.107574090
Agno_09   0.1080743385  0.741657979  0.115127318  0.0031114329  0.162368845
Agno_11   0.7277442985  0.115270470  0.064845829 -0.0047818226  0.092505748
inf_15    0.1564053994  0.148766517  0.073083365  0.1760697311  0.109459461
Agno_08   0.0825282410  0.161785319  0.178344656  0.0061597241  0.741682764
inf_16    1.0000000000  0.106521576  0.057372296  0.1318803204  0.082874709
inf_14    0.1065215755  1.000000000  0.104020763  0.1603849741  0.153667184
Agno_07   0.0573722962  0.104020763  1.000000000  0.0102364441  0.155512331
infrac    0.1318803204  0.160384974  0.010236444  1.0000000000  0.156756800
inf_13    0.0828747090  0.153667184  0.155512331  0.1567567998  1.000000000
inf_12    0.0608645879  0.104718585  0.743871075  0.1703152601  0.157827699
Agno_06   0.0399569312  0.066998800  0.160752106  0.0008328139  0.095605620
inf_11    0.0515656640  0.074584676  0.156334350  0.1736262068  0.101925933
Agno_05   0.0251388280  0.040819758  0.096453397  0.0072601658  0.066986551
inf_10    0.0322789811  0.049100519  0.096392783  0.1758520810  0.072687816
Agno_04   0.0120619734  0.022141935  0.051649688  0.0045921726  0.039923640
inf_09    0.0227825583  0.032634666  0.061850308  0.1785620807  0.049676441
Agno_03  -0.0003389463  0.009746978  0.025658324  0.0032072489  0.024692176
inf_08    0.0089219360  0.023258413  0.036159345  0.1832896712  0.033412123
infsd     0.2325809732  0.307358273  0.230189473  0.7363640363  0.311590260
infmean   0.2376900699  0.313698877  0.237481054  0.7344305808  0.318935480
qinf      0.2376900699  0.313698877  0.237481054  0.7344305808  0.318935480
inf_07    0.0014289946  0.007895383  0.019142413  0.1921033656  0.016799863
Agno_02  -0.0112842862 -0.006797014  0.002170331 -0.0016393952  0.004412530
fiscsd    0.1657360109  0.232621528  0.285933575  0.0276102230  0.239642591
fiscmean  0.1731389298  0.242575655  0.306490617  0.0277734167  0.251928312
qfisc     0.1731389298  0.242575655  0.306490617  0.0277734167  0.251928312
recency  -0.3733113127 -0.307160435 -0.237541748 -0.0005491430 -0.242213265
inf_06    0.0030679126  0.003446805  0.010282063  0.1485772783  0.012569181
Agno_01  -0.0084140104 -0.008144178 -0.004115968 -0.0013151468  0.001800675
               inf_12       Agno_06      inf_11      Agno_05      inf_10      Agno_04
Agno_10   0.070002289  0.0401027533  0.05038653  0.016673890  0.02773010 -0.008457059
Agno_09   0.107033912  0.0729595105  0.07483051  0.039285402  0.04549006  0.014926333
Agno_11   0.064249525  0.0403975590  0.04986290  0.023280514  0.03022956  0.005204377
inf_15    0.079244255  0.0467432426  0.05946893  0.027802270  0.03921275  0.009572863
Agno_08   0.165521513  0.1038959492  0.10350216  0.068029804  0.07128388  0.035225081
inf_16    0.060864588  0.0399569312  0.05156566  0.025138828  0.03227898  0.012061973
inf_14    0.104718585  0.0669987997  0.07458468  0.040819758  0.04910052  0.022141935
Agno_07   0.743871075  0.1607521065  0.15633435  0.096453397  0.09639278  0.051649688
infrac    0.170315260  0.0008328139  0.17362621  0.007260166  0.17585208  0.004592173
inf_13    0.157827699  0.0956056202  0.10192593  0.066986551  0.07268782  0.039923640
inf_12    1.000000000  0.1428833457  0.15192569  0.088866040  0.09496906  0.052092247
Agno_06   0.142883346  1.0000000000  0.73781790  0.154073073  0.14762178  0.084603443
inf_11    0.151925686  0.7378178990  1.00000000  0.138741988  0.14385899  0.082650651
Agno_05   0.088866040  0.1540730733  0.13874199  1.000000000  0.74087252  0.156506753
inf_10    0.094969060  0.1476217785  0.14385899  0.740872520  1.00000000  0.135599513
Agno_04   0.052092247  0.0846034431  0.08265065  0.156506753  0.13559951  1.000000000
inf_09    0.063088100  0.0887454887  0.09144728  0.149318240  0.14056653  0.740533584
Agno_03   0.029498831  0.0505641190  0.05459964  0.084463781  0.07646386  0.158244771
inf_08    0.039478705  0.0584035234  0.06456999  0.086966411  0.08407667  0.154656627
infsd     0.323954881  0.2228445233  0.33104990  0.223991989  0.32705399  0.220502048
infmean   0.331597661  0.2301937897  0.33890916  0.231215048  0.33473311  0.226553764
qinf      0.331597661  0.2301937897  0.33890916  0.231215048  0.33473311  0.226553764
inf_07    0.026990783  0.0293826887  0.04183789  0.054952237  0.05996868  0.085112713
Agno_02   0.011645303  0.0145755963  0.02827231  0.045816927  0.04854650  0.077426320
fiscsd    0.243370113  0.2870129285  0.24961793  0.282364609  0.24390133  0.282145728
fiscmean  0.255401426  0.3074954661  0.26234326  0.303519454  0.25740053  0.300716038
qfisc     0.255401426  0.3074954661  0.26234326  0.303519454  0.25740053  0.300716038
recency  -0.183294276 -0.1420802408 -0.11752836 -0.043380417 -0.04684837  0.060412655
inf_06    0.013015013  0.0177763731  0.02915109  0.036776098  0.04455300  0.054583365
Agno_01   0.002822371  0.0088543619  0.02065456  0.029468813  0.03385495  0.053732225
              inf_09       Agno_03       inf_08      infsd    infmean       qinf
Agno_10  0.008324138 -0.0236463905 -0.004811516  0.1796034  0.1852470  0.1852470
Agno_09  0.026961421  0.0005115566  0.013443694  0.2137988  0.2198394  0.2198394
Agno_11  0.017502032 -0.0075607599  0.003779203  0.1519848  0.1570228  0.1570228
inf_15   0.023452327 -0.0041584347  0.011044651  0.3063031  0.3120987  0.3120987
Agno_08  0.044033186  0.0154718985  0.025701880  0.2256531  0.2327317  0.2327317
inf_16   0.022782558 -0.0003389463  0.008921936  0.2325810  0.2376901  0.2376901
inf_14   0.032634666  0.0097469783  0.023258413  0.3073583  0.3136989  0.3136989
Agno_07  0.061850308  0.0256583238  0.036159345  0.2301895  0.2374811  0.2374811
infrac   0.178562081  0.0032072489  0.183289671  0.7363640  0.7344306  0.7344306
inf_13   0.049676441  0.0246921765  0.033412123  0.3115903  0.3189355  0.3189355
inf_12   0.063088100  0.0294988309  0.039478705  0.3239549  0.3315977  0.3315977
Agno_06  0.088745489  0.0505641190  0.058403523  0.2228445  0.2301938  0.2301938
inf_11   0.091447284  0.0545996356  0.064569991  0.3310499  0.3389092  0.3389092
Agno_05  0.149318240  0.0844637809  0.086966411  0.2239920  0.2312150  0.2312150
inf_10   0.140566531  0.0764638577  0.084076665  0.3270540  0.3347331  0.3347331
Agno_04  0.740533584  0.1582447707  0.154656627  0.2205020  0.2265538  0.2265538
inf_09   1.000000000  0.1433360491  0.151499990  0.3330083  0.3394528  0.3394528
Agno_03  0.143336049  1.0000000000  0.742052847  0.2092719  0.2148109  0.2148109
inf_08   0.151499990  0.7420528470  1.000000000  0.3306809  0.3363992  0.3363992
infsd    0.333008349  0.2092719124  0.330680874  1.0000000  0.9984684  0.9984684
infmean  0.339452755  0.2148108702  0.336399152  0.9984684  1.0000000  1.0000000
qinf     0.339452755  0.2148108702  0.336399152  0.9984684  1.0000000  1.0000000
inf_07   0.090234633  0.1373033092  0.137041335  0.3167597  0.3211092  0.3211092
Agno_02  0.076556190  0.1407300565  0.130359352  0.1795231  0.1837722  0.1837722
fiscsd   0.250688742  0.2697954565  0.243284923  0.6141049  0.6128358  0.6128358
fiscmean 0.261805074  0.2865875657  0.253167406  0.6204742  0.6231033  0.6231033
qfisc    0.261805074  0.2865875657  0.253167406  0.6204742  0.6231033  0.6231033
recency  0.027110462  0.1615914675  0.099510801 -0.1921729 -0.1992241 -0.1992241
inf_06   0.062379980  0.0795385376  0.081394900  0.2323205  0.2362689  0.2362689
Agno_01  0.059182585  0.0821274843  0.082325135  0.1300940  0.1337050  0.1337050
               inf_07      Agno_02      fiscsd    fiscmean       qfisc      recency
Agno_10  -0.015794592 -0.041367943  0.25476173  0.26961055  0.26961055 -0.559924000
Agno_09  -0.002685072 -0.022680281  0.27552583  0.29315575  0.29315575 -0.411989842
Agno_11  -0.005955077 -0.020961138  0.19563382  0.20953325  0.20953325 -0.518120358
inf_15    0.001069376 -0.016983012  0.22049458  0.22795476  0.22795476 -0.398535056
Agno_08   0.006711149 -0.009655496  0.28536627  0.30574626  0.30574626 -0.319497773
inf_16    0.001428995 -0.011284286  0.16573601  0.17313893  0.17313893 -0.373311313
inf_14    0.007895383 -0.006797014  0.23262153  0.24257566  0.24257566 -0.307160435
Agno_07   0.019142413  0.002170331  0.28593358  0.30649062  0.30649062 -0.237541748
infrac    0.192103366 -0.001639395  0.02761022  0.02777342  0.02777342 -0.000549143
inf_13    0.016799863  0.004412530  0.23964259  0.25192831  0.25192831 -0.242213265
inf_12    0.026990783  0.011645303  0.24337011  0.25540143  0.25540143 -0.183294276
Agno_06   0.029382689  0.014575596  0.28701293  0.30749547  0.30749547 -0.142080241
inf_11    0.041837885  0.028272307  0.24961793  0.26234326  0.26234326 -0.117528365
Agno_05   0.054952237  0.045816927  0.28236461  0.30351945  0.30351945 -0.043380417
inf_10    0.059968677  0.048546500  0.24390133  0.25740053  0.25740053 -0.046848365
Agno_04   0.085112713  0.077426320  0.28214573  0.30071604  0.30071604  0.060412655
inf_09    0.090234633  0.076556190  0.25068874  0.26180507  0.26180507  0.027110462
Agno_03   0.137303309  0.140730057  0.26979546  0.28658757  0.28658757  0.161591468
inf_08    0.137041335  0.130359352  0.24328492  0.25316741  0.25316741  0.099510801
infsd     0.316759736  0.179523139  0.61410495  0.62047422  0.62047422 -0.192172874
infmean   0.321109185  0.183772224  0.61283580  0.62310335  0.62310335 -0.199224088
qinf      0.321109185  0.183772224  0.61283580  0.62310335  0.62310335 -0.199224088
inf_07    1.000000000  0.735591736  0.22022888  0.22684890  0.22684890  0.178425532
Agno_02   0.735591736  1.000000000  0.23854798  0.25084141  0.25084141  0.277276020
fiscsd    0.220228877  0.238547982  1.00000000  0.99085692  0.99085692 -0.267480535
fiscmean  0.226848902  0.250841406  0.99085692  1.00000000  1.00000000 -0.291828852
qfisc     0.226848902  0.250841406  0.99085692  1.00000000  1.00000000 -0.291828852
recency   0.178425532  0.277276020 -0.26748054 -0.29182885 -0.29182885  1.000000000
inf_06    0.135231580  0.134984993  0.15256519  0.15873587  0.15873587  0.177993927
Agno_01   0.137335796  0.144851075  0.16291844  0.17373996  0.17373996  0.263970051
                inf_06      Agno_01
Agno_10  -0.0165100069 -0.033040546
Agno_09  -0.0058649534 -0.020014613
Agno_11  -0.0055316094 -0.017648705
inf_15    0.0002152439 -0.012014692
Agno_08   0.0005547368 -0.011835830
inf_16    0.0030679126 -0.008414010
inf_14    0.0034468052 -0.008144178
Agno_07   0.0102820633 -0.004115968
infrac    0.1485772783 -0.001315147
inf_13    0.0125691811  0.001800675
inf_12    0.0130150131  0.002822371
Agno_06   0.0177763731  0.008854362
inf_11    0.0291510929  0.020654558
Agno_05   0.0367760982  0.029468813
inf_10    0.0445530004  0.033854952
Agno_04   0.0545833645  0.053732225
inf_09    0.0623799802  0.059182585
Agno_03   0.0795385376  0.082127484
inf_08    0.0813949002  0.082325135
infsd     0.2323204744  0.130094009
infmean   0.2362688887  0.133704986
qinf      0.2362688887  0.133704986
inf_07    0.1352315804  0.137335796
Agno_02   0.1349849928  0.144851075
fiscsd    0.1525651936  0.162918442
fiscmean  0.1587358724  0.173739959
qfisc     0.1587358724  0.173739959
recency   0.1779939269  0.263970051
inf_06    1.0000000000  0.732806517
Agno_01   0.7328065173  1.000000000

Rattle timestamp: 2017-12-24 23:37:26 ezequieltomas
======================================================================
"


# prep pre mod bd x rut
#
nrow(nd.xrut)
# [1] 219726
nd.xrut <- nd.xrut[complete.cases(nd.xrut),]
nrow(nd.xrut)
# [1] 219726
ncol(nd.xrut)
# [1] 34
colnames(nd.xrut)
'
 [1] "RutEmpresa" "crae"       "Region"     "Agno_01"    "inf_06"    
 [6] "Agno_02"    "inf_07"     "Agno_03"    "inf_08"     "Agno_04"   
[11] "inf_09"     "Agno_05"    "inf_10"     "Agno_06"    "inf_11"    
[16] "Agno_07"    "inf_12"     "Agno_08"    "inf_13"     "Agno_09"   
[21] "inf_14"     "Agno_10"    "inf_15"     "Agno_11"    "inf_16"    
[26] "fiscmean"   "infmean"    "qfisc"      "qinf"       "fiscsd"    
[31] "infsd"      "recency"    "infrac"     "Infractor" 
'

#modelamiento para base por rut
nd <- nd.xrut
#nd <- nd.xrut[c(-32)] # se elimina recency, no aporta a los modelos

ndxcorr<-nd[c(-1,-27,-28,-29,-30)]

# correlaciones entre variables
# realizado con rattle version 5.1.3

"
Correlation summary using the 'Spearman' covariance.

Note that only correlations between numeric variables are reported.

              Agno_10       Agno_09      Agno_11        inf_15       Agno_08
Agno_10   1.000000000  0.1746277943  0.187819774  0.7160462387  0.1134700177
Agno_09   0.174627794  1.0000000000  0.127829715  0.1552200765  0.1840869584
Agno_11   0.187819774  0.1278297153  1.000000000  0.1745121100  0.0985238685
inf_15    0.716046239  0.1552200765  0.174512110  1.0000000000  0.1085287148
Agno_08   0.113470018  0.1840869584  0.098523869  0.1085287148  1.0000000000
inf_16    0.154288087  0.1080743385  0.727744299  0.1564053994  0.0825282410
inf_14    0.156547595  0.7416579790  0.115270470  0.1487665174  0.1617853185
Agno_07   0.072463929  0.1151273178  0.064845829  0.0730833654  0.1783446559
infrac   -0.016892550  0.0031114329 -0.004781823  0.1760697311  0.0061597241
inf_13    0.107574090  0.1623688454  0.092505748  0.1094594608  0.7416827642
inf_12    0.070002289  0.1070339120  0.064249525  0.0792442553  0.1655215127
Agno_06   0.040102753  0.0729595105  0.040397559  0.0467432426  0.1038959492
inf_11    0.050386534  0.0748305104  0.049862903  0.0594689315  0.1035021648
Agno_05   0.016673890  0.0392854019  0.023280514  0.0278022704  0.0680298045
inf_10    0.027730098  0.0454900627  0.030229560  0.0392127538  0.0712838771
Agno_04  -0.008457059  0.0149263327  0.005204377  0.0095728630  0.0352250811
inf_09    0.008324138  0.0269614213  0.017502032  0.0234523267  0.0440331856
Agno_03  -0.023646391  0.0005115566 -0.007560760 -0.0041584347  0.0154718985
inf_08   -0.004811516  0.0134436938  0.003779203  0.0110446509  0.0257018803
infsd     0.179603435  0.2137988323  0.151984801  0.3063031310  0.2256531303
inf_07   -0.015794592 -0.0026850721 -0.005955077  0.0010693760  0.0067111494
Agno_02  -0.041367943 -0.0226802815 -0.020961138 -0.0169830121 -0.0096554965
fiscmean  0.269610547  0.2931557509  0.209533246  0.2279547568  0.3057462622
recency  -0.559924000 -0.4119898419 -0.518120358 -0.3985350559 -0.3194977726
inf_06   -0.016510007 -0.0058649534 -0.005531609  0.0002152439  0.0005547368
Agno_01  -0.033040546 -0.0200146129 -0.017648705 -0.0120146925 -0.0118358295
                inf_16       inf_14      Agno_07        infrac       inf_13
Agno_10   0.1542880868  0.156547595  0.072463929 -0.0168925500  0.107574090
Agno_09   0.1080743385  0.741657979  0.115127318  0.0031114329  0.162368845
Agno_11   0.7277442985  0.115270470  0.064845829 -0.0047818226  0.092505748
inf_15    0.1564053994  0.148766517  0.073083365  0.1760697311  0.109459461
Agno_08   0.0825282410  0.161785319  0.178344656  0.0061597241  0.741682764
inf_16    1.0000000000  0.106521576  0.057372296  0.1318803204  0.082874709
inf_14    0.1065215755  1.000000000  0.104020763  0.1603849741  0.153667184
Agno_07   0.0573722962  0.104020763  1.000000000  0.0102364441  0.155512331
infrac    0.1318803204  0.160384974  0.010236444  1.0000000000  0.156756800
inf_13    0.0828747090  0.153667184  0.155512331  0.1567567998  1.000000000
inf_12    0.0608645879  0.104718585  0.743871075  0.1703152601  0.157827699
Agno_06   0.0399569312  0.066998800  0.160752106  0.0008328139  0.095605620
inf_11    0.0515656640  0.074584676  0.156334350  0.1736262068  0.101925933
Agno_05   0.0251388280  0.040819758  0.096453397  0.0072601658  0.066986551
inf_10    0.0322789811  0.049100519  0.096392783  0.1758520810  0.072687816
Agno_04   0.0120619734  0.022141935  0.051649688  0.0045921726  0.039923640
inf_09    0.0227825583  0.032634666  0.061850308  0.1785620807  0.049676441
Agno_03  -0.0003389463  0.009746978  0.025658324  0.0032072489  0.024692176
inf_08    0.0089219360  0.023258413  0.036159345  0.1832896712  0.033412123
infsd     0.2325809732  0.307358273  0.230189473  0.7363640363  0.311590260
inf_07    0.0014289946  0.007895383  0.019142413  0.1921033656  0.016799863
Agno_02  -0.0112842862 -0.006797014  0.002170331 -0.0016393952  0.004412530
fiscmean  0.1731389298  0.242575655  0.306490617  0.0277734167  0.251928312
recency  -0.3733113127 -0.307160435 -0.237541748 -0.0005491430 -0.242213265
inf_06    0.0030679126  0.003446805  0.010282063  0.1485772783  0.012569181
Agno_01  -0.0084140104 -0.008144178 -0.004115968 -0.0013151468  0.001800675
               inf_12       Agno_06      inf_11      Agno_05      inf_10
Agno_10   0.070002289  0.0401027533  0.05038653  0.016673890  0.02773010
Agno_09   0.107033912  0.0729595105  0.07483051  0.039285402  0.04549006
Agno_11   0.064249525  0.0403975590  0.04986290  0.023280514  0.03022956
inf_15    0.079244255  0.0467432426  0.05946893  0.027802270  0.03921275
Agno_08   0.165521513  0.1038959492  0.10350216  0.068029804  0.07128388
inf_16    0.060864588  0.0399569312  0.05156566  0.025138828  0.03227898
inf_14    0.104718585  0.0669987997  0.07458468  0.040819758  0.04910052
Agno_07   0.743871075  0.1607521065  0.15633435  0.096453397  0.09639278
infrac    0.170315260  0.0008328139  0.17362621  0.007260166  0.17585208
inf_13    0.157827699  0.0956056202  0.10192593  0.066986551  0.07268782
inf_12    1.000000000  0.1428833457  0.15192569  0.088866040  0.09496906
Agno_06   0.142883346  1.0000000000  0.73781790  0.154073073  0.14762178
inf_11    0.151925686  0.7378178990  1.00000000  0.138741988  0.14385899
Agno_05   0.088866040  0.1540730733  0.13874199  1.000000000  0.74087252
inf_10    0.094969060  0.1476217785  0.14385899  0.740872520  1.00000000
Agno_04   0.052092247  0.0846034431  0.08265065  0.156506753  0.13559951
inf_09    0.063088100  0.0887454887  0.09144728  0.149318240  0.14056653
Agno_03   0.029498831  0.0505641190  0.05459964  0.084463781  0.07646386
inf_08    0.039478705  0.0584035234  0.06456999  0.086966411  0.08407667
infsd     0.323954881  0.2228445233  0.33104990  0.223991989  0.32705399
inf_07    0.026990783  0.0293826887  0.04183789  0.054952237  0.05996868
Agno_02   0.011645303  0.0145755963  0.02827231  0.045816927  0.04854650
fiscmean  0.255401426  0.3074954661  0.26234326  0.303519454  0.25740053
recency  -0.183294276 -0.1420802408 -0.11752836 -0.043380417 -0.04684837
inf_06    0.013015013  0.0177763731  0.02915109  0.036776098  0.04455300
Agno_01   0.002822371  0.0088543619  0.02065456  0.029468813  0.03385495
              Agno_04      inf_09       Agno_03       inf_08      infsd
Agno_10  -0.008457059 0.008324138 -0.0236463905 -0.004811516  0.1796034
Agno_09   0.014926333 0.026961421  0.0005115566  0.013443694  0.2137988
Agno_11   0.005204377 0.017502032 -0.0075607599  0.003779203  0.1519848
inf_15    0.009572863 0.023452327 -0.0041584347  0.011044651  0.3063031
Agno_08   0.035225081 0.044033186  0.0154718985  0.025701880  0.2256531
inf_16    0.012061973 0.022782558 -0.0003389463  0.008921936  0.2325810
inf_14    0.022141935 0.032634666  0.0097469783  0.023258413  0.3073583
Agno_07   0.051649688 0.061850308  0.0256583238  0.036159345  0.2301895
infrac    0.004592173 0.178562081  0.0032072489  0.183289671  0.7363640
inf_13    0.039923640 0.049676441  0.0246921765  0.033412123  0.3115903
inf_12    0.052092247 0.063088100  0.0294988309  0.039478705  0.3239549
Agno_06   0.084603443 0.088745489  0.0505641190  0.058403523  0.2228445
inf_11    0.082650651 0.091447284  0.0545996356  0.064569991  0.3310499
Agno_05   0.156506753 0.149318240  0.0844637809  0.086966411  0.2239920
inf_10    0.135599513 0.140566531  0.0764638577  0.084076665  0.3270540
Agno_04   1.000000000 0.740533584  0.1582447707  0.154656627  0.2205020
inf_09    0.740533584 1.000000000  0.1433360491  0.151499990  0.3330083
Agno_03   0.158244771 0.143336049  1.0000000000  0.742052847  0.2092719
inf_08    0.154656627 0.151499990  0.7420528470  1.000000000  0.3306809
infsd     0.220502048 0.333008349  0.2092719124  0.330680874  1.0000000
inf_07    0.085112713 0.090234633  0.1373033092  0.137041335  0.3167597
Agno_02   0.077426320 0.076556190  0.1407300565  0.130359352  0.1795231
fiscmean  0.300716038 0.261805074  0.2865875657  0.253167406  0.6204742
recency   0.060412655 0.027110462  0.1615914675  0.099510801 -0.1921729
inf_06    0.054583365 0.062379980  0.0795385376  0.081394900  0.2323205
Agno_01   0.053732225 0.059182585  0.0821274843  0.082325135  0.1300940
               inf_07      Agno_02    fiscmean      recency        inf_06
Agno_10  -0.015794592 -0.041367943  0.26961055 -0.559924000 -0.0165100069
Agno_09  -0.002685072 -0.022680281  0.29315575 -0.411989842 -0.0058649534
Agno_11  -0.005955077 -0.020961138  0.20953325 -0.518120358 -0.0055316094
inf_15    0.001069376 -0.016983012  0.22795476 -0.398535056  0.0002152439
Agno_08   0.006711149 -0.009655496  0.30574626 -0.319497773  0.0005547368
inf_16    0.001428995 -0.011284286  0.17313893 -0.373311313  0.0030679126
inf_14    0.007895383 -0.006797014  0.24257566 -0.307160435  0.0034468052
Agno_07   0.019142413  0.002170331  0.30649062 -0.237541748  0.0102820633
infrac    0.192103366 -0.001639395  0.02777342 -0.000549143  0.1485772783
inf_13    0.016799863  0.004412530  0.25192831 -0.242213265  0.0125691811
inf_12    0.026990783  0.011645303  0.25540143 -0.183294276  0.0130150131
Agno_06   0.029382689  0.014575596  0.30749547 -0.142080241  0.0177763731
inf_11    0.041837885  0.028272307  0.26234326 -0.117528365  0.0291510929
Agno_05   0.054952237  0.045816927  0.30351945 -0.043380417  0.0367760982
inf_10    0.059968677  0.048546500  0.25740053 -0.046848365  0.0445530004
Agno_04   0.085112713  0.077426320  0.30071604  0.060412655  0.0545833645
inf_09    0.090234633  0.076556190  0.26180507  0.027110462  0.0623799802
Agno_03   0.137303309  0.140730057  0.28658757  0.161591468  0.0795385376
inf_08    0.137041335  0.130359352  0.25316741  0.099510801  0.0813949002
infsd     0.316759736  0.179523139  0.62047422 -0.192172874  0.2323204744
inf_07    1.000000000  0.735591736  0.22684890  0.178425532  0.1352315804
Agno_02   0.735591736  1.000000000  0.25084141  0.277276020  0.1349849928
fiscmean  0.226848902  0.250841406  1.00000000 -0.291828852  0.1587358724
recency   0.178425532  0.277276020 -0.29182885  1.000000000  0.1779939269
inf_06    0.135231580  0.134984993  0.15873587  0.177993927  1.0000000000
Agno_01   0.137335796  0.144851075  0.17373996  0.263970051  0.7328065173
              Agno_01
Agno_10  -0.033040546
Agno_09  -0.020014613
Agno_11  -0.017648705
inf_15   -0.012014692
Agno_08  -0.011835830
inf_16   -0.008414010
inf_14   -0.008144178
Agno_07  -0.004115968
infrac   -0.001315147
inf_13    0.001800675
inf_12    0.002822371
Agno_06   0.008854362
inf_11    0.020654558
Agno_05   0.029468813
inf_10    0.033854952
Agno_04   0.053732225
inf_09    0.059182585
Agno_03   0.082127484
inf_08    0.082325135
infsd     0.130094009
inf_07    0.137335796
Agno_02   0.144851075
fiscmean  0.173739959
recency   0.263970051
inf_06    0.732806517
Agno_01   1.000000000

Rattle timestamp: 2017-12-25 00:46:19 ezequieltomas
======================================================================
"


#separación por años
# se define el entrenamiento por el conjunto de datos desde el 2006 al 2014
train <- subset(nd, nd$Agno_01 >= 1| nd$Agno_02 >= 1|nd$Agno_03 >= 1|nd$Agno_04 >= 1|nd$Agno_05 >= 1|nd$Agno_06 >= 1
	|nd$Agno_07 >= 1|nd$Agno_08 >= 1|nd$Agno_09 >= 1)
# se eliminan del train los datos del 2015 (22 y 23) y 2016 (24 y 25), y se saca el RUT
# se eliminan los atributos con mayor correlación
# infmean(27), qfisc(28), qinf(29) y fiscsd (30)
train <-train[c(-1,-22,-23,-24,-25,-27,-28,-29,-30)]

#train2 <- subset(nd, nd$Agno_08 >= 1|nd$Agno_09 >= 1|nd$Agno_10 >= 1)
#train2 <-train2[c(-1,-24,-25)]

# se define como test los datos del año 2015 y 2016
#test <- subset(nd, nd$Agno_11 >= 1)
#test <- subset(nd, nd$Agno_11 >= 1 | nd$Agno_10 >= 1)
test <- subset(nd, nd$Agno_10 >= 1)
# se eliminan los datos del 2015 y 2016 y se saca el RUT
test <- test[c(-22,-23,-24,-25,-27,-28,-28,-29,-30,-1)]


trainxRUT <- train
testxRUT <- test

nrow(trainxRUT)
# [1] 192162
ncol(trainxRUT)
# [1] 25
colnames(trainxRUT)
'
 [1] "crae"      "Region"    "Agno_01"   "inf_06"    "Agno_02"   "inf_07"   
 [7] "Agno_03"   "inf_08"    "Agno_04"   "inf_09"    "Agno_05"   "inf_10"   
[13] "Agno_06"   "inf_11"    "Agno_07"   "inf_12"    "Agno_08"   "inf_13"   
[19] "Agno_09"   "inf_14"    "fiscmean"  "infsd"     "recency"   "infrac"   
[25] "Infractor"
'
nrow(testxRUT)
# [1] 36730
ncol(testxRUT)
# [1] 25
colnames(testxRUT)
'
 [1] "crae"      "Region"    "Agno_01"   "inf_06"    "Agno_02"   "inf_07"   
 [7] "Agno_03"   "inf_08"    "Agno_04"   "inf_09"    "Agno_05"   "inf_10"   
[13] "Agno_06"   "inf_11"    "Agno_07"   "inf_12"    "Agno_08"   "inf_13"   
[19] "Agno_09"   "inf_14"    "fiscmean"  "infsd"     "recency"   "infrac"   
[25] "Infractor"
'
#
'
var1 <- as.data.frame(dummy(nd.xrut$Region))
var2 <- as.data.frame(dummy(nd.xrut$crae))
#var3 <- as.data.frame(dummy(nd.xrut$tramoventasSII))
#
#juntar las dummies en un nuevo data frame
#fd <- cbind(nd.xrut[c(-2,-3,-5)], var1, var2,var3)
fd <- cbind(nd.xrut[c(-2,-3)], var1, var2)
#
z <- colnames(fd)
hd <-gsub(")", "\\.",colnames(fd)) 
colnames(fd) <- hd
#
ar <- "Infractor ~"
for (i in 2:length(fd)){
	ar = paste(ar, sprintf("`%s`",hd[i]), sep=" + ")
}
f20170927xrut <- as.formula( ar )
#
# para validación 75%-25%
nrow(fd)
# [1] 219726
ncol(fd)
# [1] 64
set.seed(1234)
index <- sample(1:nrow(fd), round(0.75*nrow(fd)))
train20170927xrut <- fd[index,]
nrow(train20170927xrut)
# [1] 164794
ncol(train20170927xrut)
# [1] 64
test20170927xrut <- fd[-index,]
nrow(test20170927xrut)
# [1] 54932
ncol(test20170927xrut)
# [1] 64
#
'
#

# ejecucion modelos x rut
#
train <- trainxRUT
test <- testxRUT
seqcut <- seq(0, 1 , by=0.05)
#
# arbol ctree
#
dt.1.20170927xrut <- party::ctree(Infractor ~. , data=train)
#
dframect <- data.frame(Threshold = double(), Recall = double(),Precision = double(), Fscore = double(), Accuracy = double() )
for(i in 1:20){
	resdt.1 <- predict(dt.1.20170927xrut , test[,-25] , type= "response")
	resdt.1 <- ifelse(resdt.1 > seqcut[i] , 1,0)
	predct <- caret::confusionMatrix(data = resdt.1, reference = test$Infractor, positive="1")
	dframect[i,1] <- seqcut[i]
	dframect[i,2] <- predct$byClass[1]
	dframect[i,3] <- predct$byClass[3]
	dframect[i,4] <- predct$byClass[7]
	dframect[i,5] <- predct$overall[1]
}
dfct20170927xrut <- melt(dframect, id=c("Threshold"))
graficodfct20170927xrut<-ggplot( data = dfct20170927xrut , aes(x=Threshold , y = value, group= variable, colour = variable )  )+ geom_line() + ggtitle ("Cálculo del Threshold para ctree") + theme (plot.title = element_text(face="bold"))
jpeg(filename="1-graficodfct20170927xrut.jpeg")
graficodfct20170927xrut
dev.off()
#
# se selecciona 0.4375 como punto
resdt.1 <- predict(dt.1.20170927xrut , test[,-25], type="response")
resdt.1 <- ifelse(resdt.1 > 0.4375,1,0)
cmdt.1.20170927xrut <- caret::confusionMatrix(data=resdt.1 , reference = test$Infractor , positive = "1")
cmdt.1.20170927xrut
"
Confusion Matrix and Statistics

          Reference
Prediction     0     1
         0 14690  2326
         1  2229 17485
                                          
               Accuracy : 0.876           
                 95% CI : (0.8726, 0.8793)
    No Information Rate : 0.5394          
    P-Value [Acc > NIR] : <2e-16          
                                          
                  Kappa : 0.7505          
 Mcnemar's Test P-Value : 0.1549          
                                          
            Sensitivity : 0.8826          
            Specificity : 0.8683          
         Pos Pred Value : 0.8869          
         Neg Pred Value : 0.8633          
             Prevalence : 0.5394          
         Detection Rate : 0.4760          
   Detection Prevalence : 0.5367          
      Balanced Accuracy : 0.8754          
                                          
       'Positive' Class : 1 
"
#
cmdt.1.20170927xrut$byClass
'
         Sensitivity          Specificity       Pos Pred Value 
           0.8825905            0.8682546            0.8869331 
      Neg Pred Value            Precision               Recall 
           0.8633051            0.8869331            0.8825905 
                  F1           Prevalence       Detection Rate 
           0.8847565            0.5393684            0.4760414 
Detection Prevalence    Balanced Accuracy 
           0.5367275            0.8754226 
'
#
rcdt.1.20170927xrut <- roc.curve(response = test$Infractor , predicted = resdt.1, plotit = TRUE)
rcdt.1.20170927xrut
'
Area under the curve (AUC): 0.875
'
jpeg(filename="1-rcdt.1.20170927xrut.jpeg")
roc.curve(response = test$Infractor , predicted = resdt.1, plotit = TRUE)
dev.off()
summary(rcdt.1.20170927xrut)
'
Call: 
roc.curve(response = test$Infractor, predicted = resdt.1, plotit = TRUE)

Area under the curve (AUC): 
0.875 

False positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
0.00000 0.06587 0.13170 0.37720 0.56590 1.00000 

True positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
 0.0000  0.4413  0.8826  0.6275  0.9413  1.0000 
'
#summary(dt.1.20170927xrut)
#printcp(dt.1.20170927xrut)
#plot(dt.1.20170927xrut)
#

#
# arbol Random Forest
#
tuneRF20170927xrut <- tuneRF(x=train[,c(-25)] , y=as.factor(train$Infractor),stepFactor=1)
'
mtry = 4  OOB error = 2.14% 
Searching left ...
Searching right ...
'
tuneRF20170927xrut <- tuneRF(x=train[,c(-25)] , y=as.factor(train$Infractor),stepFactor=1.5)
'
mtry = 4  OOB error = 2.12% 
Searching left ...
mtry = 3 	OOB error = 2.17% 
-0.02678133 0.05 
Searching right ...
mtry = 6 	OOB error = 2.02% 
0.04471744 0.05 
'
tuneRF20170927xrut <- tuneRF(x=train[,c(-25)] , y=as.factor(train$Infractor),stepFactor=2)
'
mtry = 4  OOB error = 2.08% 
Searching left ...
mtry = 2 	OOB error = 3.01% 
-0.4482845 0.05 
Searching right ...
mtry = 8 	OOB error = 2.05% 
0.01402454 0.05 
'
tuneRF20170927xrut <- tuneRF(x=train[,c(-25)] , y=as.factor(train$Infractor),stepFactor=2.5)
'
mtry = 4  OOB error = 2.12% 
Searching left ...
mtry = 2 	OOB error = 3.01% 
-0.4210656 0.05 
Searching right ...
mtry = 10 	OOB error = 2.05% 
0.03339062 0.05 
'
tuneRF20170927xrut <- tuneRF(x=train[,c(-25)] , y=as.factor(train$Infractor),stepFactor=3)
'
mtry = 4  OOB error = 2.05% 
Searching left ...
mtry = 2 	OOB error = 3% 
-0.466582 0.05 
Searching right ...
mtry = 12 	OOB error = 2.01% 
0.01778907 0.05 
'
tuneRF20170927xrut <- tuneRF(x=train[,c(-25)] , y=as.factor(train$Infractor),stepFactor=3.5)
'
mtry = 4  OOB error = 2.11% 
Searching left ...
mtry = 2 	OOB error = 3.03% 
-0.4409293 0.05 
Searching right ...
mtry = 14 	OOB error = 2.06% 
0.02174988 0.05 
'
#
rf.1.20170927xrut <- randomForest(x=train[,c(-25)] , y=as.factor(train$Infractor), importance = TRUE , ntree= 100 ,  mtry = 12 , replace= TRUE)
#
dframe <- data.frame(Threshold = double(), Recall = double(),Precision = double(), Fscore = double(), Accuracy = double() )
for(i in 1:20){
	resrf.1 <- predict(rf.1.20170927xrut , test[,c(-25)] , type= "prob")
	resrf.1 <- ifelse(resrf.1[,2]> seqcut[i] , 1,0)
	predrf <- caret::confusionMatrix(data = resrf.1, reference = test$Infractor, positive="1")
	dframe[i,1] <- seqcut[i]
	dframe[i,2] <- predrf$byClass[1]
	dframe[i,3] <- predrf$byClass[3]
	dframe[i,4] <- predrf$byClass[7]
	dframe[i,5] <- predrf$overall[1]
}
dfrf20170927xrut <- melt(dframe, id=c("Threshold"))
graficodfrf20170927xrut<-ggplot( data = dfrf20170927xrut , aes(x=Threshold , y = value, group= variable, colour = variable )  )+ geom_line() + ggtitle ("Cálculo del Threshold para Random Forest") + theme (plot.title = element_text(face="bold"))
jpeg(filename="1-graficodfrf20170927xrut.jpeg")
graficodfrf20170927xrut
dev.off()
#
# se selecciona 0.292 como punto
resrf.1 <- predict(rf.1.20170927xrut , test[,c(-25)] , type= "prob")
resrf.1 <- ifelse(resrf.1[,2]> 0.292 , 1,0)
cmrf.1.20170927xrut <- caret::confusionMatrix(data = resrf.1, reference = test$Infractor, positive="1")
cmrf.1.20170927xrut
"
Confusion Matrix and Statistics

          Reference
Prediction     0     1
         0 16574   631
         1   345 19180
                                         
               Accuracy : 0.9734         
                 95% CI : (0.9717, 0.975)
    No Information Rate : 0.5394         
    P-Value [Acc > NIR] : < 2.2e-16      
                                         
                  Kappa : 0.9466         
 Mcnemar's Test P-Value : < 2.2e-16      
                                         
            Sensitivity : 0.9681         
            Specificity : 0.9796         
         Pos Pred Value : 0.9823         
         Neg Pred Value : 0.9633         
             Prevalence : 0.5394         
         Detection Rate : 0.5222         
   Detection Prevalence : 0.5316         
      Balanced Accuracy : 0.9739         
                                         
       'Positive' Class : 1 
"
cmrf.1.20170927xrut$byClass
'
         Sensitivity          Specificity       Pos Pred Value 
           0.9681490            0.9796087            0.9823303 
      Neg Pred Value            Precision               Recall 
           0.9633246            0.9823303            0.9681490 
                  F1           Prevalence       Detection Rate 
           0.9751881            0.5393684            0.5221889 
Detection Prevalence    Balanced Accuracy 
           0.5315818            0.9738789 
'
rcrf.1.20170927xrut <- roc.curve(response = test$Infractor , predicted = resrf.1, plotit = TRUE)
rcrf.1.20170927xrut
'
Area under the curve (AUC): 0.974
'
jpeg(filename="1-rcrf.1.20170927xrut.jpeg")
roc.curve(response = test$Infractor , predicted = resrf.1, plotit = TRUE)
dev.off()
summary(rcrf.1.20170927xrut)
'
Call: 
roc.curve(response = test$Infractor, predicted = resrf.1, plotit = TRUE)

Area under the curve (AUC): 
0.974 

False positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
0.00000 0.01020 0.02039 0.34010 0.51020 1.00000 

True positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
 0.0000  0.4841  0.9681  0.6560  0.9841  1.0000 
'
summary(rf.1.20170927xrut)
'
                Length Class  Mode     
call                 7 -none- call     
type                 1 -none- character
predicted       192162 factor numeric  
err.rate           300 -none- numeric  
confusion            6 -none- numeric  
votes           384324 matrix numeric  
oob.times       192162 -none- numeric  
classes              2 -none- character
importance          96 -none- numeric  
importanceSD        72 -none- numeric  
localImportance      0 -none- NULL     
proximity            0 -none- NULL     
ntree                1 -none- numeric  
mtry                 1 -none- numeric  
forest              14 -none- list     
y               192162 factor numeric  
test                 0 -none- NULL     
inbag                0 -none- NULL
'
varImpPlot(rf.1.20170927xrut)
jpeg(filename="1-rcrf.1.20170927xrut-varimpplot.jpeg")
varImpPlot(rf.1.20170927xrut)
dev.off()
#
# cubist (Rule- And Instance-Based Regression Modeling)
#
cu.1.20170927xrut <- cubist(x=train[,c(-25)] , y=train$Infractor)
#
dframecu <- data.frame(Threshold = double(), Recall = double(),Precision = double(), Fscore = double(), Accuracy = double() )
for(i in 1:20){
	rescu.1 <- predict(cu.1.20170927xrut , test[,c(-25)] , type= "response")
	rescu.1 <- ifelse(rescu.1> seqcut[i] , 1,0)
	predcu.1 <- caret::confusionMatrix(data = rescu.1, reference = test$Infractor, positive="1")
	dframecu[i,1] <- seqcut[i]
	dframecu[i,2] <- predcu.1$byClass[1]
	dframecu[i,3] <- predcu.1$byClass[3]
	dframecu[i,4] <- predcu.1$byClass[7]
	dframecu[i,5] <- predcu.1$overall[1]
}
dfcu20170927xrut.1 <- melt(dframecu, id=c("Threshold"))
graficodfcu20170927xrut.1<-ggplot( data = dfcu20170927xrut.1 , aes(x=Threshold , y = value, group= variable, colour = variable )  )+ geom_line() + ggtitle ("Cálculo del Threshold para cubist") + theme (plot.title = element_text(face="bold"))
jpeg(filename="1-graficodfcu20170927xrut.1.jpeg")
graficodfcu20170927xrut.1
dev.off()
#
rescu.1 <- predict(cu.1.20170927xrut , test[,-25], type="response")
rescu.1 <- ifelse(rescu.1 > 0.4063,1,0)
cmcu.1.20170927xrut <- caret::confusionMatrix(data = rescu.1 , reference = test$Infractor, positive="1")
cmcu.1.20170927xrut
"
Confusion Matrix and Statistics

          Reference
Prediction     0     1
         0 15859  1092
         1  1060 18719
                                         
               Accuracy : 0.9414         
                 95% CI : (0.939, 0.9438)
    No Information Rate : 0.5394         
    P-Value [Acc > NIR] : <2e-16         
                                         
                  Kappa : 0.8821         
 Mcnemar's Test P-Value : 0.504          
                                         
            Sensitivity : 0.9449         
            Specificity : 0.9373         
         Pos Pred Value : 0.9464         
         Neg Pred Value : 0.9356         
             Prevalence : 0.5394         
         Detection Rate : 0.5096         
   Detection Prevalence : 0.5385         
      Balanced Accuracy : 0.9411         
                                         
       'Positive' Class : 1  
"
cmcu.1.20170927xrut$byClass
'
         Sensitivity          Specificity       Pos Pred Value       Neg Pred Value 
           0.9448791            0.9373485            0.9464078            0.9355790 
           Precision               Recall                   F1           Prevalence 
           0.9464078            0.9448791            0.9456428            0.5393684 
      Detection Rate Detection Prevalence    Balanced Accuracy 
           0.5096379            0.5384971            0.9411138 
'
rccu.1.20170927xrut <- roc.curve(response = test$Infractor , predicted = rescu.1, plotit = TRUE)
rccu.1.20170927xrut
'
Area under the curve (AUC): 0.941
'
jpeg(filename="1-rccu.1.20170927xrut.jpeg")
roc.curve(response = test$Infractor , predicted = rescu.1, plotit = TRUE)
dev.off()
summary(rccu.1.20170927xrut)
'
Call: 
roc.curve(response = test$Infractor, predicted = rescu.1, plotit = TRUE)

Area under the curve (AUC): 
0.941 

False positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
0.00000 0.03133 0.06265 0.35420 0.53130 1.00000 

True positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
 0.0000  0.4724  0.9449  0.6483  0.9724  1.0000 
'
summary(cu.1.20170927xrut)
'
Evaluation on training data (192162 cases, sampled):

    Average  |error|                0.0
    Relative |error|               0.17
    Correlation coefficient        0.90


	Attribute usage:
	  Conds  Model

	   63%     5%    recency
	   27%     6%    infrac
	   20%     7%    infsd
	   10%     5%    fiscmean
	    9%     3%    inf_14
	    6%     6%    inf_09
	    6%     6%    inf_13
	    6%     6%    inf_07
	    5%     6%    inf_12
	    3%     5%    inf_06
	    2%     8%    inf_10
	    2%     8%    inf_08
	    2%     8%    inf_11
	           3%    Agno_09
	           3%    Agno_04
	           3%    Agno_02
	           3%    Agno_03
	           3%    Agno_07
	           3%    Agno_06
	           3%    Agno_05
	           3%    Agno_08
	           3%    Agno_01


Time: 9.6 secs
'
#

#
#logit
logit0 <- glm(Infractor ~. ,family=binomial(link='logit'), data=train , control = list(maxit = 1000))
#
summary(logit0)
'
Call:
glm(formula = Infractor ~ ., family = binomial(link = "logit"), 
    data = train, control = list(maxit = 1000))

Deviance Residuals: 
    Min       1Q   Median       3Q      Max  
-8.4904  -0.1147  -0.0341  -0.0086   3.6612  

Coefficients:
             Estimate Std. Error z value Pr(>|z|)    
(Intercept) -3.226401   0.115664 -27.895  < 2e-16 ***
crae102     -0.101882   0.209070  -0.487   0.6260    
crae103      0.184902   0.155819   1.187   0.2354    
crae104      0.016318   0.072694   0.224   0.8224    
crae105     -0.070357   0.224809  -0.313   0.7543    
crae106     -0.158670   0.075477  -2.102   0.0355 *  
crae107     -0.017273   0.066610  -0.259   0.7954    
crae108      0.023167   0.072438   0.320   0.7491    
crae109      0.027387   0.073028   0.375   0.7076    
crae110     -0.350922   0.137741  -2.548   0.0108 *  
crae111     -0.172388   0.071194  -2.421   0.0155 *  
crae112     -0.202105   0.241424  -0.837   0.4025    
crae113      0.041062   0.084174   0.488   0.6257    
crae114     -0.196203   0.119381  -1.643   0.1003    
crae115     -0.139035   0.079513  -1.749   0.0804 .  
crae116     -0.126978   0.100670  -1.261   0.2072    
crae117     -9.262613  71.868597  -0.129   0.8975    
Region2     -0.109079   0.108649  -1.004   0.3154    
Region3      0.053456   0.127028   0.421   0.6739    
Region4      0.076864   0.110989   0.693   0.4886    
Region5      0.039650   0.095724   0.414   0.6787    
Region6      0.164602   0.101072   1.629   0.1034    
Region7     -0.077082   0.101933  -0.756   0.4495    
Region8     -0.009598   0.096885  -0.099   0.9211    
Region9      0.105545   0.103311   1.022   0.3070    
Region10     0.127475   0.102285   1.246   0.2127    
Region11    -0.136485   0.184305  -0.741   0.4590    
Region12    -0.030944   0.141830  -0.218   0.8273    
Region13    -0.112828   0.089088  -1.266   0.2053    
Region14     0.036125   0.126697   0.285   0.7755    
Region15     0.155919   0.144087   1.082   0.2792    
Agno_01     -0.700500   0.032581 -21.500  < 2e-16 ***
inf_06      -0.113751   0.052894  -2.151   0.0315 *  
Agno_02     -0.677320   0.024214 -27.972  < 2e-16 ***
inf_07      -0.278230   0.035252  -7.893 2.96e-15 ***
Agno_03     -0.679708   0.023719 -28.657  < 2e-16 ***
inf_08      -0.239176   0.032933  -7.263 3.80e-13 ***
Agno_04     -0.641571   0.020650 -31.069  < 2e-16 ***
inf_09      -0.338689   0.029713 -11.399  < 2e-16 ***
Agno_05     -0.647422   0.023003 -28.145  < 2e-16 ***
inf_10      -0.329277   0.034373  -9.580  < 2e-16 ***
Agno_06     -0.687732   0.021774 -31.585  < 2e-16 ***
inf_11      -0.232665   0.030203  -7.703 1.32e-14 ***
Agno_07     -0.678389   0.023554 -28.801  < 2e-16 ***
inf_12      -0.283207   0.032664  -8.670  < 2e-16 ***
Agno_08     -0.588500   0.023477 -25.067  < 2e-16 ***
inf_13      -0.452720   0.032308 -14.013  < 2e-16 ***
Agno_09     -0.510374   0.024001 -21.264  < 2e-16 ***
inf_14      -0.671156   0.031472 -21.326  < 2e-16 ***
fiscmean     7.751926   0.154854  50.060  < 2e-16 ***
infsd        1.711905   0.066099  25.899  < 2e-16 ***
recency     -1.034850   0.015562 -66.497  < 2e-16 ***
infrac       2.946853   0.066155  44.545  < 2e-16 ***
---
Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1

(Dispersion parameter for binomial family taken to be 1)

    Null deviance: 82485  on 192161  degrees of freedom
Residual deviance: 33262  on 192109  degrees of freedom
AIC: 33368

Number of Fisher Scoring iterations: 13
'
dframelog <- data.frame(Threshold = double(), Recall = double(),Precision = double(), Fscore = double(), Accuracy = double() )
for(i in 1:20){
	lg.res <- predict(logit0 , test[,-25], type="response" )
	lg.res <- ifelse(lg.res>= seqcut[i] , 1,0)
	predlg <- caret::confusionMatrix(lg.res, test$Infractor, positive="1")
	dframelog[i,1] <- seqcut[i]
	dframelog[i,2] <- predlg$byClass[1]
	dframelog[i,3] <- predlg$byClass[3]
	dframelog[i,4] <- predlg$byClass[7]
	dframelog[i,5] <- predlg$overall[1]
}

dflg20170927xrut <- melt(dframelog, id=c("Threshold"))

graficodflg20170927xrut<-ggplot( data = dflg20170927xrut , aes(x=Threshold , y = value, group= variable, colour = variable )  )+ geom_line() + ggtitle ("Cálculo del Threshold para Regresión Logística") + theme (plot.title = element_text(face="bold"))
jpeg(filename="1-graficodflg20170927xrut.jpeg")
graficodflg20170927xrut
dev.off()

ct <- 0.2292
reslogit0 <- predict(logit0,test[,-25] , type = 'response')
reslogit0 <- ifelse(reslogit0 > ct,1,0)
logres0 <- caret::confusionMatrix(reslogit0 , test$Infractor , positive = "1")
logres0
"
Confusion Matrix and Statistics

          Reference
Prediction     0     1
         0 14197  2474
         1  2722 17337
                                          
               Accuracy : 0.8585          
                 95% CI : (0.8549, 0.8621)
    No Information Rate : 0.5394          
    P-Value [Acc > NIR] : < 2.2e-16       
                                          
                  Kappa : 0.715           
 Mcnemar's Test P-Value : 0.0006112       
                                          
            Sensitivity : 0.8751          
            Specificity : 0.8391          
         Pos Pred Value : 0.8643          
         Neg Pred Value : 0.8516          
             Prevalence : 0.5394          
         Detection Rate : 0.4720          
   Detection Prevalence : 0.5461          
      Balanced Accuracy : 0.8571          
                                          
       'Positive' Class : 1  
"
logres0$byClass
'
         Sensitivity          Specificity       Pos Pred Value       Neg Pred Value 
           0.8751199            0.8391158            0.8643003            0.8515986 
           Precision               Recall                   F1           Prevalence 
           0.8643003            0.8751199            0.8696764            0.5393684 
      Detection Rate Detection Prevalence    Balanced Accuracy 
           0.4720120            0.5461203            0.8571178 
'
#
rclo20170927xrut <- roc.curve(response = test$Infractor , predicted = reslogit0, plotit = TRUE)
rclo20170927xrut
'
Area under the curve (AUC): 0.857
'
jpeg(filename="1-rcdl20170927xrut.jpeg")
roc.curve(response = test$Infractor , predicted = reslogit0, plotit = TRUE)
dev.off()
summary(rclo20170927xrut)
'
Call: 
roc.curve(response = test$Infractor, predicted = reslogit0, plotit = TRUE)

Area under the curve (AUC): 
0.857 

False positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
0.00000 0.08044 0.16090 0.38700 0.58040 1.00000 

True positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
 0.0000  0.4376  0.8751  0.6250  0.9376  1.0000 
'
