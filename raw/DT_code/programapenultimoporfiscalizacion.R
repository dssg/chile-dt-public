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

# seleccion atributos 6
#
# Atributos que nos permiten predecir
#
nd.6 <- basereactivaplus
#
nd.6 <- nd.6[c("Infractor","crae", "Region", "ntrabajadoresSII", "tramoventasSII", "numfisc", "numinf", "recency")]
#
nrow(nd.6)
# [1] 644443
#
apply(nd.6, 2, pMiss)
'
       Infractor             crae           Region ntrabajadoresSII 
         0.00000          0.00000          0.00000         27.04910 
  tramoventasSII          numfisc           numinf          recency 
        22.93578          0.00000          0.00000          0.00000 
'
summary(nd.6)
'
   Infractor           crae            Region       ntrabajadoresSII
 Min.   :0.0000   111    :118948   13     :272758   Min.   :    1   
 1st Qu.:0.0000   107    :118063   5      : 65259   1st Qu.:   51   
 Median :0.0000   109    : 67585   8      : 56726   Median :  312   
 Mean   :0.4631   106    : 57705   10     : 39108   Mean   : 2074   
 3rd Qu.:1.0000   115    : 57394   7      : 33920   3rd Qu.: 1783   
 Max.   :1.0000   104    : 56694   6      : 33753   Max.   :50445   
                  (Other):168054   (Other):142919   NAs   :174316  
tramoventasSII      numfisc            numinf           recency      
13     :104298   Min.   :   0.00   Min.   :   0.00   Min.   :   0.0  
11     : 74340   1st Qu.:   1.00   1st Qu.:   0.00   1st Qu.:   0.0  
7      : 53135   Median :   4.00   Median :   2.00   Median :  11.0  
9      : 46411   Mean   :  81.22   Mean   :  34.52   Mean   : 131.8  
10     : 46206   3rd Qu.:  32.00   3rd Qu.:  14.00   3rd Qu.:  90.0  
(Other):172245   Max.   :3595.00   Max.   :1534.00   Max.   :3618.0  
NAs   :147808
'

# preparacion pre modelos 6
#
nd.6 <- nd.6[complete.cases(nd.6),]
nrow(nd.6)
# [1] 470127
ncol(nd.6)
# [1] 8
colnames(nd.6)
'
[1] "Infractor"        "crae"             "Region"          
[4] "ntrabajadoresSII" "tramoventasSII"   "numfisc"         
[7] "numinf"           "recency" 
'
apply(nd.6, 2, pMiss)
'
       Infractor             crae           Region ntrabajadoresSII 
               0                0                0                0 
  tramoventasSII          numfisc           numinf          recency 
               0                0                0                0 
'
summary(nd.6)
'
   Infractor           crae            Region       ntrabajadoresSII
 Min.   :0.0000   111    : 99484   13     :216356   Min.   :    1   
 1st Qu.:0.0000   107    : 89082   5      : 44840   1st Qu.:   51   
 Median :0.0000   109    : 49788   8      : 38437   Median :  312   
 Mean   :0.4422   104    : 43733   10     : 27813   Mean   : 2074   
 3rd Qu.:1.0000   106    : 40801   6      : 22393   3rd Qu.: 1783   
 Max.   :1.0000   115    : 40208   7      : 21517   Max.   :50445   
                  (Other):107031   (Other): 98771                   
 tramoventasSII      numfisc           numinf           recency      
 13     :104015   Min.   :   0.0   Min.   :   0.00   Min.   :   0.0  
 11     : 74048   1st Qu.:   2.0   1st Qu.:   1.00   1st Qu.:   1.0  
 7      : 49593   Median :  10.0   Median :   5.00   Median :  14.0  
 10     : 45709   Mean   : 109.9   Mean   :  46.63   Mean   : 122.2  
 9      : 45428   3rd Qu.:  63.0   3rd Qu.:  26.00   3rd Qu.:  88.0  
 8      : 44397   Max.   :3595.0   Max.   :1534.00   Max.   :3576.0  
 (Other):106937 
'
#
var1 <- as.data.frame(dummy(nd.6$Region))
var2 <- as.data.frame(dummy(nd.6$crae))
var3 <- as.data.frame(dummy(nd.6$tramoventasSII))
#
#juntar las dummies en un nuevo data frame
fd <- cbind(nd.6[c(-2,-3,-5)], var1, var2,var3)
#
z <- colnames(fd)
hd <-gsub(")", "\\.",colnames(fd)) 
colnames(fd) <- hd
#
ar <- "Infractor ~"
for (i in 2:length(fd)){
	ar = paste(ar, sprintf("`%s`",hd[i]), sep=" + ")
}
f20170927 <- as.formula( ar )
#
# para validación 75%-25%
nrow(fd)
# [1] 470127
ncol(fd)
# [1] 50
set.seed(1234)
index <- sample(1:nrow(fd), round(0.75*nrow(fd)))
train20170927.6 <- fd[index,]
nrow(train20170927.6)
# [1] 352595
ncol(train20170927.6)
# [1] 50
test20170927.6 <- fd[-index,]
nrow(test20170927.6)
# [1] 117532
ncol(test20170927.6)
# [1] 50
#
summary(train20170927.6)
'
   Infractor      ntrabajadoresSII    numfisc         numinf       
 Min.   :0.0000   Min.   :    1    Min.   :   0   Min.   :   0.00  
 1st Qu.:0.0000   1st Qu.:   51    1st Qu.:   2   1st Qu.:   1.00  
 Median :0.0000   Median :  312    Median :  10   Median :   5.00  
 Mean   :0.4425   Mean   : 2070    Mean   : 110   Mean   :  46.63  
 3rd Qu.:1.0000   3rd Qu.: 1784    3rd Qu.:  63   3rd Qu.:  26.00  
 Max.   :1.0000   Max.   :50445    Max.   :3595   Max.   :1534.00  
    recency          Region.1          Region.2          Region.3     
 Min.   :   0.0   Min.   :0.00000   Min.   :0.00000   Min.   :0.0000  
 1st Qu.:   1.0   1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.0000  
 Median :  14.0   Median :0.00000   Median :0.00000   Median :0.0000  
 Mean   : 122.2   Mean   :0.03045   Mean   :0.04164   Mean   :0.0216  
 3rd Qu.:  88.5   3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.0000  
 Max.   :3576.0   Max.   :1.00000   Max.   :1.00000   Max.   :1.0000  
    Region.4          Region.5          Region.6          Region.7      
 Min.   :0.00000   Min.   :0.00000   Min.   :0.00000   Min.   :0.00000  
 1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.00000  
 Median :0.00000   Median :0.00000   Median :0.00000   Median :0.00000  
 Mean   :0.03254   Mean   :0.09541   Mean   :0.04756   Mean   :0.04574  
 3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.00000  
 Max.   :1.00000   Max.   :1.00000   Max.   :1.00000   Max.   :1.00000  
    Region.8          Region.9         Region.10         Region.11       
 Min.   :0.00000   Min.   :0.00000   Min.   :0.00000   Min.   :0.000000  
 1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.000000  
 Median :0.00000   Median :0.00000   Median :0.00000   Median :0.000000  
 Mean   :0.08152   Mean   :0.03944   Mean   :0.05929   Mean   :0.008066  
 3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.000000  
 Max.   :1.00000   Max.   :1.00000   Max.   :1.00000   Max.   :1.000000  
   Region.12         Region.13        Region.14         Region.15       
 Min.   :0.00000   Min.   :0.0000   Min.   :0.00000   Min.   :0.000000  
 1st Qu.:0.00000   1st Qu.:0.0000   1st Qu.:0.00000   1st Qu.:0.000000  
 Median :0.00000   Median :0.0000   Median :0.00000   Median :0.000000  
 Mean   :0.01286   Mean   :0.4606   Mean   :0.01381   Mean   :0.009439  
 3rd Qu.:0.00000   3rd Qu.:1.0000   3rd Qu.:0.00000   3rd Qu.:0.000000  
 Max.   :1.00000   Max.   :1.0000   Max.   :1.00000   Max.   :1.000000  
    crae.101          crae.102           crae.103           crae.104      
 Min.   :0.00000   Min.   :0.000000   Min.   :0.000000   Min.   :0.00000  
 1st Qu.:0.00000   1st Qu.:0.000000   1st Qu.:0.000000   1st Qu.:0.00000  
 Median :0.00000   Median :0.000000   Median :0.000000   Median :0.00000  
 Mean   :0.03933   Mean   :0.008959   Mean   :0.009989   Mean   :0.09328  
 3rd Qu.:0.00000   3rd Qu.:0.000000   3rd Qu.:0.000000   3rd Qu.:0.00000  
 Max.   :1.00000   Max.   :1.000000   Max.   :1.000000   Max.   :1.00000  
    crae.105           crae.106         crae.107         crae.108      
 Min.   :0.000000   Min.   :0.0000   Min.   :0.0000   Min.   :0.00000  
 1st Qu.:0.000000   1st Qu.:0.0000   1st Qu.:0.0000   1st Qu.:0.00000  
 Median :0.000000   Median :0.0000   Median :0.0000   Median :0.00000  
 Mean   :0.003732   Mean   :0.0868   Mean   :0.1888   Mean   :0.06978  
 3rd Qu.:0.000000   3rd Qu.:0.0000   3rd Qu.:0.0000   3rd Qu.:0.00000  
 Max.   :1.000000   Max.   :1.0000   Max.   :1.0000   Max.   :1.00000  
    crae.109         crae.110          crae.111         crae.112       
 Min.   :0.0000   Min.   :0.00000   Min.   :0.0000   Min.   :0.000000  
 1st Qu.:0.0000   1st Qu.:0.00000   1st Qu.:0.0000   1st Qu.:0.000000  
 Median :0.0000   Median :0.00000   Median :0.0000   Median :0.000000  
 Mean   :0.1059   Mean   :0.02155   Mean   :0.2117   Mean   :0.004824  
 3rd Qu.:0.0000   3rd Qu.:0.00000   3rd Qu.:0.0000   3rd Qu.:0.000000  
 Max.   :1.0000   Max.   :1.00000   Max.   :1.0000   Max.   :1.000000  
    crae.113          crae.114          crae.115          crae.116       
 Min.   :0.00000   Min.   :0.00000   Min.   :0.00000   Min.   :0.000000  
 1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.000000  
 Median :0.00000   Median :0.00000   Median :0.00000   Median :0.000000  
 Mean   :0.04618   Mean   :0.01767   Mean   :0.08588   Mean   :0.005496  
 3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.000000  
 Max.   :1.00000   Max.   :1.00000   Max.   :1.00000   Max.   :1.000000  
    crae.117         tramoventasSII.1  tramoventasSII.10 tramoventasSII.11
 Min.   :0.0000000   Min.   :0.00000   Min.   :0.00000   Min.   :0.0000   
 1st Qu.:0.0000000   1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.0000   
 Median :0.0000000   Median :0.00000   Median :0.00000   Median :0.0000   
 Mean   :0.0001588   Mean   :0.02562   Mean   :0.09739   Mean   :0.1579   
 3rd Qu.:0.0000000   3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.0000   
 Max.   :1.0000000   Max.   :1.00000   Max.   :1.00000   Max.   :1.0000   
 tramoventasSII.12 tramoventasSII.13 tramoventasSII.2   tramoventasSII.3  
 Min.   :0.00000   Min.   :0.0000    Min.   :0.000000   Min.   :0.000000  
 1st Qu.:0.00000   1st Qu.:0.0000    1st Qu.:0.000000   1st Qu.:0.000000  
 Median :0.00000   Median :0.0000    Median :0.000000   Median :0.000000  
 Mean   :0.06403   Mean   :0.2211    Mean   :0.004149   Mean   :0.007825  
 3rd Qu.:0.00000   3rd Qu.:0.0000    3rd Qu.:0.000000   3rd Qu.:0.000000  
 Max.   :1.00000   Max.   :1.0000    Max.   :1.000000   Max.   :1.000000  
 tramoventasSII.4  tramoventasSII.5  tramoventasSII.6  tramoventasSII.7
 Min.   :0.00000   Min.   :0.00000   Min.   :0.00000   Min.   :0.0000  
 1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.0000  
 Median :0.00000   Median :0.00000   Median :0.00000   Median :0.0000  
 Mean   :0.03096   Mean   :0.03865   Mean   :0.05638   Mean   :0.1054  
 3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.0000  
 Max.   :1.00000   Max.   :1.00000   Max.   :1.00000   Max.   :1.0000  
 tramoventasSII.8  tramoventasSII.9 
 Min.   :0.00000   Min.   :0.00000  
 1st Qu.:0.00000   1st Qu.:0.00000  
 Median :0.00000   Median :0.00000  
 Mean   :0.09429   Mean   :0.09626  
 3rd Qu.:0.00000   3rd Qu.:0.00000  
 Max.   :1.00000   Max.   :1.00000  
'
summary(test20170927.6)
'
   Infractor      ntrabajadoresSII    numfisc           numinf       
 Min.   :0.0000   Min.   :    1    Min.   :   0.0   Min.   :   0.00  
 1st Qu.:0.0000   1st Qu.:   50    1st Qu.:   2.0   1st Qu.:   1.00  
 Median :0.0000   Median :  310    Median :  10.0   Median :   5.00  
 Mean   :0.4412   Mean   : 2087    Mean   : 109.9   Mean   :  46.61  
 3rd Qu.:1.0000   3rd Qu.: 1780    3rd Qu.:  63.0   3rd Qu.:  26.00  
 Max.   :1.0000   Max.   :50445    Max.   :3591.0   Max.   :1533.00  
    recency          Region.1          Region.2          Region.3      
 Min.   :   0.0   Min.   :0.00000   Min.   :0.00000   Min.   :0.00000  
 1st Qu.:   1.0   1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.00000  
 Median :  14.0   Median :0.00000   Median :0.00000   Median :0.00000  
 Mean   : 122.2   Mean   :0.03051   Mean   :0.04105   Mean   :0.02199  
 3rd Qu.:  88.0   3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.00000  
 Max.   :3562.0   Max.   :1.00000   Max.   :1.00000   Max.   :1.00000  
    Region.4          Region.5          Region.6          Region.7      
 Min.   :0.00000   Min.   :0.00000   Min.   :0.00000   Min.   :0.00000  
 1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.00000  
 Median :0.00000   Median :0.00000   Median :0.00000   Median :0.00000  
 Mean   :0.03264   Mean   :0.09529   Mean   :0.04785   Mean   :0.04585  
 3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.00000  
 Max.   :1.00000   Max.   :1.00000   Max.   :1.00000   Max.   :1.00000  
    Region.8          Region.9         Region.10         Region.11       
 Min.   :0.00000   Min.   :0.00000   Min.   :0.00000   Min.   :0.000000  
 1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.000000  
 Median :0.00000   Median :0.00000   Median :0.00000   Median :0.000000  
 Mean   :0.08249   Mean   :0.04044   Mean   :0.05878   Mean   :0.008108  
 3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.000000  
 Max.   :1.00000   Max.   :1.00000   Max.   :1.00000   Max.   :1.000000  
   Region.12         Region.13        Region.14         Region.15       
 Min.   :0.00000   Min.   :0.0000   Min.   :0.00000   Min.   :0.000000  
 1st Qu.:0.00000   1st Qu.:0.0000   1st Qu.:0.00000   1st Qu.:0.000000  
 Median :0.00000   Median :0.0000   Median :0.00000   Median :0.000000  
 Mean   :0.01257   Mean   :0.4589   Mean   :0.01407   Mean   :0.009461  
 3rd Qu.:0.00000   3rd Qu.:1.0000   3rd Qu.:0.00000   3rd Qu.:0.000000  
 Max.   :1.00000   Max.   :1.0000   Max.   :1.00000   Max.   :1.000000  
    crae.101          crae.102         crae.103           crae.104      
 Min.   :0.00000   Min.   :0.0000   Min.   :0.000000   Min.   :0.00000  
 1st Qu.:0.00000   1st Qu.:0.0000   1st Qu.:0.000000   1st Qu.:0.00000  
 Median :0.00000   Median :0.0000   Median :0.000000   Median :0.00000  
 Mean   :0.04069   Mean   :0.0089   Mean   :0.009912   Mean   :0.09226  
 3rd Qu.:0.00000   3rd Qu.:0.0000   3rd Qu.:0.000000   3rd Qu.:0.00000  
 Max.   :1.00000   Max.   :1.0000   Max.   :1.000000   Max.   :1.00000  
    crae.105           crae.106          crae.107         crae.108      
 Min.   :0.000000   Min.   :0.00000   Min.   :0.0000   Min.   :0.00000  
 1st Qu.:0.000000   1st Qu.:0.00000   1st Qu.:0.0000   1st Qu.:0.00000  
 Median :0.000000   Median :0.00000   Median :0.0000   Median :0.00000  
 Mean   :0.003956   Mean   :0.08675   Mean   :0.1916   Mean   :0.06847  
 3rd Qu.:0.000000   3rd Qu.:0.00000   3rd Qu.:0.0000   3rd Qu.:0.00000  
 Max.   :1.000000   Max.   :1.00000   Max.   :1.0000   Max.   :1.00000  
    crae.109         crae.110          crae.111         crae.112       
 Min.   :0.0000   Min.   :0.00000   Min.   :0.0000   Min.   :0.000000  
 1st Qu.:0.0000   1st Qu.:0.00000   1st Qu.:0.0000   1st Qu.:0.000000  
 Median :0.0000   Median :0.00000   Median :0.0000   Median :0.000000  
 Mean   :0.1059   Mean   :0.02196   Mean   :0.2113   Mean   :0.005233  
 3rd Qu.:0.0000   3rd Qu.:0.00000   3rd Qu.:0.0000   3rd Qu.:0.000000  
 Max.   :1.0000   Max.   :1.00000   Max.   :1.0000   Max.   :1.000000  
    crae.113          crae.114          crae.115          crae.116       
 Min.   :0.00000   Min.   :0.00000   Min.   :0.00000   Min.   :0.000000  
 1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.000000  
 Median :0.00000   Median :0.00000   Median :0.00000   Median :0.000000  
 Mean   :0.04582   Mean   :0.01708   Mean   :0.08446   Mean   :0.005496  
 3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.000000  
 Max.   :1.00000   Max.   :1.00000   Max.   :1.00000   Max.   :1.000000  
    crae.117         tramoventasSII.1  tramoventasSII.10 tramoventasSII.11
 Min.   :0.0000000   Min.   :0.00000   Min.   :0.00000   Min.   :0.0000   
 1st Qu.:0.0000000   1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.0000   
 Median :0.0000000   Median :0.00000   Median :0.00000   Median :0.0000   
 Mean   :0.0001276   Mean   :0.02509   Mean   :0.09673   Mean   :0.1563   
 3rd Qu.:0.0000000   3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.0000   
 Max.   :1.0000000   Max.   :1.00000   Max.   :1.00000   Max.   :1.0000   
 tramoventasSII.12 tramoventasSII.13 tramoventasSII.2   tramoventasSII.3  
 Min.   :0.00000   Min.   :0.0000    Min.   :0.000000   Min.   :0.000000  
 1st Qu.:0.00000   1st Qu.:0.0000    1st Qu.:0.000000   1st Qu.:0.000000  
 Median :0.00000   Median :0.0000    Median :0.000000   Median :0.000000  
 Mean   :0.06302   Mean   :0.2217    Mean   :0.004314   Mean   :0.008023  
 3rd Qu.:0.00000   3rd Qu.:0.0000    3rd Qu.:0.000000   3rd Qu.:0.000000  
 Max.   :1.00000   Max.   :1.0000    Max.   :1.000000   Max.   :1.000000  
 tramoventasSII.4  tramoventasSII.5  tramoventasSII.6  tramoventasSII.7
 Min.   :0.00000   Min.   :0.00000   Min.   :0.00000   Min.   :0.0000  
 1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.00000   1st Qu.:0.0000  
 Median :0.00000   Median :0.00000   Median :0.00000   Median :0.0000  
 Mean   :0.03005   Mean   :0.03826   Mean   :0.05826   Mean   :0.1057  
 3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.00000   3rd Qu.:0.0000  
 Max.   :1.00000   Max.   :1.00000   Max.   :1.00000   Max.   :1.0000  
 tramoventasSII.8  tramoventasSII.9 
 Min.   :0.00000   Min.   :0.00000  
 1st Qu.:0.00000   1st Qu.:0.00000  
 Median :0.00000   Median :0.00000  
 Mean   :0.09487   Mean   :0.09774  
 3rd Qu.:0.00000   3rd Qu.:0.00000  
 Max.   :1.00000   Max.   :1.00000 
'

# ejecucion modelos 6
#
train <- train20170927.6
test <- test20170927.6
seqcut <- seq(0, 1 , by=0.05)
#
# arbol ctree
#
dt.1.20170927.6 <- ctree(Infractor ~. , data=train)
#
dframect <- data.frame(Threshold = double(), Recall = double(),Precision = double(), Fscore = double(), Accuracy = double() )
for(i in 1:20){
	resdt.1 <- predict(dt.1.20170927.6 , test[,-1] , type= "response")
	resdt.1 <- ifelse(resdt.1 > seqcut[i] , 1,0)
	predct <- caret::confusionMatrix(data = resdt.1, reference = test$Infractor, positive="1")
	dframect[i,1] <- seqcut[i]
	dframect[i,2] <- predct$byClass[1]
	dframect[i,3] <- predct$byClass[3]
	dframect[i,4] <- predct$byClass[7]
	dframect[i,5] <- predct$overall[1]
}
dfct20170927.6 <- melt(dframect, id=c("Threshold"))
graficodfct20170927.6<-ggplot( data = dfct20170927.6 , aes(x=Threshold , y = value, group= variable, colour = variable )  )+ geom_line() + ggtitle ("Cálculo del Threshold para ctree") + theme (plot.title = element_text(face="bold"))
jpeg(filename="1-graficodfct20170927.6.jpeg")
graficodfct20170927.6
dev.off()
#
# se selecciona 0.4583 como punto
resdt.1 <- predict(dt.1.20170927.6 , test[,-1], type="response")
resdt.1 <- ifelse(resdt.1 > 0.4583,1,0)
cmdt.1.20170927.6 <- caret::confusionMatrix(data=resdt.1 , reference = test$Infractor , positive = "1")
cmdt.1.20170927.6
"
Confusion Matrix and Statistics

          Reference
Prediction     0     1
         0 45327 28735
         1 20354 23116
                                          
               Accuracy : 0.5823          
                 95% CI : (0.5795, 0.5852)
    No Information Rate : 0.5588          
    P-Value [Acc > NIR] : < 2.2e-16       
                                          
                  Kappa : 0.1383          
 Mcnemar's Test P-Value : < 2.2e-16       
                                          
            Sensitivity : 0.4458          
            Specificity : 0.6901          
         Pos Pred Value : 0.5318          
         Neg Pred Value : 0.6120          
             Prevalence : 0.4412          
         Detection Rate : 0.1967          
   Detection Prevalence : 0.3699          
      Balanced Accuracy : 0.5680          
                                          
       'Positive' Class : 1  
"
#
cmdt.1.20170927.6$byClass
'
         Sensitivity          Specificity       Pos Pred Value 
           0.4458159            0.6901083            0.5317690 
      Neg Pred Value            Precision               Recall 
           0.6120143            0.5317690            0.4458159 
                  F1           Prevalence       Detection Rate 
           0.4850138            0.4411650            0.1966784 
Detection Prevalence    Balanced Accuracy 
           0.3698567            0.5679621 
'
#
rcdt.1.20170927.6 <- roc.curve(response = test$Infractor , predicted = resdt.1, plotit = TRUE)
rcdt.1.20170927.6
'
Area under the curve (AUC): 0.568
'
jpeg(filename="1-rcdt.1.20170927.6.jpeg")
roc.curve(response = test$Infractor , predicted = resdt.1, plotit = TRUE)
dev.off()
summary(rcdt.1.20170927.6)
'
Call: 
roc.curve(response = test$Infractor, predicted = resdt.1, plotit = TRUE)

Area under the curve (AUC): 
0.568 

False positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
 0.0000  0.1549  0.3099  0.4366  0.6549  1.0000 

True positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
 0.0000  0.2229  0.4458  0.4819  0.7229  1.0000 
'
#summary(dt.1.20170927.6)
#printcp(dt.1.20170927.6)
#plot(dt.1.20170927.6)
#

#
# arbol Random Forest
#
tuneRF20170927.6 <- tuneRF(x=train[,c(-1)] , y=as.factor(train$Infractor),stepFactor=1)
'
mtry = 7  OOB error = 41.01% 
Searching left ...
Searching right ...
'
tuneRF20170927.6 <- tuneRF(x=train[,c(-1)] , y=as.factor(train$Infractor),stepFactor=1.5)
'
mtry = 7  OOB error = 41.05% 
Searching left ...
mtry = 5 	OOB error = 41.11% 
-0.001312635 0.05 
Searching right ...
mtry = 10 	OOB error = 41.41% 
-0.008684118 0.05 
'
tuneRF20170927.6 <- tuneRF(x=train[,c(-1)] , y=as.factor(train$Infractor),stepFactor=2)
'
mtry = 7  OOB error = 41% 
Searching left ...
mtry = 4 	OOB error = 41.1% 
-0.002275604 0.05 
Searching right ...
mtry = 14 	OOB error = 42.13% 
-0.02739717 0.05 
'
#
rf.1.20170927.6 <- randomForest(x=train[,c(-1)] , y=as.factor(train$Infractor), importance = TRUE , ntree= 100 ,  mtry = 7 , replace= TRUE)
#
dframe <- data.frame(Threshold = double(), Recall = double(),Precision = double(), Fscore = double(), Accuracy = double() )
for(i in 1:20){
	resrf.1 <- predict(rf.1.20170927.6 , test[,c(-1)] , type= "prob")
	resrf.1 <- ifelse(resrf.1[,2]> seqcut[i] , 1,0)
	predrf <- caret::confusionMatrix(data = resrf.1, reference = test$Infractor, positive="1")
	dframe[i,1] <- seqcut[i]
	dframe[i,2] <- predrf$byClass[1]
	dframe[i,3] <- predrf$byClass[3]
	dframe[i,4] <- predrf$byClass[7]
	dframe[i,5] <- predrf$overall[1]
}
dfrf20170927.6 <- melt(dframe, id=c("Threshold"))
graficodfrf20170927.6<-ggplot( data = dfrf20170927.6 , aes(x=Threshold , y = value, group= variable, colour = variable )  )+ geom_line() + ggtitle ("Cálculo del Threshold para Random Forest") + theme (plot.title = element_text(face="bold"))
jpeg(filename="1-graficodfrf20170927.6.jpeg")
graficodfrf20170927.6
dev.off()
#
# se selecciona 0.33 como punto
resrf.1 <- predict(rf.1.20170927.6 , test[,c(-1)] , type= "prob")
resrf.1 <- ifelse(resrf.1[,2]> 0.33 , 1,0)
cmrf.1.20170927.6 <- caret::confusionMatrix(data = resrf.1, reference = test$Infractor, positive="1")
cmrf.1.20170927.6
"
Confusion Matrix and Statistics

          Reference
Prediction     0     1
         0 42022 25035
         1 23659 26816
                                          
               Accuracy : 0.5857          
                 95% CI : (0.5829, 0.5885)
    No Information Rate : 0.5588          
    P-Value [Acc > NIR] : < 2.2e-16       
                                          
                  Kappa : 0.1574          
 Mcnemar's Test P-Value : 4.632e-10       
                                          
            Sensitivity : 0.5172          
            Specificity : 0.6398          
         Pos Pred Value : 0.5313          
         Neg Pred Value : 0.6267          
             Prevalence : 0.4412          
         Detection Rate : 0.2282          
   Detection Prevalence : 0.4295          
      Balanced Accuracy : 0.5785          
                                          
       'Positive' Class : 1  
"
cmrf.1.20170927.6$byClass
'
         Sensitivity          Specificity       Pos Pred Value 
           0.5171742            0.6397893            0.5312729 
      Neg Pred Value            Precision               Recall 
           0.6266609            0.5312729            0.5171742 
                  F1           Prevalence       Detection Rate 
           0.5241288            0.4411650            0.2281591 
Detection Prevalence    Balanced Accuracy 
           0.4294575            0.5784817 
'
rcrf.1.20170927.6 <- roc.curve(response = test$Infractor , predicted = resrf.1, plotit = TRUE)
rcrf.1.20170927.6
'
Area under the curve (AUC): 0.578
'
jpeg(filename="1-rcrf.1.20170927.6.jpeg")
roc.curve(response = test$Infractor , predicted = resrf.1, plotit = TRUE)
dev.off()
summary(rcrf.1.20170927.6)
'
Call: 
roc.curve(response = test$Infractor, predicted = resrf.1, plotit = TRUE)

Area under the curve (AUC): 
0.578 

False positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
 0.0000  0.1801  0.3602  0.4534  0.6801  1.0000 

True positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
 0.0000  0.2586  0.5172  0.5057  0.7586  1.0000 
'
summary(rf.1.20170927.6)
'
                Length Class  Mode     
call                 7 -none- call     
type                 1 -none- character
predicted       352595 factor numeric  
err.rate           300 -none- numeric  
confusion            6 -none- numeric  
votes           705190 matrix numeric  
oob.times       352595 -none- numeric  
classes              2 -none- character
importance         196 -none- numeric  
importanceSD       147 -none- numeric  
localImportance      0 -none- NULL     
proximity            0 -none- NULL     
ntree                1 -none- numeric  
mtry                 1 -none- numeric  
forest              14 -none- list     
y               352595 factor numeric  
test                 0 -none- NULL     
inbag                0 -none- NULL  
'
varImpPlot(rf.1.20170927.6)
jpeg(filename="1-rcrf.1.20170927.6-varimpplot.jpeg")
varImpPlot(rf.1.20170927.6)
dev.off()
#
# cubist (Rule- And Instance-Based Regression Modeling)
#
cu.1.20170927.6 <- cubist(x=train[,c(-1)] , y=train$Infractor)
#
dframecu <- data.frame(Threshold = double(), Recall = double(),Precision = double(), Fscore = double(), Accuracy = double() )
for(i in 1:20){
	rescu.1 <- predict(cu.1.20170927.6 , test[,c(-1)] , type= "response")
	rescu.1 <- ifelse(rescu.1> seqcut[i] , 1,0)
	predcu.1 <- caret::confusionMatrix(data = rescu.1, reference = test$Infractor, positive="1")
	dframecu[i,1] <- seqcut[i]
	dframecu[i,2] <- predcu.1$byClass[1]
	dframecu[i,3] <- predcu.1$byClass[3]
	dframecu[i,4] <- predcu.1$byClass[7]
	dframecu[i,5] <- predcu.1$overall[1]
}
dfcu20170927.6.1 <- melt(dframecu, id=c("Threshold"))
graficodfcu20170927.6.1<-ggplot( data = dfcu20170927.6.1 , aes(x=Threshold , y = value, group= variable, colour = variable )  )+ geom_line() + ggtitle ("Cálculo del Threshold para cubist") + theme (plot.title = element_text(face="bold"))
jpeg(filename="1-graficodfcu20170927.6.1.jpeg")
graficodfcu20170927.6.1
dev.off()
#
rescu.1 <- predict(cu.1.20170927.6 , test[,-1], type="response")
rescu.1 <- ifelse(rescu.1 > 0.05,1,0)
cmcu.1.20170927.6 <- caret::confusionMatrix(data = rescu.1 , reference = test$Infractor, positive="1")
cmcu.1.20170927.6
"
Confusion Matrix and Statistics

          Reference
Prediction     0     1
         0 41092 24950
         1 24589 26901
                                          
               Accuracy : 0.5785          
                 95% CI : (0.5757, 0.5813)
    No Information Rate : 0.5588          
    P-Value [Acc > NIR] : <2e-16          
                                          
                  Kappa : 0.1445          
 Mcnemar's Test P-Value : 0.1058          
                                          
            Sensitivity : 0.5188          
            Specificity : 0.6256          
         Pos Pred Value : 0.5225          
         Neg Pred Value : 0.6222          
             Prevalence : 0.4412          
         Detection Rate : 0.2289          
   Detection Prevalence : 0.4381          
      Balanced Accuracy : 0.5722          
                                          
       'Positive' Class : 1 
"
cmcu.1.20170927.6$byClass
'
         Sensitivity          Specificity       Pos Pred Value 
           0.5188135            0.6256299            0.5224510 
      Neg Pred Value            Precision               Recall 
           0.6222101            0.5224510            0.5188135 
                  F1           Prevalence       Detection Rate 
           0.5206259            0.4411650            0.2288823 
Detection Prevalence    Balanced Accuracy 
           0.4380935            0.5722217 
'
scores<-cmcu.1.20170927.6
print(c("Recall", scores$byClass[1]))
'
                            Sensitivity 
           "Recall" "0.518813523365027" 
'
print(c("Precision", scores$byClass[3]))
'
                         Pos Pred Value 
        "Precision" "0.522450961351719" 
'
print(c("Fscore", scores$byClass[7]))
'
                                     F1 
           "Fscore" "0.520625889046942"
'
print(c("Accuracy", scores$overall[1]))
'
                         Accuracy 
      "Accuracy" "0.578506279141" 
'
rccu.1.20170927.6 <- roc.curve(response = test$Infractor , predicted = rescu.1, plotit = TRUE)
rccu.1.20170927.6
'
Area under the curve (AUC): 0.572
'
jpeg(filename="1-rccu.1.20170927.6.jpeg")
roc.curve(response = test$Infractor , predicted = rescu.1, plotit = TRUE)
dev.off()
summary(rccu.1.20170927.6)
'
Call: 
roc.curve(response = test$Infractor, predicted = rescu.1, plotit = TRUE)

Area under the curve (AUC): 
0.572 

False positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
 0.0000  0.1872  0.3744  0.4581  0.6872  1.0000 

True positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
 0.0000  0.2594  0.5188  0.5063  0.7594  1.0000 
'
summary(cu.1.20170927.6)
'
Evaluation on training data (352595 cases, sampled):

    Average  |error|                0.5
    Relative |error|               0.95
    Correlation coefficient        0.13


	Attribute usage:
	  Conds  Model

	   81%           Region.13
	   55%    89%    numinf
	   51%    60%    ntrabajadoresSII
	   26%    26%    crae.101
	   25%    92%    numfisc
	   23%           Region.7
	   22%           Region.9
	   21%    25%    recency
	   15%           Region.11
	   14%     3%    Region.14
	   13%           Region.10
	   12%     5%    crae.108
	   10%    26%    tramoventasSII.1
	   10%     5%    Region.12
	    9%     3%    Region.6
	    7%    36%    crae.113
	    6%    35%    crae.115
	    5%    32%    tramoventasSII.11
	    5%     4%    Region.1
	    5%    16%    crae.112
	    5%    11%    crae.104
	    4%    13%    crae.107
	    3%     5%    Region.5
	    3%     4%    crae.103
	    3%    32%    crae.111
	    2%     2%    Region.2
	    1%     3%    Region.8
	    1%    27%    tramoventasSII.13
	           2%    Region.4
	          16%    crae.109
	          47%    tramoventasSII.7
	          36%    crae.106
	          27%    crae.114
	          22%    tramoventasSII.6
	          22%    tramoventasSII.8
	          19%    tramoventasSII.10
	          18%    crae.110
	          15%    tramoventasSII.4
	          14%    tramoventasSII.5
	          13%    tramoventasSII.9
	          12%    tramoventasSII.12
	           8%    crae.116
	           5%    Region.3
	           4%    tramoventasSII.3
	           3%    crae.105


Time: 76.6 secs
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
-1.5782  -1.0690  -0.9114   1.2299   1.8687  

Coefficients: (3 not defined because of singularities)
                    Estimate Std. Error z value Pr(>|z|)    
(Intercept)       -4.658e-01  2.837e-01  -1.642 0.100676    
ntrabajadoresSII  -9.082e-09  1.193e-06  -0.008 0.993927    
numfisc           -1.691e-03  8.002e-05 -21.135  < 2e-16 ***
numinf             3.845e-03  1.753e-04  21.931  < 2e-16 ***
recency            2.718e-06  1.177e-05   0.231 0.817302    
Region.1          -1.087e-01  3.995e-02  -2.721 0.006504 ** 
Region.2          -1.043e-01  3.870e-02  -2.696 0.007019 ** 
Region.3           5.585e-02  4.186e-02   1.334 0.182057    
Region.4          -1.104e-01  3.964e-02  -2.784 0.005363 ** 
Region.5           2.565e-02  3.655e-02   0.702 0.482791    
Region.6           6.828e-02  3.821e-02   1.787 0.073985 .  
Region.7           2.606e-01  3.837e-02   6.793 1.10e-11 ***
Region.8          -7.448e-02  3.683e-02  -2.022 0.043130 *  
Region.9           3.762e-01  3.892e-02   9.666  < 2e-16 ***
Region.10          2.075e-01  3.764e-02   5.512 3.54e-08 ***
Region.11         -3.221e-01  5.237e-02  -6.150 7.73e-10 ***
Region.12          1.219e-01  4.590e-02   2.655 0.007930 ** 
Region.13         -4.134e-01  3.527e-02 -11.721  < 2e-16 ***
Region.14          1.639e-01  4.526e-02   3.621 0.000293 ***
Region.15                 NA         NA      NA       NA    
crae.101           4.799e-01  2.821e-01   1.701 0.088924 .  
crae.102           2.144e-01  2.841e-01   0.755 0.450437    
crae.103           5.718e-01  2.836e-01   2.016 0.043792 *  
crae.104           4.879e-01  2.818e-01   1.731 0.083382 .  
crae.105           5.345e-01  2.870e-01   1.862 0.062580 .  
crae.106           3.854e-01  2.818e-01   1.368 0.171395    
crae.107           4.482e-01  2.817e-01   1.591 0.111570    
crae.108           4.922e-01  2.818e-01   1.747 0.080723 .  
crae.109           4.348e-01  2.817e-01   1.543 0.122777    
crae.110           3.689e-01  2.826e-01   1.306 0.191649    
crae.111           3.305e-01  2.816e-01   1.173 0.240668    
crae.112          -2.912e-01  2.865e-01  -1.016 0.309464    
crae.113           2.515e-01  2.820e-01   0.892 0.372408    
crae.114           3.513e-01  2.827e-01   1.243 0.213956    
crae.115           2.520e-01  2.818e-01   0.894 0.371066    
crae.116           6.386e-01  2.855e-01   2.237 0.025292 *  
crae.117                  NA         NA      NA       NA    
tramoventasSII.1   1.459e-01  2.618e-02   5.572 2.52e-08 ***
tramoventasSII.10 -4.527e-02  1.561e-02  -2.900 0.003733 ** 
tramoventasSII.11 -1.053e-01  1.414e-02  -7.446 9.62e-14 ***
tramoventasSII.12 -8.675e-02  1.779e-02  -4.876 1.08e-06 ***
tramoventasSII.13 -1.424e-01  1.464e-02  -9.724  < 2e-16 ***
tramoventasSII.2   1.510e-01  5.430e-02   2.781 0.005416 ** 
tramoventasSII.3   1.561e-01  4.024e-02   3.880 0.000104 ***
tramoventasSII.4   2.317e-01  2.240e-02  10.346  < 2e-16 ***
tramoventasSII.5   2.395e-01  2.061e-02  11.623  < 2e-16 ***
tramoventasSII.6   2.400e-01  1.814e-02  13.229  < 2e-16 ***
tramoventasSII.7   1.721e-01  1.526e-02  11.283  < 2e-16 ***
tramoventasSII.8   8.750e-02  1.567e-02   5.584 2.35e-08 ***
tramoventasSII.9          NA         NA      NA       NA    
---
Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1

(Dispersion parameter for binomial family taken to be 1)

    Null deviance: 484131  on 352594  degrees of freedom
Residual deviance: 474565  on 352548  degrees of freedom
AIC: 474659

Number of Fisher Scoring iterations: 4
'

dframelog <- data.frame(Threshold = double(), Recall = double(),Precision = double(), Fscore = double(), Accuracy = double() )
for(i in 1:20){
	lg.res <- predict(logit0 , test[,-1], type="response" )
	lg.res <- ifelse(lg.res>= seqcut[i] , 1,0)
	predlg <- caret::confusionMatrix(lg.res, test$Infractor, positive="1")
	dframelog[i,1] <- seqcut[i]
	dframelog[i,2] <- predlg$byClass[1]
	dframelog[i,3] <- predlg$byClass[3]
	dframelog[i,4] <- predlg$byClass[7]
	dframelog[i,5] <- predlg$overall[1]
}

dflg20170927.6 <- melt(dframelog, id=c("Threshold"))

graficodflg20170927.6<-ggplot( data = dflg20170927.6 , aes(x=Threshold , y = value, group= variable, colour = variable )  )+ geom_line() + ggtitle ("Cálculo del Threshold para Regresión Logística") + theme (plot.title = element_text(face="bold"))
jpeg(filename="1-graficodflg20170927.6.jpeg")
graficodflg20170927.6
dev.off()

ct <- 0.45
reslogit0 <- predict(logit0,test[,-1] , type = 'response')
reslogit0 <- ifelse(reslogit0 > ct,1,0)
logres0 <- caret::confusionMatrix(reslogit0 , test$Infractor , positive = "1")
logres0
"
Confusion Matrix and Statistics

          Reference
Prediction     0     1
         0 40415 24690
         1 25266 27161
                                          
               Accuracy : 0.575           
                 95% CI : (0.5721, 0.5778)
    No Information Rate : 0.5588          
    P-Value [Acc > NIR] : < 2e-16         
                                          
                  Kappa : 0.139           
 Mcnemar's Test P-Value : 0.01009         
                                          
            Sensitivity : 0.5238          
            Specificity : 0.6153          
         Pos Pred Value : 0.5181          
         Neg Pred Value : 0.6208          
             Prevalence : 0.4412          
         Detection Rate : 0.2311          
   Detection Prevalence : 0.4461          
      Balanced Accuracy : 0.5696          
                                          
       'Positive' Class : 1 
"
logres0$byClass
'
         Sensitivity          Specificity       Pos Pred Value 
           0.5238279            0.6153225            0.5180727 
      Neg Pred Value            Precision               Recall 
           0.6207665            0.5180727            0.5238279 
                  F1           Prevalence       Detection Rate 
           0.5209344            0.4411650            0.2310945 
Detection Prevalence    Balanced Accuracy 
           0.4460658            0.5695752
'
scores<-logres0
print(c("Recall", scores$byClass[1]))
'
                            Sensitivity 
           "Recall" "0.523827891458217" 
'
print(c("Precision", scores$byClass[3]))
'
                         Pos Pred Value 
        "Precision" "0.518072748774486" 
'
print(c("Fscore", scores$byClass[7]))
'
                                     F1 
           "Fscore" "0.520934425286254" 
'
print(c("Accuracy", scores$overall[1]))
'
                               Accuracy 
         "Accuracy" "0.574958309226423" 
'
#
rclo20170927.6 <- roc.curve(response = test$Infractor , predicted = reslogit0, plotit = TRUE)
rclo20170927.6
'
Area under the curve (AUC): 0.570
'
jpeg(filename="1-rcdl20170927.6.jpeg")
roc.curve(response = test$Infractor , predicted = reslogit0, plotit = TRUE)
dev.off()
summary(rclo20170927.6)
'
Call: 
roc.curve(response = test$Infractor, predicted = reslogit0, plotit = TRUE)

Area under the curve (AUC): 
0.57 

False positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
 0.0000  0.1923  0.3847  0.4616  0.6923  1.0000 

True positive rate for evaluated thresholds: 
   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
 0.0000  0.2619  0.5238  0.5079  0.7619  1.0000 
'
