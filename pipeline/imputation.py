"""
Functions for generating features
"""
import pandas as pd

from fancyimpute import BayesianRidgeRegression, BiScaler, IterativeSVD, KNN, MatrixFactorization, MICE, NuclearNormMinimization, SimilarityWeightedAveraging, SimpleFill, SoftImpute

"""
Simple function that performs simple imputation methods on an entire dataframe
"""
def simple_imputation(df, method='mean'):
	"""
	Input:
	df -- dataframe of features
	features -- list of features
	method -- method for imputation
		- 'mean': computes the mean
		- 'mode' : computes the mode
		- 'max': computes the max value
		- 'min': computes the minimum value
		- 'median': computes the median value
		- 'zeros': imputes with all zeros
		- 'ones' : imputes with all ones
	Output:
	imputed dataframe
	"""
	if method == 'mean':
		return df.transform(lambda x: x.fillna(x.mean()))
	elif method == 'mode':
		return df.transform(lambda x: x.fillna(x.mode()[0]))
	elif method == 'max':
		return df.transform(lambda x: x.fillna(x.max()))
	elif method == 'min':
		return df.transform(lambda x: x.fillna(x.min()))
	elif method == 'median': 
		return df.transform(lambda x: x.fillna(x.median()))
	elif method == 'zeros':
		return df.fillna(0)
	elif method == 'ones':
		return df.fillna(1)


"""
Simple function that performs simple imputation methods on select features
"""
def simple_feature_imputation(df, features, method='mean'):
	"""
	Input:
	df -- dataframe of features
	features -- list of features
	method -- method for imputation
		- 'mean': computes the mean
		- 'mode' : computes the mode
		- 'max': computes the max value
		- 'min': computes the minimum value
		- 'median': computes the median value
		- 'zeros': imputes with all zeros
		- 'ones' : imputes with all ones
	Output:
	imputed dataframe of features in the list
	"""
	if method == 'mean':
		return df[features].transform(lambda x: x.fillna(x.mean()))
	elif method == 'mode':
		return df[features].transform(lambda x: x.fillna(x.mode()[0]))
	elif method == 'max':
		return df[features].transform(lambda x: x.fillna(x.max()))
	elif method == 'min':
		return df[features].transform(lambda x: x.fillna(x.min()))
	elif method == 'median': 
		return df[features].transform(lambda x: x.fillna(x.median()))
	elif method == 'zeros':
		return df[features].fillna(0)
	elif method == 'ones':
		return df[features].fillna(1)
"""
Function that fills value of a dataframe from a matrix
"""

def fill_values(df, array):
	"""
	Inputs:
	df -- dataframe that will be filled in
	array -- numpy array of values to be used for filling

	Output:
	filled dataframe with values
	"""
	# Save indices of missing values in dataframe
	indices_of_missing = df[df.isnull()].index
	# Fill in values using the array
	for fill_index, dataframe_index in enumerate(indices_of_missing):
		df.loc[dataframe_index, df.columns] = array[fill_index]
	return df

"""
Function that performs complx imputation on entire dataframe
"""

def complex_imputation(df, method='mice', neighbors = 3):
	"""
	Inputs:
	df -- dataframe of incomplete data
	method -- method of imputation
		- 'knn': Imputes using K Nearest Neighbors of completed rows
		- 'soft_impute': Imputes using iterative soft thresholding of SVD decompositions
		- 'mice': Imputes using Multiple Imputation by Chained Equations method
		- 'nuclear_nm': Imputation using Exact Matrix Completion via Convex Optimization method
		- 'matrix_factorization': Imputes by factorization of matrix in low-rank U and V
								  with L1 sparsity on U elements and L2 sparsity on V elements
		- 'iterative_svd': Imputes based on iterative low-rank SVD decomposition
	neighbors -- parameter for KNN imputation
	
	Output:
	Completed matrix
	"""
	# Create matrix of features
	X_incomplete = df.values
	# Normalize matrix by std and mean (0 mean, 1 variance)
	X_incomplete_normalized = BiScaler().fit_transform(X_incomplete)
	
	if method == 'knn':
		X_complete =  KNN(neighbors).complete(X_incomplete)
		return fill_values(df, X_complete)

	if method == 'soft_impute':	
		X_complete_normalized = SoftImpute().complete(X_incomplete_normalized)
		X_complete=BiScaler().inverse_transform(X_complete_normalized)
		return fill_values(df, X_complete)
	
	if method == 'mice':
		X_complete = MICE().complete(X_incomplete)
		return fill_values(df, X_complete)

	if method == 'nuclear_nm':
		X_complete = NuclearNormMinimization().complete(X_incomplete)
		return fill_values(df, X_complete)

	if method == 'matrix_factorization':
		X_complete =  MatrixFactorization().complete(X_incomplete)
		return fill_values(df, X_complete)

	if method == 'iterative_svd':
		X_complete = IterativeSVD().complete(X_incomplete)
		return fill_values(df, X_complete)
"""
Function that performs imputation based on variable type
"""
def impute(df, config):
	"""
	Inputs:
	df -- dataframe of incomplete data
	config -- configuration file

	Output:
	Imputed matrix
	"""
	if config['imputation']['multiple_imputation']==True:
		return complex_imputation(df, method = config['imputation']['multiple_imputation_method'])

	if config['imputation']['select_imputation']==True:	
		# Select boolean variables
		boolean = df.select_dtypes(include=['bool', 'category'])
		boolean_imputed = simple_imputation(boolean, method=config['imputation']['binary'])
		
		# Select continuous variables
		continuous = df.select_dtypes(include=['float64', 'int64'])
		continuous_imputed = simple_imputation(continuous, method=config['imputation']['continuous'])

		return pd.concat([boolean_imputed, continuous_imputed], axis=1)
