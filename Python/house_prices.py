#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import packages
import cvxopt
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.api as sm
from statsmodels.distributions.empirical_distribution import ECDF
from statsmodels.multivariate.pca import PCA


# In[2]:


# adjust default plot fonts
plt.rc('font', size=11)
plt.rc('figure', titlesize=13)


# In[3]:


# set paths
file_train = '~/Downloads/SpotX/house-prices-advanced-regression-techniques/train.csv'
file_test = '~/Downloads/SpotX/house-prices-advanced-regression-techniques/test.csv'
file_benchmark = '~/Downloads/SpotX/house-prices-advanced-regression-techniques/sample_submission.csv'


# In[4]:


# create dataframes
df_train0 = pd.read_csv(file_train)
df_test0 = pd.read_csv(file_test)
df_benchmark = pd.read_csv(file_benchmark)


# In[5]:


# convert false missing values back to 'NA'
col_index = df_train0.columns.to_list()
list_false_na = ['Alley', 'BsmtQual', 'BsmtCond', 'BsmtExposure', 'BsmtFinType1', 'BsmtFinType2', 'FireplaceQu', 'GarageType', 'GarageFinish', 'GarageQual', 'GarageCond', 'PoolQC', 'Fence', 'MiscFeature']
df_false_na = df_train0.loc[:, list_false_na].fillna(value='NA')
df_train1 = df_train0.drop(list_false_na, axis=1)
df_train2 = df_train1.join(df_false_na)
df_train3 = df_train2.reindex(columns=col_index)


# In[6]:


# create SalePriceCategory
df_train4 = df_train3.assign(SalePriceCategory=np.where(df_train3['SalePrice']>=200000, 1, 0))


# In[7]:


# delete rows with missing values
df_train5 = df_train4.dropna()


# In[8]:


# descriptive statistics
print('Descriptive Statistics for Sale Price\n')
print('skewness      %f' % df_train5['SalePrice'].skew())
print('kurtosis      %f' % df_train5['SalePrice'].kurt())
print(df_train5.loc[:, 'SalePrice'].describe())


# In[9]:


# histogram
f, ax = plt.subplots(figsize=(11, 7))
sns.distplot(df_train5['SalePrice'])
plt.ticklabel_format(style='plain', axis='y')
ax.set_title('Probability Density Function of Sale Price\n')
ax.set_xlabel('Sale Price')
ax.set_ylabel('Density')
plt.show()


# In[10]:


# empirical cumulative distribution function
f, ax = plt.subplots(figsize=(11, 7))
ecdf = ECDF(df_train5['SalePrice'])
threshold = ecdf(200000)
plt.plot(ecdf.x, ecdf.y)
ax.set_title('Empirical Cumulative Distribution Function of Sale Price\n')
ax.set_xlabel('Sale Price')
ax.set_ylabel('Probability')
plt.axhline(y=threshold, xmin=0, xmax=0.25, linestyle='--')
plt.axvline(x=200000, ymin=0, ymax=threshold-0.015, linestyle='--')
plt.text(x=210000, y=threshold-0.015, s='P(Sale Price < $200,000) = %f' % threshold)
plt.show()


# In[11]:


# select variables with absolute correlation > 0.5
corr_minimum = abs(0.5)
corr_matrix0 = df_train5.corr(method='pearson')
corr_vars = corr_matrix0.gt(corr_minimum).query('SalePrice == True').index.to_list()
corr_vars.remove('SalePriceCategory')


# In[12]:


# correlation heatmap
corr_matrix1 = corr_matrix0.loc[corr_vars, corr_vars]
matrix_index = corr_matrix1.sort_values('SalePrice', ascending=False).index.to_list()
corr_matrix2 = corr_matrix1.reindex(index=matrix_index, columns=matrix_index)
f, ax = plt.subplots(figsize=(11, 11))
ax.set_title('Heatmap of Variables with Absolute Pearson Correlation Exceeding 0.5 for Sale Price\n')
sns.heatmap(corr_matrix2, cmap='YlGnBu', annot=True, cbar=False)
plt.show()


# In[13]:


# create principal components
# NOTE: drop_first=False for get_dummies because PCA eliminates collinearity
list_dummy_vars = ['MSSubClass'] + df_train5.select_dtypes(include=['object']).columns.to_list()
df_dummy_vars = pd.get_dummies(df_train5.loc[:, list_dummy_vars].astype('category'), prefix=list_dummy_vars, drop_first=False, dtype=int)
pca_model = PCA(df_dummy_vars, standardize=False)
df_train6 = df_train5.join(pca_model.factors)
pca_model.plot_scree(log_scale=False)
plt.show()
print('Top 15 Loadings for Principal Component 1', '\n\n', pca_model.loadings.loc[:, ['comp_000']].nlargest(15, ['comp_000']), '\n')
print('Top 15 Loadings for Principal Component 2', '\n\n', pca_model.loadings.loc[:, ['comp_001']].nlargest(15, ['comp_001']), '\n')
print('Top 15 Loadings for Principal Component 3', '\n\n', pca_model.loadings.loc[:, ['comp_002']].nlargest(15, ['comp_002']))


# In[14]:


# logit model
predict_vars = corr_vars + ['comp_000', 'comp_001', 'comp_002']
predict_vars.remove('SalePrice')
y = df_train6.loc[:, 'SalePriceCategory']
X = sm.add_constant(df_train6.loc[:, predict_vars].astype('float'))
logit_model = sm.Logit(y, X)
logit_results = logit_model.fit_regularized(method='l1_cvxopt_cp', disp=0, alpha=0)
margeff = logit_results.get_margeff()
print(logit_results.summary(), '\n')
print(margeff.summary())


# In[15]:


# train prediction
col1 = df_train6.loc[:, 'Id']
col2 = df_train6.loc[:, 'SalePrice']
col3 = df_train6.loc[:, 'SalePriceCategory']
col4 = logit_results.predict(X)
df_train_estimates = pd.concat([col1, col2, col3, col4], axis=1, keys=['Id', 'Sale Price', 'Sale Price Category', 'Predicted Probability'])
df_train_estimates['Predicted Category'] = np.where(df_train_estimates['Predicted Probability']>=threshold, 1, 0)
df_train_estimates['TP'] = np.where((df_train_estimates['Sale Price Category']==1) & (df_train_estimates['Predicted Category']==1), 1, 0)
df_train_estimates['TN'] = np.where((df_train_estimates['Sale Price Category']==0) & (df_train_estimates['Predicted Category']==0), 1, 0)
df_train_estimates['FP'] = np.where((df_train_estimates['Sale Price Category']==0) & (df_train_estimates['Predicted Category']==1), 1, 0)
df_train_estimates['FN'] = np.where((df_train_estimates['Sale Price Category']==1) & (df_train_estimates['Predicted Category']==0), 1, 0)


# In[16]:


# train predictive fitness
TP = df_train_estimates.loc[:, 'TP'].sum()
TN = df_train_estimates.loc[:, 'TN'].sum()
FP = df_train_estimates.loc[:, 'FP'].sum()
FN = df_train_estimates.loc[:, 'FN'].sum()
ACC = (TP+TN)/(TP+TN+FP+FN)
PPV = TP/(TP+FP)
TPR = TP/(TP+FN)
TNR = TN/(TN+FP)
print('Train Predictive Fitness\n')
print('Accuracy:     %f' % ACC)
print('Precision:    %f' % PPV)
print('Sensitivity:  %f' % TPR)
print('Specificity:  %f' % TNR)


# In[17]:


# ready test data
df_test1 = df_test0.join(df_benchmark, lsuffix='_left', rsuffix='_right')
col_index = df_test1.columns.to_list()
list_false_na = ['Alley', 'BsmtQual', 'BsmtCond', 'BsmtExposure', 'BsmtFinType1', 'BsmtFinType2', 'FireplaceQu', 'GarageType', 'GarageFinish', 'GarageQual', 'GarageCond', 'PoolQC', 'Fence', 'MiscFeature']
df_false_na = df_test1.loc[:, list_false_na].fillna(value='NA')
df_test2 = df_test1.drop(list_false_na, axis=1)
df_test3 = df_test2.join(df_false_na)
df_test4 = df_test3.reindex(columns=col_index)
df_test5 = df_test4.assign(SalePriceCategory=np.where(df_test4['SalePrice']>=200000, 1, 0))
df_test6 = df_test5.dropna()
list_dummy_vars = ['MSSubClass'] + df_test6.select_dtypes(include=['object']).columns.to_list()
df_dummy_vars = pd.get_dummies(df_test6.loc[:, list_dummy_vars].astype('category'), prefix=list_dummy_vars, drop_first=False, dtype=int)
pca_model = PCA(df_dummy_vars, standardize=False)
df_test6 = df_test6.join(pca_model.factors)


# In[18]:


# test prediction
X = sm.add_constant(df_test6.loc[:, predict_vars].astype('float'))
col1 = df_test6.loc[:, 'Id_left']
col2 = df_test6.loc[:, 'SalePrice']
col3 = df_test6.loc[:, 'SalePriceCategory']
col4 = logit_results.predict(X)
df_test_estimates = pd.concat([col1, col2, col3, col4], axis=1, keys=['Id', 'Sale Price', 'Sale Price Category', 'Predicted Probability'])
df_test_estimates['Predicted Category'] = np.where(df_test_estimates['Predicted Probability']>=threshold, 1, 0)
df_test_estimates['TP'] = np.where((df_test_estimates['Sale Price Category']==1) & (df_test_estimates['Predicted Category']==1), 1, 0)
df_test_estimates['TN'] = np.where((df_test_estimates['Sale Price Category']==0) & (df_test_estimates['Predicted Category']==0), 1, 0)
df_test_estimates['FP'] = np.where((df_test_estimates['Sale Price Category']==0) & (df_test_estimates['Predicted Category']==1), 1, 0)
df_test_estimates['FN'] = np.where((df_test_estimates['Sale Price Category']==1) & (df_test_estimates['Predicted Category']==0), 1, 0)


# In[19]:


# test predictive fitness
TP = df_test_estimates.loc[:, 'TP'].sum()
TN = df_test_estimates.loc[:, 'TN'].sum()
FP = df_test_estimates.loc[:, 'FP'].sum()
FN = df_test_estimates.loc[:, 'FN'].sum()
ACC = (TP+TN)/(TP+TN+FP+FN)
PPV = TP/(TP+FP)
TPR = TP/(TP+FN)
TNR = TN/(TN+FP)
print('Test Predictive Fitness\n')
print('Accuracy:     %f' % ACC)
print('Precision:    %f' % PPV)
print('Sensitivity:  %f' % TPR)
print('Specificity:  %f' % TNR)

