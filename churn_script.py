import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from patsy import dmatrices
from sklearn.linear_model import LogisticRegression
from sklearn.cross_validation import train_test_split
from sklearn import metrics
from sklearn.cross_validation import cross_val_score
from __future__ import division
%pylab inline
from iminuit import Minuit, describe, Struct
from __future__ import print_function
from __future__ import division

from scipy.cluster.vq import kmeans,vq
from scipy.spatial.distance import cdist
import matplotlib.pyplot as plt


df = pd.read_csv('o.csv', delimiter='|')
#col_names = churn_df.columns.split('|')
col_names = df.columns.tolist()

print ("Column names:")
print (col_names)
print ("\nSample data:")
df['is_payer'].head(10)  # change the date and time

#df.info
df['l30'].describe()

df['churn'].describe()

df.groupby('churn').mean()

%matplotlib inline

df.churn.hist()
plt.title('Histogram of Churn Rate')
plt.xlabel('Churn or Not Churn')
plt.ylabel('Frequency')

df.is_payer.hist()
plt.title('Histogram of Is_Payer')
plt.xlabel('Payer or Not Payer')
plt.ylabel('Frequency')

df.pre_titan_user.hist()
plt.title('Histogram of pre_titan_user')
plt.xlabel('Pre_titan_user or Not')
plt.ylabel('Frequency')

# barplot of marriage rating grouped by affair (True or False)
pd.crosstab(df.is_payer, df.churn.astype(bool)).plot(kind='bar')
plt.title('Is Payer Distribution by Churn Status')
plt.xlabel('Is Payer')
plt.ylabel('Frequency')

# create dataframes with an intercept column and dummy variables for
# occupation and occupation_husb
y, X = dmatrices('churn ~  purchases +  \
                  days_to_first_purchase + days_since_last_purchase + is_payer + pre_titan_user  + \
                  days_since_install + total_bonus_roll_used + total_turns + tournament_turns +\
                  pvp_turns + tournament_bonus_rolls_used + \
                  pvp_bonus_rolls_used + gross_revenue + gross_rev_at_7 + \
                  gross_rev_at_30 + gross_rev_at_60 + gross_rev_at_90 + gross_rev_at_180 + gross_rev_at_365 + gross_rev_at_1095',
                  df, return_type="dataframe")
print (X.columns)

# flatten y into a 1-D array
y = np.ravel(y)

# instantiate a logistic regression model, and fit with X and y
model = LogisticRegression()
model = model.fit(X,y)
# check the accuracy on the training set
model.score(X,y)

# what percentage had churn?
y.mean()

# examine the coefficients
pd.DataFrame(zip(X.columns, np.transpose(model.coef_)))

# evaluate the model by splitting into train and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)
model2 = LogisticRegression()
model2.fit(X_train, y_train)

# predict class labels for the test set
predicted = model2.predict(X_test)
print (predicted)

# generate class probabilities
probs = model2.predict_proba(X_test)
print (probs)

# generate evaluation metrics
print (metrics.accuracy_score(y_test, predicted))
print (metrics.roc_auc_score(y_test, probs[:, 1]))

##get confusion matrix
print (metrics.confusion_matrix(y_test, predicted))
print (metrics.classification_report(y_test, predicted))

# evaluate the model using 10-fold cross-validation
scores = cross_val_score(LogisticRegression(), X, y, scoring='accuracy', cv=10)
print (scores)
print (scores.mean())

from pylab import plot,show
from numpy import vstack,array
from numpy.random import rand
from scipy.cluster.vq import kmeans,vq

# data generation
data = vstack((rand(150,2) + array([.5,.5]),rand(150,2)))
#data = vstack(df.churn)

# computing K-Means with K = 2 (2 clusters)
centroids,_ = kmeans(data,10)
# assign each sample to a cluster
idx,_ = vq(data,centroids)

# some plotting using numpy's logical indexing
plot(data[idx==0,0],data[idx==0,1],'ob',
     data[idx==1,0],data[idx==1,1],'or')
plot(centroids[:,0],centroids[:,1],'sg',markersize=8)
show()

W = df[['churn', 'total_bonus_roll_used','total_turns','tournament_turns',
                  'pvp_turns','tournament_bonus_rolls_used',
                  'pvp_bonus_rolls_used'
                  ]].as_matrix().astype(np.float)
print(W)

W = df[['pvp_bonus_rolls_used']].as_matrix().astype(float)
print(W)

print ("Feature space holds %d observations and %d features" % X.shape)
#print "Unique target labels:", np.unique(y)

from sklearn.cross_validation import KFold

def run_cv(X,y,clf_class):
    # Construct a kfolds object
    kf = KFold(len(y),n_folds=5,shuffle=True)
    print(type(kf))
    y_pred = y.copy()
    
    # Iterate through folds
    for train_index, test_index in kf:
        print("{}{}".format(train_index, test_index))
        X_train, X_test = X[train_index], X[test_index]
        y_train = y[train_index]
        # Initialize a classifier with key word arguments
        clf = clf_class(model)
        clf.fit(X_train,y_train)
        y_pred[test_index] = clf.predict(X_test)
    return y_pred


run_cv(X, df.churn.values, SVC)

from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier as RF
from sklearn.neighbors import KNeighborsClassifier as KNN

def accuracy(y_true,y_pred):
    # NumPy interprets True and False as 1. and 0.
    return np.mean(y_true == y_pred)

print("Support vector machines:")
print("%.3f" % accuracy(df.churn, run_cv(X,df.churn,SVC)))
print("Random forest:")
print("%.3f" % accuracy(df.churn, run_cv(X,df.churn,RF)))
print("K-nearest-neighbors:")
print("%.3f" % accuracy(df.churn, run_cv(X,df.churn,KNN)))

def run_prob_cv(X, y, clf_class, **kwargs):
    kf = KFold(len(y), n_folds=5, shuffle=True)
    y_prob = np.zeros((len(y),2))
    for train_index, test_index in kf:
        X_train, X_test = X[train_index], X[test_index]
        y_train = y[train_index]
        clf = clf_class(**kwargs)
        clf.fit(X_train,y_train)
        # Predict probabilities, not classes
        y_prob[test_index] = clf.predict_proba(X_test)
    return y_prob
      
	  
	  import warnings
warnings.filterwarnings('ignore')

# Use 10 estimators so predictions are all multiples of 0.1
pred_prob = run_prob_cv(X, y, RF, n_estimators=10)
pred_churn = pred_prob[:,1]
is_churn = y == 1

# Number of times a predicted probability is assigned to an observation
counts = pd.value_counts(pred_churn)

# calculate true probabilities
true_prob = {}
for prob in counts.index:
    true_prob[prob] = np.mean(is_churn[pred_churn == prob])
    true_prob = pd.Series(true_prob)

# pandas-fu
counts = pd.concat([counts,true_prob], axis=1).reset_index()
counts.columns = ['pred_prob', 'count', 'true_prob']
counts

from ggplot import *
%matplotlib inline

baseline = np.mean(is_churn)
ggplot(counts,aes(x='pred_prob',y='true_prob',size='count')) + \
    geom_point(color='blue') + \
    stat_function(fun = lambda x: x, color='red') + \
    stat_function(fun = lambda x: baseline, color='green') + \
    xlim(-0.05,  1.05) + ylim(-0.05,1.05) + \
    ggtitle("Random Forest") + \
    xlab("Predicted probability") + ylab("Relative frequency of outcome")

