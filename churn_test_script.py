from __future__ import division
import pandas as pd
import numpy as np

churn_df = pd.read_csv('dice.csv')
#col_names = churn_df.columns.split(',')
col_names = churn_df.columns.tolist()

print ("Column names:")
print (col_names)

print ("\nSample data:")
churn_df[col_names].head(10)

to_drop = ['target_date']
churn_feat_space = churn_df.drop(to_drop,axis=1)
print(churn_feat_space)

mat = churn_df[['platform', 'churn']]
print mat

features = churn_feat_space.columns
print(features)

X = churn_feat_space[['channel_name', 'platform', 'churn']].as_matrix().astype(float)
print(X)

from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
X = scaler.fit_transform(X)

X = churn_feat_space.as_matrix().astype(np.float)
print(X)

from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
X = scaler.fit_transform(X)

print "Feature space holds %d observations and %d features" % X.shape
#print "Unique target labels:", np.unique(y)

from sklearn.cross_validation import KFold

def run_cv(X,y,clf_class,**kwargs):
    # Construct a kfolds object
    kf = KFold(len(y),n_folds=5,shuffle=True)
    y_pred = y.copy()
    
    # Iterate through folds
    for train_index, test_index in kf:
        X_train, X_test = X[train_index], X[test_index]
        y_train = y[train_index]
        # Initialize a classifier with key word arguments
        clf = clf_class(**kwargs)
        clf.fit(X_train,y_train)
        y_pred[test_index] = clf.predict(X_test)
    return y_pred
	
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier as RF
from sklearn.neighbors import KNeighborsClassifier as KNN

def accuracy(y_true,y_pred):
    # NumPy interprets True and False as 1. and 0.
    return np.mean(y_true == y_pred)

print "Support vector machines:"
print "%.3f" % accuracy(churn_feat_space.count, run_cv(X,churn_feat_space.churn,SVC))
print "Random forest:"
print "%.3f" % accuracy(churn_feat_space.count, run_cv(X,churn_feat_space.churn,RF))
print "K-nearest-neighbors:"
print "%.3f" % accuracy(churn_feat_space.count, run_cv(X,churn_feat_space.churn,KNN))

from sklearn.metrics import confusion_matrix

y = np.array(y)
class_names = np.unique(y)

confusion_matrices = [
    ( "Support Vector Machines", confusion_matrix(y,run_cv(X,y,SVC)) ),
    ( "Random Forest", confusion_matrix(y,run_cv(X,y,RF)) ),
    ( "K-Nearest-Neighbors", confusion_matrix(y,run_cv(X,y,KNN)) ),
]

# Pyplot code not included to reduce clutter
from churn_display import draw_confusion_matrices
%matplotlib inline

draw_confusion_matrices(confusion_matrices,class_names)

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

from __future__ import division
import numpy as np

__author__ = "Eric Chiang"
__email__  = "eric[at]yhathq.com"

"""
Measurements inspired by Philip Tetlock's "Expert Political Judgment"
Equations take from Yaniv, Yates, & Smith (1991):
  "Measures of Descrimination Skill in Probabilistic Judgement"
"""


def calibration(prob,outcome,n_bins=10):
    """Calibration measurement for a set of predictions.
    When predicting events at a given probability, how far is frequency
    of positive outcomes from that probability?
    NOTE: Lower scores are better
    prob: array_like, float
        Probability estimates for a set of events
    outcome: array_like, bool
        If event predicted occurred
    n_bins: int
        Number of judgement categories to prefrom calculation over.
        Prediction are binned based on probability, since "descrete" 
        probabilities aren't required. 
    """
    prob = np.array(prob)
    outcome = np.array(outcome)

    c = 0.0
    # Construct bins
    judgement_bins = np.arange(n_bins + 1) / n_bins
    # Which bin is each prediction in?
    bin_num = np.digitize(prob,judgement_bins)
    for j_bin in np.unique(bin_num):
        # Is event in bin
        in_bin = bin_num == j_bin
        # Predicted probability taken as average of preds in bin
        predicted_prob = np.mean(prob[in_bin])
        # How often did events in this bin actually happen?
        true_bin_prob = np.mean(outcome[in_bin])
        # Squared distance between predicted and true times num of obs
        c += np.sum(in_bin) * ((predicted_prob - true_bin_prob) ** 2)
    return c / len(prob)

def discrimination(prob,outcome,n_bins=10):
    """Discrimination measurement for a set of predictions.
    For each judgement category, how far from the base probability
    is the true frequency of that bin?
    NOTE: High scores are better
    prob: array_like, float
        Probability estimates for a set of events
    outcome: array_like, bool
        If event predicted occurred
    n_bins: int
        Number of judgement categories to prefrom calculation over.
        Prediction are binned based on probability, since "descrete" 
        probabilities aren't required. 
    """
    prob = np.array(prob)
    outcome = np.array(outcome)

    d = 0.0
    # Base frequency of outcomes
    base_prob = np.mean(outcome)
    # Construct bins
    judgement_bins = np.arange(n_bins + 1) / n_bins
    # Which bin is each prediction in?
    bin_num = np.digitize(prob,judgement_bins)
    for j_bin in np.unique(bin_num):
        in_bin = bin_num == j_bin
        true_bin_prob = np.mean(outcome[in_bin])
        # Squared distance between true and base times num of obs
        d += np.sum(in_bin) * ((true_bin_prob - base_prob) ** 2)
    return d / len(prob)

from churn_measurements import calibration, discrimination

fr
test_churn_df.to_csv("test_churn.csv")

from yhat import Yhat,YhatModel,preprocess

class ChurnModel(YhatModel):
    # Type casts incoming data as a dataframe
    @preprocess(in_type=pd.DataFrame,out_type=pd.DataFrame)
    def execute(self,data):
        # Collect customer meta data
        response = data[['Area Code','Phone']]
        charges = ['Day Charge','Eve Charge','Night Charge','Intl Charge']
        response['customer_worth'] = data[charges].sum(axis=1)
        # Convert yes no columns to bool
        data[yes_no_cols] = data[yes_no_cols] == 'yes'
        # Create feature space
        X = data[features].as_matrix().astype(float)
        X = scaler.transform(X)
        # Make prediction
        churn_prob = clf.predict_proba(X)
        response['churn_prob'] = churn_prob[:,1]
        # Calculate expected loss by churn
        response['expected_loss'] = response['churn_prob'] * response['customer_worth']
        response = response.sort('expected_loss', ascending=False)
        # Return response DataFrame
        return response

yh = Yhat(
    "e[at]yhathq.com",
    "MY API KEY",
    "http://cloud.yhathq.com/"
)

response = yh.deploy("PythonChurnModel",ChurnModel,globals())

	
