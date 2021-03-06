from tqdm import tqdm

from sklearn.utils import resample
from sklearn.model_selection import train_test_split
from string import punctuation
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score, classification_report
import csv
from collections import Counter
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.svm import LinearSVC
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB

import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer
from sklearn.metrics import f1_score, make_scorer, classification_report
import pandas as pd


class Classifiers():

    """In-sample and out-sample topic prediction based on CAP and CMP coding scheme.

    This class applies a set of traditional SML classifiers on different samples (more specifically, CAP and CMP data)
    to get some in-sample and out-sample predictions.
    """

    def __init__(self, trainingdata, evaluationdata):

        self.datasource_1 = trainingdata # datasource_1 = pandas df, e.g., CAP data
        self.datasource_2 = evaluationdata  # datasource_2 = pandas df, e.g., CMP data
        self.names = ["Naive Bayes", "Linear SVM", "Logistic Regression", "Random Forest" ]
        self.classifiers = [MultinomialNB(), LinearSVC(), LogisticRegression(), RandomForestClassifier() ]
        self.parameters = [
                      {'vect__ngram_range': [(1, 1), (1, 2)],
                      'clf__alpha': (1e-2, 1e-3)},
                      {'vect__ngram_range': [(1, 1), (1, 2)],
                      'clf__C': (np.logspace(-5, 1, 5))},
                      {'vect__ngram_range': [(1, 1), (1, 2)],
                      'clf__C': (np.logspace(-5, 1, 5))},
                      {'vect__ngram_range': [(1, 1), (1, 2)],
                      'clf__max_depth': (1, 2)}
                     ]

    def down_sample_majority(self, df, majortopic):

        df_majority = df[df[majortopic]==0]
        df_minority = df[df[majortopic]==1]
        df_majority_downsampled = resample(df_majority,
                                         replace=False,     #
                                         n_samples=len(df[df[majortopic]==1]), # set to N of minority topic
                                         random_state=123) #

        df_downsampled = pd.concat([df_minority, df_majority_downsampled])

        return df_downsampled


    def train_classifiers(self, majortopic):

        print(f"majortopic:{majortopic}")

        df = self.down_sample_majority(self.datasource_1, majortopic)

        X_train, X_test, y_train, y_test = train_test_split(df['text'], df[majortopic], test_size=0.2, random_state=42)



        class_report_insample = []
        class_report_outsample = []

        f1 = make_scorer(f1_score, average='macro') # macro --> emphasizes the prediction of the smaller classes (in this case: hateful/ spam)


        for vec, n in tqdm(zip([CountVectorizer(stop_words='english'), TfidfVectorizer(stop_words='english')], ["Count", "Tfidf"])):
            for name, classifier, params in zip(self.names, self.classifiers, self.parameters):
                clf_pipe = Pipeline([
                    ('vect', vec),
                    ('clf', classifier),
                ])

                gs_clf = GridSearchCV(clf_pipe, param_grid=params, cv=3, n_jobs=-1, scoring=f1) #or; scoring='accuracy' if you don't want to create the f1 object yourself (see above)
                clf = gs_clf.fit(X_train, y_train)

                score = clf.score(X_test, y_test)

                print("{} score: {}".format(name, score))
                print("{} are the best estimators".format(clf.best_estimator_))

                results_to_dict = classification_report((clf.best_estimator_.predict(X_test)), y_test, output_dict= True)
                results_to_dict['classifier'] = name
                results_to_dict['parameters'] = clf.best_params_
                results_to_dict['vectorizer'] = n
                class_report_insample.append(results_to_dict)

                results_to_dict = classification_report((clf.best_estimator_.predict(self.datasource_2['text'])), self.datasource_2[majortopic], output_dict= True)
                results_to_dict['classifier'] = name
                results_to_dict['parameters'] = clf.best_params_
                results_to_dict['vectorizer'] = n
                class_report_outsample.append(results_to_dict)

        return class_report_insample, class_report_outsample

    def clean_classification_report(self, class_report):

        appended_data = []
        for i in class_report:
            data = pd.DataFrame.from_dict(i)
            appended_data.append(data)

        df = pd.concat(appended_data)
        df['indicators'] = df.index

        return df

    def get_classification_results(self, major_topic):
        """This function will train several classifiers and return performance metrics using the original data"""

        appended_data = []

        print(f"starting classification of the topic ... {major_topic}....")

        class_report_insample, class_report_outsample = self.train_classifiers(major_topic)

        df_insample = self.clean_classification_report(class_report_insample)
        df_outsample = self.clean_classification_report(class_report_outsample)

        return df_insample, df_outsample

def clean_metrics(df):
    i = ['precision', 'recall', 'support', 'f1-score']
    df[df['indicators'].isin(i)]
    df.reset_index(inplace=True)
    d = df[df['indicators'].isin(i)].groupby(['classifier',  'indicators', 'vectorizer']).max()
    return d

def clean_parameters(df):
    i= ['vect__ngram_range', 'clf__alpha', 'clf__C']

    df = df[df['indicators'].isin(i)].groupby(['classifier', 'indicators', 'vectorizer']).max()
    df.reset_index(inplace=True)
    e = df.groupby(['classifier', 'vectorizer']).apply(lambda g: pd.Series(g.parameters.values, index= + g.indicators.astype(str)))

    parameters = pd.DataFrame(e)
    parameters.reset_index(inplace=True)
    parameters['unique_'] = parameters['classifier'] + "_" +  parameters['vectorizer']
    p = parameters.pivot(index='unique_', columns='indicators', values=0)
    parameters = parameters.groupby(['classifier','vectorizer']).first()

    p.reset_index(inplace=True)
    len(p) == len(parameters)
    parameters.reset_index(inplace=True)
    parameters = pd.merge(p, parameters, on='unique_')
    return parameters

def get_cleaned_data(dataset):
    df = pd.merge(clean_metrics(dataset), clean_parameters(dataset), on=['classifier','vectorizer'], how='left')
    df.rename(columns={'index': 'metrics'}, inplace=True)
    print("..........loaded the data frame........")
    return df
