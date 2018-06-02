
import signals.technical_machine_learner as tml
from sklearn.ensemble import ExtraTreesClassifier
import pandas as pd
from sklearn.model_selection import GridSearchCV
from sklearn.svm import SVC
from sklearn import svm

def get_results_4instrument(**kwargs):

    data_frame = tml.get_tms(**kwargs)

    feature_list = ['obv', 'rsi_6', 'rsi_12', 'atr_14', 'mfi_14',
                    'adx_14', 'adx_20', 'rocr_1', 'rocr_3', 'rocr_12', 'cci_12', 'cci_20',
                    'macd_12_26', 'macd_signal_12_26_9', 'macd_hist_12_26_9',
                    'williams_r_10', 'tsf_10_3', 'tsf_20_3', 'trix_15']
    num_trees = 100
    max_features = 7
    tuned_parameters = [{'gamma': [0.001, 0.01, 0.1, 1, 10, 100], 'C': [1, 10, 100, 1000], 'kernel': ['rbf']}]

    test_sample_list = []
    for i in range(950,len(data_frame.index),50):
        print(i)
        training_sample = data_frame.iloc[(i-950):i]
        test_sample = data_frame.iloc[(i+3):(i+53)]

        X = training_sample[feature_list].values
        Y = training_sample['target3'].values

        extra_tree_model = ExtraTreesClassifier(n_estimators=num_trees, max_features=max_features)

        extra_tree_model.fit(X, Y)

        importance_frame = pd.DataFrame.from_items([('importance', extra_tree_model.feature_importances_), ('name', feature_list)])
        importance_frame.sort_values('importance', inplace=True, ascending=False)
        important_feature_list = importance_frame['name'].iloc[:6].values

        clf = GridSearchCV(SVC(), tuned_parameters, cv=5, scoring='precision_macro')

        X = training_sample[important_feature_list].values
        clf.fit(X, Y)
        print(clf.best_params_)
        final_clf = svm.SVC(kernel=clf.best_params_['kernel'], C=clf.best_params_['C'], gamma=clf.best_params_['gamma'])
        final_clf.fit(X, Y)

        X_test = test_sample[important_feature_list].values

        test_sample['prediction'] = final_clf.predict(X_test)
        test_sample_list.append(test_sample)

    return pd.concat(test_sample_list)

