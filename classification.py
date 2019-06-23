from pprint import pprint

# from skmultilearn.dataset import load_dataset
#
#
#
# X_train, y_train, feature_names, label_names = load_dataset('emotions', 'train')
# X_test, y_test, _, _ = load_dataset('emotions', 'test')
#
#
# print(X_train)
# # pprint(label_names)
#
# print(type(y_test))
#


# # pprint(feature_names)
#
# from skmultilearn.problem_transform import BinaryRelevance
# from sklearn.svm import SVC
#
# clf = BinaryRelevance(
#     classifier=SVC(gamma='auto'),
#     require_dense=[False, True],
# )
#
# clf.fit(X_train, y_train)
#
# print(clf)
#
# prediction = clf.predict(X_test)
#
# pprint(prediction)
#
# import sklearn.metrics as metrics
# print(metrics.hamming_loss(y_test, prediction))
# print(metrics.accuracy_score(y_test, prediction))


from scipy import sparse
from numpy import array
I = array([1,3,1,0,1,3])
J = array([0,3,1,2,0,2])
V = array([4,5,7,9])



from skmultilearn.dataset import load_dataset

X_train, y_train, _, _ = load_dataset('scene', 'train')
X_test,  y_test, _, _ = load_dataset('scene', 'test')


from skmultilearn.ext import Meka
meka_classpath = "/opt/meka-release-1.9.0/lib"
meka = Meka(
        meka_classifier = "meka.classifiers.multilabel.BCC", # Binary Relevance
        weka_classifier = "weka.classifiers.bayes.NaiveBayesMultinomial", # with Naive Bayes single-label classifier
        meka_classpath = meka_classpath, #obtained via download_meka
        java_command = '/usr/bin/java' # path to java executable
)
meka

meka.fit(X_train, y_train)
predictions = meka.predict(X_test)

from sklearn.metrics import hamming_loss

print(hamming_loss(y_test, predictions))