# Ground truth and predictions
# Ensure gt is the ground truth, approx and hive are predictions

gt = 10.67777778
approx = [10.6640625,
10.68452381,
10.69122807,
10.70886076,
10.75132275,
10.75362319,
10.73333333,
10.61554192,
10.63718821,
10.65903308,
10.68405797,
10.68686869,
10.70281124,
10.75252525,
10.74000000,
10.72549020,
10.72549020,
11.00000000,
11.35575486,
10.73118280,
10.68888889,
11.44585091,
11.31515152,
10.81535948,
10.60992908,
10.61821705,
10.62393162,
10.65000000,
10.67479675,
10.68535826]
hive = [
10.18772563,
10.75177305,
10.534375,
10.6928839,
10.68589744,
10.56906077,
10.142857143,
10.51301115,
11.19649123,
10.72727273,
10.5221843,
10.49753695,
10.50220264,
16.32686567,
10.70886076,
10.71491228,
10.69259259,
10.73563218,
10.75177305,
10.7311828,
10.75384615,
10.56395349,
10.52317881,
10.70781893,
10.68646865,
10.72868217,
10.72463768,
10.69148936,
9.612903226,
10.70833333
]

# Approximation Approach MAPE: 0.8731682932626177
# Streaming Query Hive MAPE: 0.8730383973208999
# Approximation Approach Percentage Accuracy: 99.12683170673738
# Streaming Query Hive Percentage Accuracy: 99.1269616026791

def percent_error(estimate, truth):
    return abs(estimate - truth) / truth * 100

def mean_absolute_percentage_error(y_true, y_pred):
    y_true, y_pred = list(y_true), list(y_pred)
    return sum(abs((yt - yp) / yt) for yt, yp in zip(y_true, y_pred)) / len(y_true) * 100

def percentage_accuracy(y_true, y_pred):
    y_true, y_pred = list(y_true), list(y_pred)
    return sum(1 - abs((yt - yp) / yt) for yt, yp in zip(y_true, y_pred)) / len(y_true) * 100

# Example usage:
# y_true = [10.67777778]
# y_pred = [10.77101275]
print(len(hive), len(approx))  # Should be the same length
# Correct usage: y_true = [gt], y_pred = [approx] or [hive]
print('Approximation Approach MAPE:', mean_absolute_percentage_error([gt], approx))
print('Streaming Query Hive MAPE:', mean_absolute_percentage_error([gt], hive))
print('Approximation Approach Percentage Accuracy:', percentage_accuracy([gt], approx))
print('Streaming Query Hive Percentage Accuracy:', percentage_accuracy([gt], hive))