"""
1. Find MW MAE for each GSP
2. Weighted average over each GSP by capacity of each capacity.

Sum (over GSP) (Sum (over time) (y_hat - y_i) / 48)  * w_i
w_i should be GSP capacity / total capacity

For example if there are 2 GSPs
1. MAE is 50MW with 100 MW capacity
2. MAE is 10MW with 1 MW capacity.

Normally MAE would get 30 MW
WMAE would be very closed to 50 MW.
"""
