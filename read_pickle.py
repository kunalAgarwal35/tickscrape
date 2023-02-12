import pickle
import os
import pandas as pd

filename = '2022-12-14-09-21-47-944496.pickle'


filepath  = os.path.join("C:\\Users\\Animesh\\Desktop\\Projects\\Trading\\test_ticks\\", filename)


with open(filepath, 'rb') as f:
    d = pickle.load(f)

print(d[0])
