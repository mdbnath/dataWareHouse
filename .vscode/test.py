import os,sys
try:
    f = open("C:/Users/manja/OneDrive/Documents/Advanced Database/clinical_trials_dump/13-10-2021/trial-01-13102021.txt", mode='rb')
    print(f.read())
finally:
    f.close()