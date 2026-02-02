from rdflib import Graph, Literal, RDF, URIRef, Namespace
from tqdm import tqdm
import pandas as pd
import numpy as np
import os
# get all the files in the folder starting with 'University*' and load it into a list
def get_files():
    import os
    files = os.listdir()
    files = [f for f in files if f.startswith('University')]
    return files
file_names = get_files()

g = Graph()
for file in file_names:
    g+=Graph().parse(file)
    
n = Namespace("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#")
meta = {}
entities = {}
relations = {}
relations_string = {}

for subj, pred, obj in tqdm(g):
    # 空图谱检查
    if "#" in subj:
        subj = subj.split('#')[-1]
    if "#" in obj:
        obj = obj.split('#')[-1]
    if "#" in pred:
        pred = pred.split('#')[-1]
    if subj not in entities:
        entities[subj] = len(entities)
    if obj not in entities:
        entities[obj] = len(entities)
    if pred not in relations:
        relations[pred] = []
    if pred not in relations_string:
        relations_string[pred] = []
    relations_string[pred].append((subj, obj))
    relations[pred].append((entities[subj], entities[obj]))
# save everything in a csv file
# make sure the folder lumb is exist, if not, create it
try:
    os.mkdir('LUBM')
except:
    pass

for pred in relations.keys():
    pred_1 = 't' if pred=='type' else pred
    df = pd.DataFrame(relations[pred], columns=[pred_1+'_1', pred_1+'_2'])
    df.to_csv(f'LUBM/{pred_1}.csv', index=False)

# write a txt file to store the entity mapping
entities_items = list(entities.items())
entities_items.sort(key=lambda x: x[1])                     
entities_id = [x[0] for x in entities_items]
with open('LUBM/mapping.txt', 'w') as f:
    for e in entities_id:
        f.write(f'{e}\n')
        
# write the relations metadata to a txt file, format: relation_name, arity, record_count
with open('LUBM/meta.txt', 'w') as f:
    for pred in relations.keys():
        pred_1 = 't' if pred=='type' else pred
        f.write(f'{pred_1}\t2\t{len(relations[pred])}\n')
