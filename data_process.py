#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from multiprocessing import Pool, cpu_count, Value
import jieba
import numpy as np
from tqdm import tqdm
import logging
import warnings

jieba.setLogLevel(logging.INFO)
warnings.filterwarnings("ignore")
# reader = pd.read_csv('百度百科.csv', chunksize=10000, iterator=True)
# num = 0
# for chunk in reader:
#     num += len(chunk)

cnt = cpu_count()

# size = int(num / cnt) + 1

reader = pd.read_csv('百度百科.csv', chunksize=100000, iterator=True)

punctuation = [",", "。", ":", ";", ".", "'", '"', "’", "？", "/", "\\", "-", "+", "&", "(", ")", "！", "《", "》", "，", "、",
               "“", "[", "]", "（", "）", "{", "}"]

num = Value('L', 0)


def __deal(x):
    # global count
    count = num.value
    with open('token1.txt', 'a', encoding='utf-8') as outf:
        sentence = jieba.lcut(x)
        tokenized = [txt for txt in sentence if txt not in punctuation]
        outf.write(' '.join(tokenized) + '\n')
        count += 1
        if count % 10000 == 0:
            tqdm.write('写入{}行'.format(count))


def cut_word(indf):
    # print('进程开始')
    tqdm.pandas()
    indf.text.progress_apply(lambda x: __deal(x))


if __name__ == '__main__':

    for chunk in tqdm(reader):
        pool = Pool(cnt)
        tmp = np.array_split(chunk, cnt, axis=0)
        for df in tmp:
            result = pool.apply_async(cut_word, (df,))
        # pool.close()
        # pool.join()
        result.wait()
        # pool.terminate()
