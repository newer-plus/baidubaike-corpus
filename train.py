from gensim.models import Word2Vec
from gensim.models.word2vec import LineSentence
import logging

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',level=logging.INFO)

def train():
    baike = open('token.txt','r',encoding='utf-8')
    model = Word2Vec(LineSentence(baike),sg=1,size=192,window=5,min_count=10,workers=9)
    model.save('baidubaike.word2vec')

if __name__ == '__main__':
    train()