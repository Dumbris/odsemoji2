from gensim.models.keyedvectors import KeyedVectors
import numpy as np

NEIGHBORS = 10

w2v = KeyedVectors.load_word2vec_format('all.norm-sz100-w10-cb0-it1-min100.w2v', binary=True, unicode_errors='ignore')
w2v.init_sims(replace=True)

from gensim.models import fasttext

# load_fasttext_format
# model = FastText.load_fasttext_format('/media/data/word2vec/araneum_none_fasttextskipgram_300_5_2018.tgz')

model = fasttext.FastText.load('/media/data/word2vec/araneum_fasttext/araneum_none_fasttextskipgram_300_5_2018.model')

print(model.most_similar('tensorflow'))
for n in model.most_similar(positive=[u'пожар_NOUN']):
    print(n[0], n[1])

'сепулька' in model.wv.vocab

model['сепулька']

