import pytest
from odsemoji.tokenization import text2tagged, get_pipeline

def test_text2tagged():
    res = text2tagged([u'пожар пожарит'])
    assert res == [[u'пожар_NOUN', 'жарить_VERB']]

