import pytest
from odsemoji.clean_text import normalize_repeated


@pytest.mark.parametrize("a,b", [
    ["tesssssssst", "tesst"],
    ["ffffffffffffuuuuuuuuuuuuuccccckkkkkkkkkkkk", "ffuucckk"],
    ["ррррррррррусcкий", "ррусcкий"]
]
                         )
def test_make_pairs(a,b):
    res = normalize_repeated(a, 2)
    assert res == b
