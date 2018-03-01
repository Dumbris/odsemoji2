import pytest
import unittest
import json
from pathlib import Path
from odsemoji2.utils.slack_data_loader2 import SlackLoader2


BASE_DIR = Path("../input/export_Feb_8_2018")
MSG_REC = """{
        "type": "message",
        "user": "U0873FY94",
        "text": "загрузилось?",
        "ts": "1450624534.000010"
    }
"""
MSG_TEXT = "загрузилось?"

class DataLoaderTest(unittest.TestCase):

    def testParseRecord(self):
        rec = SlackLoader2.parse_record(json.loads(MSG_REC), 1, {"name": "_random"})
        assert rec["text"] == MSG_TEXT

