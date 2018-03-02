import unittest
import json
from pathlib import Path
from odsemoji2.utils.slack_data_loader2 import SlackLoader2
from tests.data_loader2_att_msg import ATT_MSG, ATT_MSG_TEXT


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

    def testGetText(self):
        text = SlackLoader2.get_text(json.loads(ATT_MSG))
        assert text == ATT_MSG_TEXT

    def testGetReactions(self):
        rec = SlackLoader2.parse_record(json.loads(MSG_REC), 1, {"name": "_random"})
        assert rec["text"] == MSG_TEXT

    def testMsgLoad(self):
        sl = SlackLoader2(BASE_DIR, exclude_channels=[], only_channels=["career"])
        sl.load_messages()
        print(len(sl.messages))
        #assert rec["text"] == MSG_TEXT

    def testThreadsLoad(self):
        sl = SlackLoader2(BASE_DIR, exclude_channels=[], only_channels=["_call_4_collaboration"])
        sl.load_messages()
        sl.process_threads()
        print(len(sl.threads))
        #assert rec["text"] == MSG_TEXT

