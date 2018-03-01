import datetime
import json
import os
import re
from pathlib import Path
from collections import namedtuple, defaultdict, deque
import tqdm


re_slack_link = re.compile(r'(?P<all><(?P<id>[^\|]*)(\|(?P<title>[^>]*))?>)')


Thread = namedtuple('Tread', ['channel', 'channel_name',
                              'user', 'ts', 'text', 'reactions',
                              'sub_user', 'sub_ts', 'sub_text', 'sub_reactions',
                              'msg_counter'
                              ])

def _read_json_dict(filename, key='id'):
    with open(filename) as fin:
        records = json.load(fin)
        json_dict = {
            record[key]: record
            for record in records
        }
    return json_dict


EOU_TOKEN = ""
REACTS_THRESHOLD = 1
KEEP_N_LAST_MSG = 3


class SlackLoader2:

    def __init__(self, export_path, exclude_channels=(), only_channels=(), start_date=None, end_date=None,
                 is_sorted=True, thread_window_size=KEEP_N_LAST_MSG):
        self.export_path            = export_path
        self.exclude_channels       = exclude_channels
        self.only_channels          = only_channels
        self.is_sorted              = is_sorted
        self.thread_window_size     = thread_window_size
        self.threads_index          = None
        self.threads                = None
        self.text_messages          = None
        if start_date:
            self.start_date = (start_date - datetime.datetime(1970, 1, 1)).total_seconds()
        else:
            self.start_date = None
        if end_date:
            self.end_date = (end_date - datetime.datetime(1970, 1, 1)).total_seconds()
        else:
            self.end_date = None
        self.channels = None
        self.users = None
        self.messages = None

    def load_messages(self):
        self.channels = _read_json_dict(os.path.join(str(self.export_path), 'channels.json'))
        self.users = _read_json_dict(os.path.join(str(self.export_path), 'users.json'))
        self.messages = self.load_export(self.export_path, self.is_sorted)

    @staticmethod
    def get_reactions(msg, threshold=REACTS_THRESHOLD):
        if msg['type'] == 'message' and msg.get('subtype') is None:
            msg_reacts = {}
            #react_texts.append(normalize_links(msg['text']))
            for record in msg.get('reactions', []):
                if int(record['count']) >= threshold:
                    name = record['name'].split("::")[0] #Remove skin-tone
                    msg_reacts[name] = record['count']
            return msg_reacts if len(msg_reacts) > 0 else None
        return None

    @staticmethod
    def key_str(key):
        return str(key[0]) + "/" + str(key[1])

    @staticmethod
    def get_text(msg):
        keys = ["text", "plain_text"]
        att_keys = ["text", "more"]
        text = None
        for key in keys:
            if (key in msg) and msg[key] and (len(msg[key]) > 0):
                text = msg[key]
                break
        if "attachments" in msg:
            att_texts = []
            for att in msg["attachments"]:
                for att_key in att_keys:
                    if (att_key in att) and att[att_key] and (len(att[att_key]) > 0):
                        att_texts.append(att[att_key] + EOU_TOKEN)
            if not text:
                text = " ".join(att_texts)
            else:
                text += " ".join(att_texts)
        return text

    @staticmethod
    def parse_record(record, channel_id, channel):
        if 'ts' in record:
            record['ts'] = float(record['ts'])
            record['dt'] = datetime.datetime.fromtimestamp(record['ts'])
        record['channel'] = channel_id
        record['channel_name'] = channel["name"]
        if 'reactions' in record:
            record['reactions_'] = SlackLoader2.get_reactions(record)
        if 'text' in record:
            record['text_'] = SlackLoader2.get_text(record)
        return record

    def filtered_record(self, record):
        if record.get('subtype') == 'bot_message':
            return True
        if 'ts' in record:
            if self.start_date and float(record['ts']) < self.start_date:
                return True
            if self.end_date and float(record['ts']) > self.end_date:
                return True
        return False

    def load_export(self, export_path, is_sorted=True):
        """
                1) Link to parent message:
              "thread_ts": "1517643521.000001",
                "parent_user_id": "U1UNFRQ1K",
                2) attachments[].text
        """
        messages = []
        skipped_counter = 0
        #ref_to_id = {}
        for channel_id, channel in self.channels.items():
            #if channel['is_archived']:
            #    continue
            if channel['name'] in self.exclude_channels:
                continue
            if self.only_channels and channel['name'] not in self.only_channels:
                continue
            messages_glob = export_path / Path(channel['name'])
            for messages_filename in messages_glob.glob('*.json'):
                with open(str(messages_filename)) as f_messages:
                    for record in json.load(f_messages):
                        if self.filtered_record(record):
                            skipped_counter += 1
                            continue
                        messages.append(SlackLoader2.parse_record(record, channel_id, channel))
        if is_sorted:
            messages = sorted(messages, key=lambda x: x['ts'])
        print("{} messages was skipped.".format(skipped_counter))
        return messages

    def index_threads(self):
        dd = defaultdict(list)
        for i in range(0, len(self.messages)):
            msg = self.messages[i]
            if "thread_ts" in msg:
                key = (msg["channel"], msg["thread_ts"])
                dd[self.key_str(key)].append(i)
        self.threads_index = dd

    def rip_threads(self):
        processed_ids = []
        if not self.threads:
            self.threads = []
        for i in tqdm.tqdm(range(0, len(self.messages))):
            if i in processed_ids:
                continue
            msg = self.messages[i]
            if "text" not in msg:
                continue

            key = (msg["channel"], msg["ts"])
            msg_counter = 1
            top = Thread(
                channel     = msg.get('channel'),
                channel_name = msg.get('channel_name'),
                text        = SlackLoader2.get_text(msg),
                user        = msg.get('user'),
                ts          = msg.get('ts'),
                reactions   = SlackLoader2.get_reactions(msg),
                msg_counter  = msg_counter,
                sub_user=None, sub_ts=None, sub_text=None, sub_reactions=None
            )

            self.threads.append(top)

            processed_ids.append(i)
            deque_window = {
                'sub_user': deque(maxlen=self.thread_window_size),
                'sub_ts': deque(maxlen=self.thread_window_size),
                'sub_text': deque(maxlen=self.thread_window_size),
                'sub_reactions': deque(maxlen=self.thread_window_size)
            }

            for submsg_index in self.threads_index[self.key_str(key)]:
                if submsg_index in processed_ids:
                    continue
                submsg = self.messages[submsg_index]
                deque_window["sub_user"].append(submsg.get("user"))
                deque_window["sub_ts"].append(submsg.get("ts"))
                deque_window["sub_text"].append(self.get_text(submsg))
                deque_window["sub_reactions"].append(self.get_reactions(submsg))
                msg_counter += 1
                #create Thread tuple
                self.threads.append(
                    Thread(channel=top.channel, channel_name=top.channel_name,
                       user=top.user, ts=top.ts, text=top.text, reactions=top.reactions,
                       sub_user=list(deque_window["sub_user"]),
                       sub_ts=list(deque_window["sub_ts"]),
                       sub_text=list(deque_window["sub_text"]),
                       sub_reactions=list(deque_window["sub_reactions"]),
                       msg_counter=msg_counter
                       )
                )
                processed_ids.append(submsg_index)

    def process_threads(self):
        self.index_threads()
        self.rip_threads()






def _extract_slack_link_id(m):
    return m.group('id')


def normalize_links(text):
    return re_slack_link.sub(_extract_slack_link_id, text)


if __name__ == '__main__':
    dir = '../input/export_Feb_8_2018'
    BASE_DIR = Path("/home/algis/repos/personal/MOOC/ODS_dump/input/export_Feb_8_2018")

    loader = SlackLoader2(dir, exclude_channels=[], only_channels=['_jobs'])
    print(len(loader.messages))
    print(len(loader.threads))
    type(loader.threads)
