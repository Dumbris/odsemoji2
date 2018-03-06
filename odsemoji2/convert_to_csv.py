from pathlib import Path
import pandas as pd
import numpy as np
import csv
from odsemoji2.utils.slack_data_loader2 import SlackLoader2
from odsemoji2.utils.ktop_reactions import get_ktop_reactions

def loadThreads(base_dir):
    sl = SlackLoader2(base_dir, exclude_channels=[])#, only_channels=["_call_4_collaboration"])
    sl.load_messages()
    sl.process_threads()
    return sl.threads

if __name__ == '__main__':
    #BASE_DIR = Path("../input/export_Feb_8_2018")
    BASE_DIR = Path("/home/algis/repos/personal/MOOC/ODS_dump2/input/export_Feb_8_2018")
    REACTION_THRESHOLD = 2
    COL_PREFIX="rn_"

    threads = loadThreads(BASE_DIR)
    df = pd.DataFrame(threads).set_index(["channel_name", "ts"])
    df_top_messages = df[df["sub_text"].isnull()]
    df_top_messages["reactions"] = np.where(df_top_messages["reactions"].isnull(),
                                            {"no_reaction":3},
                                            df_top_messages["reactions"]
                                            )
    _, reacts_list, reacts_stats = get_ktop_reactions(df_top_messages, reactions_col="reactions", COL_PREFIX=COL_PREFIX)

    def format_reaction(xs):
        res = []
        for key, val in xs.items():
            key_ = COL_PREFIX + key
            if key_ not in reacts_list:
                continue
            if val < REACTION_THRESHOLD:
                continue
            res.append("__label__{}".format(key))
        if len(res) == 0:
            res.append("__label__{}".format("no_reaction"))
        return " ".join(res)

    df_top_messages["reactions_labels"] = df_top_messages["reactions"].apply(format_reaction)
    df_top_messages["text_"] = df_top_messages["text"].str.replace(r'\n', '')
    df_top_messages[["reactions_labels", "text_"]].to_csv('all_text_reacts.txt', header=None, index=None, sep=' ', quoting=csv.QUOTE_MINIMAL)
