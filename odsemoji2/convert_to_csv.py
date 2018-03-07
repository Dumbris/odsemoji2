from pathlib import Path
import pandas as pd
import numpy as np
import csv
from odsemoji2.utils.slack_data_loader2 import SlackLoader2
from odsemoji2.utils.ktop_reactions import get_ktop_reactions
from sklearn.model_selection import train_test_split
from imblearn.datasets import make_imbalance
from imblearn import over_sampling as os
from imblearn.under_sampling import RandomUnderSampler

def loadMessages(base_dir):
    sl = SlackLoader2(base_dir, exclude_channels=[])#, only_channels=["_call_4_collaboration"])
    sl.load_messages()
    return sl.messages
    #sl.process_threads()
    #return sl.threads

if __name__ == '__main__':
    #BASE_DIR = Path("../input/export_Feb_8_2018")
    BASE_DIR = Path("/home/algis/repos/personal/MOOC/ODS_dump2/input/export_Feb_8_2018")
    REACTION_THRESHOLD  = 2
    COL_PREFIX          = "rn_"
    MSG_LEN_LIMIT       = 30

    msgs = loadMessages(BASE_DIR)
    df = pd.DataFrame(msgs).set_index(["channel_name", "ts"])
    print(df.shape[0])
    #filter replies
    df = df[df["parent_user_id"].isnull()]
    df = df[df["text_"].str.len() > MSG_LEN_LIMIT]
    #filter by subtype
    df_top_messages = df[~df["subtype"].isin(['bot_message', 'channel_join', 'channel_leave'])]
    df_top_messages["reactions"] = np.where(df_top_messages["reactions_"].isnull(),
                                            {"no_reaction":3},
                                            df_top_messages["reactions_"]
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
    df_top_messages["text_"] = df_top_messages["text_"].str.replace(r'\n', '')
    FILE_NAME = "ods_reacts.txt"
    print(df_top_messages.shape[0])
    #smote = os.SMOTE(random_state=RANDOM_STATE)
    # Apply the random under-sampling
    rus = RandomUnderSampler(return_indices=True)
    X = df_top_messages.reset_index()["text_"].values.astype("U")
    y = df_top_messages.reset_index()["reactions_labels"].values.astype("U")
    X_resampled, y_resampled, idx_resampled = rus.fit_sample(X, y)
    #X, y = make_imbalance(df_top_messages["text_"].values, df_top_messages["reactions_labels"].values, ratio='auto')

    print(X_resampled.shape)

    X_train, X_test, y_train, y_test = train_test_split(X_resampled, y_resampled, test_size=0.15)
    np.savetxt(FILE_NAME+".train", np.vstack([y_train,X_train]).transpose(), delimiter=' ', fmt="%s")
    np.savetxt(FILE_NAME+".test", np.vstack([y_test,X_test]).transpose(), delimiter=' ', fmt="%s")
