import numpy as np
import logging
from langdetect import detect_langs
from langdetect.lang_detect_exception import LangDetectException

from configs.settings import LANGUAGE_PROPABILITY_TRESHOLD


def set_not_english_columns_to_null(df):
    def english_text_detection(row):
        potential_languages_propabilities = []
        try:
            potential_languages_propabilities.extend(detect_langs(row["header"]))
        except LangDetectException:
            pass
        except Exception as e:
            logging.error("Uncatched exception in language detection.", e)

        try:
            potential_languages_propabilities.extend(detect_langs(row["body"]))
        except LangDetectException:
            pass
        except Exception as e:
            logging.error("Uncatched exception in language detection.", e)

        is_en = False
        for language in potential_languages_propabilities:
            if language.lang == 'en' and language.prob > LANGUAGE_PROPABILITY_TRESHOLD:
                is_en = True
                break

        if is_en:
            return row["header"]
        return np.nan

    df["header"] = df.apply(english_text_detection, axis=1)

    df.loc[df["header"].notnull(), "body"] = df["body"]
    df.loc[df["header"].isnull(), "body"] = np.nan
