import pandas as pd
import numpy as np
import logging
import re

from langdetect import detect_langs
from langdetect.lang_detect_exception import LangDetectException
from typing import List
from nltk import word_tokenize
from nltk.stem.lancaster import LancasterStemmer


from configs.settings import LANGUAGE_PROPABILITY_TRESHOLD


def get_tokens_from_pattern(pattern: str) -> List[str]:
    stemmer = LancasterStemmer()
    alphanumerical_pattern = re.sub(r'[^a-zA-Z0-9 ]', '', pattern).lower()
    words = word_tokenize(alphanumerical_pattern)
    words = [stemmer.stem(word) for word in words]
    return words


def set_not_english_columns_to_null(df):
    def text_cleanup(row):
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
            header = " ".join(get_tokens_from_pattern(pattern=row["header"]))
            body = " ".join(get_tokens_from_pattern(pattern=row["body"]))
            return pd.Series([header, body])

        return pd.Series([np.nan, np.nan])

    df[["header", "body"]] = df.apply(text_cleanup, axis=1)
