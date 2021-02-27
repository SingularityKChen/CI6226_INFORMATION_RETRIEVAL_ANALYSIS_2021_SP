import re
import string
from pathlib import Path
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer


class indexing_get_tokens:
    def __init__(self, f_sort_dir):
        self.sort_dir = Path(f_sort_dir)
        self.stop_word = set(stopwords.words("english"))
        self.tokenizer = RegexpTokenizer(r'\s+', gaps=True)
        self.debug = True

    def reading_files(self, f_start_id, f_end_id):
        _tokens = []
        for doc_id in range(f_start_id + 1, f_end_id + 1):
            file_name = self.sort_dir / ("%d.txt" % doc_id)
            print("[INFO] current Path %s" % file_name)
            email = file_name.open().read()
            # print("[INFO] Doc is", email)
            _terms = self.tokenizer.tokenize(text=email)
            # print("[INFO] Original terms without compression:\n", _terms)
            _terms = [term for term in _terms if term not in string.punctuation]
            # print("[INFO] After removing punctuation:\n", _terms)
            _terms = [term.casefold() for term in _terms]
            # print("[INFO] After case folding:\n", _terms)
            _terms = [PorterStemmer().stem(term) for term in _terms]
            # print("[INFO] After stem:\n", _terms)
            _terms = [term for term in _terms if term not in self.stop_word]
            # print("[INFO] After removing stop words:\n", _terms)
            _terms = [term for term in _terms if not term.replace(",", "").replace(".", "").isdigit()]
            # print("[INFO] After removing numbers:\n", _terms)
            _token_doc_id = [(term, doc_id) for term in _terms]
            _tokens.extend(_token_doc_id)
        return _tokens


if __name__ == '__main__':
    sort_dir = "./docs/HillaryEmails"
    get_tokens = indexing_get_tokens(f_sort_dir=sort_dir)
    tokens = get_tokens.reading_files(f_start_id=0, f_end_id=5)
    print("[INFO] There are %d tokens" % len(tokens))
