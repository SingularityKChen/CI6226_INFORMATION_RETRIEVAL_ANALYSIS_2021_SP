import re
from pathlib import Path
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer


class indexing_get_tokens:
    def __init__(self, f_sort_dir, f_block_size=666):
        self.sort_dir = Path(f_sort_dir)
        self.block_size = f_block_size
        self.stop_word = set(stopwords.words("english"))
        # self.tokenizer = RegexpTokenizer(r'\s+', gaps=True)
        # self.tokenizer = RegexpTokenizer(r'\s+|[@&-]|\:', gaps=True)
        self.tokenizer = RegexpTokenizer(r'\s+|[\!\@\#\$\%\^\&\*\(\)\-\_\=\+\`\~\"\:\;\/\.\,\?\[\]\{\}\<\>]', gaps=True)
        self.debug = False

    def reading_files(self):
        _blocks_tokens = []
        _tokens = []
        for file_name in [self.sort_dir / "1.txt"] if self.debug else self.sort_dir.iterdir():
            document_id = int(re.split("\.|/", string=str(file_name)).pop(-2))
            print("[INFO] current Path %s" % file_name)
            email = file_name.open().read()
            # print("[INFO] Doc is", email)
            _terms = self.tokenizer.tokenize(text=email)
            # print("[INFO] Original terms without compression:\n", _terms)
            _terms = [term.casefold() for term in _terms]
            # print("[INFO] After case folding:\n", _terms)
            _terms = [PorterStemmer().stem(term) for term in _terms]
            # print("[INFO] After stem:\n", _terms)
            _terms = [term for term in _terms if term not in self.stop_word]
            # print("[INFO] After removing stop words:\n", _terms)
            _terms = [term for term in _terms if not term.replace(",", "").replace(".", "").isdigit()]
            # print("[INFO] After removing numbers:\n", _terms)
            _token_doc_id = [(term, document_id) for term in _terms]
            _tokens.extend(_token_doc_id)
            if document_id % self.block_size == 0:
                _blocks_tokens.append(_tokens)
                _tokens = []
        if _tokens:
            _blocks_tokens.append(_tokens)
        return _blocks_tokens


if __name__ == '__main__':
    sort_dir = "./docs/HillaryEmails"
    get_tokens = indexing_get_tokens(f_sort_dir=sort_dir)
    tokens = get_tokens.reading_files()
    print("[INFO] There are %d tokens from %d blocks" %
          (sum([len(block_token) for block_token in tokens]), len(tokens)))
