from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import RegexpTokenizer


class tokenize:
    def __init__(self):
        self.stop_word = set(stopwords.words("english"))
        self.tokenizer = RegexpTokenizer(r'[^a-zA-Z]', gaps=True)

    def tokenize_text(self, f_text):
        _terms = self.tokenizer.tokenize(text=f_text)
        _terms = [term.casefold() for term in _terms]
        _terms = [PorterStemmer().stem(term) for term in _terms]
        _terms = [term for term in _terms if term not in self.stop_word]
        return _terms
