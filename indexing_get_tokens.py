from pathlib import Path

from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import RegexpTokenizer


class indexing_get_tokens:
    def __init__(self, f_sort_dir, f_length_filename):
        """

        :param f_sort_dir: the directory to read the files
        :param f_length_filename: the name of the document length file that will generated
        """
        self.sort_dir = Path(f_sort_dir)
        self.stop_word = set(stopwords.words("english"))
        self.tokenizer = RegexpTokenizer(r'[^a-zA-Z]', gaps=True)
        self.length_filename = f_length_filename

    def tokenize_text(self, f_text):
        _terms = self.tokenizer.tokenize(text=f_text)
        _terms = [term.casefold() for term in _terms]
        _terms = [PorterStemmer().stem(term) for term in _terms]
        _terms = [term for term in _terms if term not in self.stop_word]
        return _terms

    def reading_files(self, f_start_id, f_end_id):
        """
        Read the documents and process the terms, also write the document length file.
        :param f_start_id: the start id of the document under processing
        :param f_end_id: the end id of the document under processing
        :return: the token id pairs
        """
        _tokens = []
        _doc_len_f = open(self.length_filename, "a")
        for doc_id in range(f_start_id + 1, f_end_id + 1):
            file_name = self.sort_dir / ("%d.txt" % doc_id)
            email = file_name.open().read()
            _terms = self.tokenize_text(f_text=email)
            _token_doc_id = [(term, doc_id) for term in _terms]
            _tokens.extend(_token_doc_id)
            print("%d %d" % (doc_id, len(_terms)), file=_doc_len_f)
        _doc_len_f.close()
        return _tokens


if __name__ == '__main__':
    sort_dir = "./docs/HillaryEmails"
    doc_length_filename = "./docs/output/document_length.txt"
    get_tokens = indexing_get_tokens(f_sort_dir=sort_dir, f_length_filename=doc_length_filename)
    Path(doc_length_filename).unlink(missing_ok=True)
    tokens = get_tokens.reading_files(f_start_id=0, f_end_id=5)
    print("[INFO] There are %d tokens" % len(tokens))
