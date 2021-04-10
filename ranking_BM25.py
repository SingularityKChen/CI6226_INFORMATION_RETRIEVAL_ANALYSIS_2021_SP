from math import log10
from re import split
from pathlib import Path
from utils import tokenize


class ranking_BM25(tokenize):
    def __init__(self, f_k1, f_b, f_n, f_index_dir, f_length_filename):
        """

        :param f_k1:
        :param f_b:
        :param f_n: number of documents in the database
        """
        super(ranking_BM25, self).__init__()
        self.k = f_k1
        self.b = f_b
        self.n = f_n
        self.tf_td = {}
        self.l_d = {}
        self.l_avg = 0
        self.doc_len_file = f_length_filename
        self.index_dir = f_index_dir
        self.init_doc_length()
        self.init_doc_avg_len()

    def compute_idf(self, f_posting_list):
        """
        Compute the idf of term in the document d
        :return:
        """
        if f_posting_list:
            return log10(self.n / len(f_posting_list))
        else:
            return 0

    def compute_tf_td(self, f_term, f_doc_id):
        """
        Compute term frequency in document d
        :return: term frequency in document d
        """
        _doc = Path(self.index_dir) / ("%d.txt" % f_doc_id)
        _doc = _doc.open().read()
        _terms = self.tokenize_text(f_text=_doc)
        print(_terms)
        return _terms.count(f_term)

    def init_doc_length(self):
        """
        Compute the length of doc d
        :return: the length of doc d
        """
        _doc_len_f = open(self.doc_len_file)
        for _line in _doc_len_f.readlines():
            _line = _line.strip()
            _doc_id, _doc_len = split(" ", _line)
            self.l_d[int(_doc_id)] = int(_doc_len)

    def init_doc_avg_len(self):
        """
        Compute the average doc length in collection
        :return: the average doc length in collection
        """
        self.l_avg = sum(self.l_d.values()) / self.n

    def get_score(self):
        """
        Compute the BM25 score
        :return:
        """
        pass


if __name__ == '__main__':
    sort_dir = "./docs/HillaryEmails"
    doc_length_filename = "./docs/output/document_length.txt"
    k1 = 0.5
    b = 0.5
    n = len(list(Path(sort_dir).iterdir()))
    bm25 = ranking_BM25(f_k1=k1, f_b=b, f_n=n, f_index_dir=sort_dir, f_length_filename=doc_length_filename)
