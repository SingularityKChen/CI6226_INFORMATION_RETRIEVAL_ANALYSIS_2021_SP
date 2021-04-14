from math import log10
from re import split
from pathlib import Path
from utils import tokenize
from multiprocessing import Pool, cpu_count


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

    def compute_idf(self, f_term_freq):
        """
        Compute the idf of this term in the documents

        :param f_term_freq: the number of documents that contains this term
        :return: the IDF (inverse document frequency) weight of this query term
        """
        if f_term_freq:
            return log10(self.n / f_term_freq)
        else:
            return 0

    @staticmethod
    def compute_tf_td(f_term, f_terms_in_doc):
        """
        Compute this term frequency in document d
        :type f_term: str
        :type f_terms_in_doc: list[str]
        :return: term frequency in document d
        """
        return f_terms_in_doc.count(f_term)

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

    def get_terms_in_the_doc(self, f_doc_id):
        _doc = Path(self.index_dir) / ("%d.txt" % f_doc_id)
        _doc = _doc.open().read()
        return self.tokenize_text(f_text=_doc)

    def get_score(self, f_terms, f_term_freq_list, f_posting_list):
        """
        Compute the BM25 score
        :type f_term_freq_list: list[int]
        :type f_terms: list
        :type f_posting_list: list[int]
        :return:
        """
        _doc_score = {}
        _pool = Pool(cpu_count())
        _terms_in_docs = _pool.map(self.get_terms_in_the_doc, f_posting_list)
        for _doc_idx, _doc_id in enumerate(f_posting_list):
            print("[INFO] Doc Id %d length %d" % (_doc_id, self.l_d[_doc_id]))
            _score = 0
            for _term_idx, _term in enumerate(f_terms):
                _tf_df = self.compute_tf_td(f_term=_term, f_terms_in_doc=_terms_in_docs[_doc_idx])
                _numerator = (self.k + 1) * _tf_df
                _denominator = self.k * (1 - self.b + self.b * self.l_d[_doc_id] / self.l_avg) + _tf_df
                _score += self.compute_idf(f_term_freq_list[_term_idx]) * _numerator / _denominator
            _doc_score[_doc_id] = _score
        return _doc_score


if __name__ == '__main__':
    sort_dir = "./docs/HillaryEmails"
    doc_length_filename = "./docs/output/document_length.txt"
    k1 = 1.2
    b = 0.75
    n = len(list(Path(sort_dir).iterdir()))
    bm25 = ranking_BM25(f_k1=k1, f_b=b, f_n=n, f_index_dir=sort_dir, f_length_filename=doc_length_filename)
