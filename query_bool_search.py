from re import split, findall
from sys import getsizeof

import pandas as pd
from nltk.stem import PorterStemmer
from numpy import int32, array
from ranking_BM25 import ranking_BM25


class query_bool_search:
    def __init__(self, f_term_filename, f_compression, f_jump_ptr, f_n, f_index_dir, f_length_filename):
        """

        :param f_term_filename: the file that contains the term-doc pairs
        :type f_term_filename: str
        :param f_compression: whether compress the indexing
        :type f_compression: bool
        :param f_jump_ptr: ture to jump the pointer when searching the indexing
        :type f_jump_ptr: bool
        """
        self.compression = f_compression
        self.jump_ptr = f_jump_ptr
        self.term_str = ""
        self.term_doc_pair = self.read_term_doc(f_term_filename=f_term_filename)
        self.half_dic_size = int(self.term_doc_pair.size / 4)
        self.bm25 = ranking_BM25(f_k1=1.2, f_b=0.75, f_n=f_n,
                                 f_index_dir=f_index_dir, f_length_filename=f_length_filename)

    def print_mem_util(self):
        print("[INFO] Current dictionary size is \n%s\n and string size is %d bytes" %
              (self.term_doc_pair.memory_usage(index=False, deep=True), getsizeof(self.term_str)))

    # @profile
    def do_query(self, f_query, f_whether_rank):
        print("[INFO] Your query is \"%s\"" % f_query)
        _operation = self.get_search_operations(f_query=f_query)
        _query_terms = self.get_query_terms(f_query=f_query)
        _posting_list = self.get_posting_lists(f_terms=_query_terms)
        _query_results = self.boolean_operation(f_operation=_operation, f_posting_list=_posting_list)
        if f_whether_rank:
            _ranking_scores = self.ranking(f_terms=_query_terms, f_postings=_posting_list, f_results=_query_results)
            print("[INFO] The doc id with scores", _ranking_scores)
            return list(map(lambda x: x[0], sorted(_ranking_scores.items(), key=lambda kv: kv[1], reverse=True)))
        else:
            return sorted(_query_results)

    @staticmethod
    def boolean_operation(f_operation, f_posting_list):
        if f_operation == 0:
            print("[INFO] AND operation")
            _query_results = set.intersection(*map(set, f_posting_list))
        elif f_operation == 1:
            _query_results = set.union(*map(set, f_posting_list))
        else:
            _query_results = set(f_posting_list[0]).difference(*map(set, f_posting_list[1:]))
        return _query_results

    def get_term_from_dic_ptr(self, f_dic_ptr):
        _term_ptr = self.term_doc_pair.loc[f_dic_ptr, "terms"]
        return self.get_term_from_term_ptr(f_term_ptr=_term_ptr)

    def get_term_from_term_ptr(self, f_term_ptr):
        _term_len_str = findall(r"^\d+", self.term_str[f_term_ptr:f_term_ptr + 3])[0]
        _term_len = int32(_term_len_str)
        f_term_ptr += len(_term_len_str)
        _term_str = self.term_str[f_term_ptr:f_term_ptr + _term_len]
        return _term_str

    # @profile
    def get_posting_lists(self, f_terms):
        """

        :param f_terms: the query terms
        :type f_terms: list[str]
        :return:
        """
        _posting_sets_list = []
        if self.compression:
            if self.jump_ptr:
                for _query_term_str in f_terms:
                    # print("[INFO] Query term %s:" % _query_term_str)
                    _dic_ptr = 0
                    _ptr_gap = self.half_dic_size
                    _term_str = self.get_term_from_dic_ptr(f_dic_ptr=_dic_ptr)
                    while _term_str != _query_term_str:
                        # print("\t\t dic_ptr %d, looking term %s" % (_dic_ptr, _term_str))
                        if _term_str < _query_term_str:
                            _dic_ptr += _ptr_gap
                            _ptr_gap = int(_ptr_gap / 2)
                        elif _term_str > _query_term_str:
                            _dic_ptr -= _ptr_gap
                            _ptr_gap = int(_ptr_gap / 2)
                        _term_str = self.get_term_from_dic_ptr(f_dic_ptr=_dic_ptr)
                    _posting_sets_list.append(self.term_doc_pair.loc[_dic_ptr, "posting"])
            else:
                for _idx, _key_row in self.term_doc_pair.iterrows():
                    if not f_terms:
                        break
                    _term_ptr = _key_row["terms"]
                    _post_list = _key_row["posting"]
                    _term_str = self.get_term_from_term_ptr(f_term_ptr=_term_ptr)
                    if _term_str in f_terms:
                        _posting_sets_list.append(_post_list)
                        f_terms.remove(_term_str)
        else:
            _posting_sets_list = list(self.term_doc_pair.loc[self.term_doc_pair["terms"].isin(f_terms)]["posting"])
        return list(map(list, _posting_sets_list))

    @staticmethod
    def get_query_terms(f_query):
        """
        To separate the query into terms.
        And a query “horse car phone” will be treated as "horse" "car"  "phone"

        :param f_query: the query string.
        :type f_query: str
        :return:
        """
        _query_terms = split("AND| |OR|NOT", f_query)
        _query_terms = [_term for _term in _query_terms if _term != ""]
        _query_terms = [PorterStemmer().stem(term) for term in _query_terms]
        print("[INFO] The query terms are: %s" % _query_terms)
        return _query_terms

    @staticmethod
    def get_search_operations(f_query):
        """
        Check what operation is in the query.
        Return the operation. 0 AND, 1 OR, 2 NOT
        :param f_query:
        :return:
        """
        _operation = 0
        if "OR" in f_query:
            _operation = 1
            print("[INFO] OR operation")
        elif "NOT" in f_query:
            _operation = 2
            print("[INFO] NOT operation")
        return _operation

    # @profile
    def compress_term_prt(self, f_term):
        _cur_term_ptr = len(self.term_str)
        _term_len_str = str(len(f_term))
        self.term_str += _term_len_str + f_term
        return _cur_term_ptr

    # @profile
    def read_term_doc(self, f_term_filename):
        _term_doc_dic = pd.read_csv(f_term_filename, header=None, sep=' \[|]',
                                    names=["terms", "posting", "empty"], dtype=str, engine='python')
        _term_doc_dic.drop(["empty"], axis=1, inplace=True)
        _term_doc_dic["posting"] = _term_doc_dic["posting"].apply(lambda x: array(map(int32, x.split(', '))))
        if self.compression:
            _term_doc_dic["terms"] = \
                _term_doc_dic["terms"].apply(lambda x: self.compress_term_prt(str(x))).astype(int32)
        return _term_doc_dic

    def ranking(self, f_terms, f_postings, f_results):
        _term_freq_list = [len(x) for x in f_postings]
        return self.bm25.get_score(f_terms=f_terms, f_posting_list=f_results, f_term_freq_list=_term_freq_list)


if __name__ == '__main__':
    _whether_jump_prt = True
    doc_length_filename = "./docs/output/document_length.txt"
    term_doc_pair_filename = "docs/output/block_4_0.txt"
    sort_dir = "./docs/HillaryEmails"
    for whether_compression in [True, False]:
        for whether_rank in [True, False]:
            query_str = "Islamophobia President Obama"
            # query_str = "horse OR rupert"
            # query_str = "horse car zalem"
            # query_str = "friend NOT fun"
            query = query_bool_search(f_term_filename=term_doc_pair_filename,
                                      f_compression=whether_compression,
                                      f_jump_ptr=_whether_jump_prt,
                                      f_n=7945, f_index_dir=sort_dir, f_length_filename=doc_length_filename)
            query_results = query.do_query(f_query=query_str, f_whether_rank=whether_rank)
            print("[INFO] Query %s Results are:" % ("ranked" if whether_rank else "unranked"))
            print(query_results)
            if whether_compression:
                print("[INFO] Currently, the indexing is compressed.")
            else:
                print("[INFO] Currently, no compression for the indexing.")
            query.print_mem_util()
