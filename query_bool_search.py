from re import split, findall
from sys import getsizeof
import pandas as pd
from nltk.stem import PorterStemmer
from numpy import int32


class query_bool_search:
    def __init__(self, f_term_filename, f_compression):
        self.compression = f_compression
        self.term_str = ""
        self.term_doc_pair = self.read_term_doc(f_term_filename=f_term_filename)

    # @profile
    def do_query(self, f_query):
        print("[INFO] Your query is \"%s\"" % f_query)
        _operation = self.get_search_operations(f_query=f_query)
        _query_terms = self.get_query_terms(f_query=f_query)
        _posting_list = self.get_posting_lists(f_terms=_query_terms)
        _query_results = self.boolean_operation(f_operation=_operation, f_posting_list=_posting_list)
        print("[INFO] Current dictionary size is %d bytes and string size is %d bytes" %
              (getsizeof(self.term_doc_pair), getsizeof(self.term_str)))
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

    # @profile
    def get_posting_lists(self, f_terms):
        """

        :param f_terms:
        :type f_terms: list[str]
        :return:
        """
        _posting_sets_list = []
        if self.compression:
            for _idx, _key_row in self.term_doc_pair.iterrows():
                _term_ptr = _key_row["terms"]
                _post_list = _key_row["posting"]
                _term_len_str = findall(r"^\d+", self.term_str[_term_ptr:_term_ptr + 3])[0]
                _term_len = int32(_term_len_str)
                _term_ptr += len(_term_len_str)
                _term_str = self.term_str[_term_ptr:_term_ptr + _term_len]
                if _term_str in f_terms:
                    _posting_sets_list.append(_post_list)
                    f_terms.remove(_term_str)
        else:
            _posting_sets_list = list(self.term_doc_pair.loc[self.term_doc_pair["terms"].isin(f_terms)]["posting"])
        return _posting_sets_list

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
        _term_doc_dic["posting"] = _term_doc_dic["posting"].apply(lambda x: list(map(int32, x.split(', '))))
        if self.compression:
            _term_doc_dic["terms"] = \
                _term_doc_dic["terms"].apply(lambda x: self.compress_term_prt(str(x))).astype(int32)
        return _term_doc_dic


if __name__ == '__main__':
    whether_compression = True
    # query_str = "horse car"
    query_str = "friend NOT fun"
    term_doc_pair_filename = "docs/output/block_5_0.txt"
    query = query_bool_search(f_term_filename=term_doc_pair_filename, f_compression=whether_compression)
    query_results = query.do_query(f_query=query_str)
    print(query_results)
