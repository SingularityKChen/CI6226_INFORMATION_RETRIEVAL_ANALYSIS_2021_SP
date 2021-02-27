

class indexing_SPIMI:
    def __init__(self, f_output_dir):
        self.output_dir = f_output_dir

    @staticmethod
    def _add_to_dictionary(f_dictionary, f_term):
        f_dictionary[f_term] = []
        return f_dictionary[f_term]

    @staticmethod
    def _get_postings_list(f_dictionary, f_term):
        return f_dictionary[f_term]

    @staticmethod
    def _add_to_postings_list(f_postings_list, f_document_id):
        return f_postings_list.append(f_document_id)

    @staticmethod
    def _sort_terms(f_dictionary):
        return [f_term for f_term in sorted(f_dictionary)]

    def _write_block_to_disk(self, f_sorted_terms, f_dictionary):
        print("[INFO] write terms into", self.output_dir)

    def spimi_invert(self, f_token_stream):
        _dictionary = {}
        for token in f_token_stream:
            if token[0] in _dictionary:
                _postings_list = self._get_postings_list(f_dictionary=_dictionary, f_term=token[0])
            else:
                _postings_list = self._add_to_dictionary(f_dictionary=_dictionary, f_term=token[0])
            if token[1] not in _postings_list:
                _postings_list = self._add_to_postings_list(f_postings_list=_postings_list, f_document_id=token[1])
            # print("[INFO] posting list\n", _postings_list)
        _sorted_terms = self._sort_terms(f_dictionary=_dictionary)
        self._write_block_to_disk(f_sorted_terms=_sorted_terms, f_dictionary=_dictionary)
        return _sorted_terms, _dictionary
