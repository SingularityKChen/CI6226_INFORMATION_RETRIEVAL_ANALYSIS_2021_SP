from math import log2, ceil, floor


class indexing_SPIMI:
    def __init__(self, f_output_dir):
        self.output_dir = f_output_dir
        self.file_name_format = "block_%d_%d.txt"

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
        return dict(sorted(f_dictionary.items()))

    def _write_block_to_disk(self, f_dictionary, f_block_id):
        f = open(self.output_dir + self.file_name_format % (0, f_block_id), "w")
        for k, v in f_dictionary.items():
            f.write(str(k) + ' ' + str(v) + '\n')
        f.close()
        print("[INFO] write %d-th block into %s" %
              (f_block_id, self.output_dir + self.file_name_format % (0, f_block_id)))

    def spimi_invert(self, f_token_stream, f_block_id):
        _dictionary = {}
        for token in f_token_stream:
            if token[0] in _dictionary:
                _postings_list = self._get_postings_list(f_dictionary=_dictionary, f_term=token[0])
            else:
                _postings_list = self._add_to_dictionary(f_dictionary=_dictionary, f_term=token[0])
            if token[1] not in _postings_list:
                _postings_list = self._add_to_postings_list(f_postings_list=_postings_list, f_document_id=token[1])
        # sort the dictionary
        _dictionary = self._sort_terms(f_dictionary=_dictionary)
        self._write_block_to_disk(f_dictionary=_dictionary, f_block_id=f_block_id)
        return _dictionary

    def try_merge_blocks(self, f_block_number):
        _tree_depth = ceil(log2(f_block_number))
        _last_dep_branch_num = f_block_number
        _last_dep_left = 0
        _left_block_file_list = []
        for _depth in range(_tree_depth):
            _cur_dep_branch_num = _last_dep_branch_num + _last_dep_left
            _cur_merge_num = floor(_cur_dep_branch_num / 2)
            _cur_dep_left = _cur_dep_branch_num % 2
            for _merge_idx in range(_cur_merge_num):
                _blk_1_filename = self.file_name_format % (_depth, _merge_idx * 2)
                _blk_2_filename = self.file_name_format % (_depth, _merge_idx * 2 + 1)
                if _last_dep_left and (not _cur_dep_left) and _merge_idx == (_cur_merge_num - 1):
                    _blk_2_filename = _left_block_file_list.pop()
                print("[INFO] Now merging %s with %s" % (_blk_1_filename, _blk_2_filename))
                self.merge_two_blocks_from_files(f_block_filename_1=_blk_1_filename,
                                                 f_block_filename_2=_blk_2_filename,
                                                 f_merged_filename=self.file_name_format % (_depth + 1, _merge_idx))
            if _cur_dep_left and (not _last_dep_left):
                _left_block_file_list.append(self.file_name_format % (_depth, _cur_merge_num * 2))
            # update the variables
            _last_dep_left = _cur_dep_left
            _last_dep_branch_num = _cur_merge_num

    def merge_two_blocks_from_files(self, f_block_filename_1, f_block_filename_2, f_merged_filename):
        _block_1 = open(self.output_dir + f_block_filename_1)
        _block_2 = open(self.output_dir + f_block_filename_2)
        _merged_block = open(self.output_dir + f_merged_filename, "w")
        _blk_1_eof = False
        _blk_2_eof = False
        _block_1_new = _block_1.readline()
        _block_2_new = _block_2.readline()
        while not (_blk_1_eof and _blk_2_eof):
            # check whether eof
            if not _block_1_new:
                _blk_1_eof = True
                _block_1.close()
            if not _block_2_new:
                _blk_2_eof = True
                _block_2.close()
            if _blk_1_eof and _blk_2_eof:
                break
            if _blk_1_eof:
                _block_2_new = _block_2_new.strip()
                _merged_block.write(_block_2_new + "\n")
                _block_2_new = _block_2.readline()
            elif _blk_2_eof:
                _block_1_new = _block_1_new.strip()
                _merged_block.write(_block_1_new + "\n")
                _block_1_new = _block_1.readline()
            else:
                # compare and merge
                _block_1_new = _block_1_new.strip()
                _block_2_new = _block_2_new.strip()
                _term_1, _post_1_str = _block_1_new.split(' [')
                _term_2, _post_2_str = _block_2_new.split(' [')
                _post_1_str = _post_1_str.strip(']')
                _post_2_str = _post_2_str.strip(']')
                if _term_1 < _term_2:
                    _merged_block.write(_block_1_new + "\n")
                    _block_1_new = _block_1.readline()
                elif _term_1 > _term_2:
                    _merged_block.write(_block_2_new + "\n")
                    _block_2_new = _block_2.readline()
                else:
                    if int(_post_1_str.split(", ")[0]) < int(_post_2_str.split(", ")[0]):
                        _new_post = "[" + _post_1_str + ", " + _post_2_str + "]"
                    else:
                        _new_post = "[" + _post_2_str + ", " + _post_1_str + "]"
                    _merged_block.write(_term_1 + ' ' + _new_post + '\n')
                    _block_1_new = _block_1.readline()
                    _block_2_new = _block_2.readline()
        _merged_block.close()

    def merge_two_blocks(self, _dic_1, _dic_2):
        """

        :param _dic_1: dictionary to be updated
        :type _dic_1: dict
        :param _dic_2: dictionary to update with
        :type _dic_2:dict
        :return: the merged dictionary
        """
        combined_keys = _dic_1.keys() | _dic_2.keys()
        _dic = {}
        for key in combined_keys:
            _dic_1_v = _dic_1.get(key, [])
            _dic_2_v = _dic_2.get(key, [])
            if _dic_1_v and _dic_2_v and (_dic_1_v[0] > _dic_2_v[0]):
                _dic[key] = _dic_2_v + _dic_1_v
            else:
                _dic[key] = _dic_1_v + _dic_2_v
            # should sort here
        return self._sort_terms(_dic)


if __name__ == '__main__':
    out_dir = "./docs/output/"
    block_size = 600
    doc_num = 7945
    loop_num = int(doc_num / block_size)
    whether_remains = doc_num % block_size != 0
    block_num = loop_num + whether_remains
    spimi = indexing_SPIMI(f_output_dir=out_dir)
    spimi.try_merge_blocks(f_block_number=block_num)
