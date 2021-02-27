from indexing_get_tokens import indexing_get_tokens
from indexing_SPIMI import indexing_SPIMI
from pathlib import Path


class indexing_component:
    def __init__(self, f_sort_dir, f_block_size=666):
        self.sort_dir = f_sort_dir
        self.doc_num = len(list(Path(self.sort_dir).iterdir()))
        self.block_size = f_block_size
        self.get_tokens = indexing_get_tokens(f_sort_dir=f_sort_dir)
        self.spimi = indexing_SPIMI(f_output_dir="")

    def process_one_block(self, f_start_id, f_end_id):
        _tokens = self.get_tokens.reading_files(f_start_id=f_start_id, f_end_id=f_end_id)
        _sored_terms, _dictionary = self.spimi.spimi_invert(f_token_stream=_tokens)
        print("[INFO] sorted terms:\n", _sored_terms)
        print("\tdictionary:\n", _dictionary)
        print("[INFO] There are %d terms" % len(_dictionary))

    def process_multiple_block(self):
        print("[INFO] There are %d documents" % self.doc_num)
        _loop_num = int(self.doc_num / self.block_size)
        for _block_idx in range(_loop_num):
            print("[INFO] Block Id %d" % _block_idx)
            self.process_one_block(f_start_id=_block_idx * self.block_size,
                                   f_end_id=(_block_idx + 1) * self.block_size - 1)
        if self.doc_num % self.block_size != 0:
            print("[INFO] processing remaining documents.")
            self.process_one_block(f_start_id=_loop_num * self.block_size, f_end_id=self.doc_num - 1)


if __name__ == '__main__':
    sort_dir = "./docs/HillaryEmails"
    indexing = indexing_component(f_sort_dir=sort_dir, f_block_size=666)
    indexing.process_multiple_block()
