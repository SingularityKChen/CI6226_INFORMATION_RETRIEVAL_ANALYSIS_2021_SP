import math
# from memory_profiler import profile
from pathlib import Path

from indexing_SPIMI import indexing_SPIMI
from indexing_get_tokens import indexing_get_tokens


class indexing_component:
    def __init__(self, f_sort_dir, f_block_size=666):
        self.sort_dir = f_sort_dir
        self.debug = False
        self.doc_num = 100 if self.debug else len(list(Path(self.sort_dir).iterdir()))
        self.block_size = 20 if self.debug else f_block_size
        self.get_tokens = indexing_get_tokens(f_sort_dir=f_sort_dir)
        self.spimi = indexing_SPIMI(f_output_dir="")

    # @profile
    # @profile(precision=4, stream=open("./docs/perf/single_block_memory.log", 'w+'))
    def process_one_block(self, f_start_id, f_end_id):
        _tokens = self.get_tokens.reading_files(f_start_id=f_start_id, f_end_id=f_end_id)
        _sored_terms, _dictionary = self.spimi.spimi_invert(f_token_stream=_tokens)
        # print("[INFO] sorted terms:\n", _sored_terms)
        # print("\tdictionary:\n", _dictionary)
        # print("[INFO] There are %d terms" % len(_dictionary))
        return _dictionary

    # @profile
    # @profile(precision=4, stream=open("./docs/perf/multi_block_memory.log", 'w+'))
    def process_multiple_block(self):
        # print("[INFO] There are %d documents" % self.doc_num)
        _loop_num = int(self.doc_num / self.block_size)
        _merge_layers = math.ceil(math.log2(_loop_num))
        _dic_lists = []
        for _block_idx in range(_loop_num):
            # print("[INFO] Block Id %d" % _block_idx)
            _dic_lists.append(self.process_one_block(f_start_id=_block_idx * self.block_size,
                                                     f_end_id=(_block_idx + 1) * self.block_size - 1))
        if self.doc_num % self.block_size != 0:
            # print("[INFO] processing remaining documents.")
            _dic_lists.append(self.process_one_block(f_start_id=_loop_num * self.block_size, f_end_id=self.doc_num - 1))
        while len(_dic_lists) != 1:
            _dic_lists.append(self.spimi.merge_two_blocks(_dic_1=_dic_lists.pop(0), _dic_2=_dic_lists.pop(0)))
        return _dic_lists[0]


if __name__ == '__main__':
    import cProfile
    import pstats
    profiler = cProfile.Profile()
    _rpt_timing = False
    _blk_size = 600
    sort_dir = "./docs/HillaryEmails"
    indexing = indexing_component(f_sort_dir=sort_dir, f_block_size=_blk_size)
    if _rpt_timing:
        profiler.enable()
    dic = indexing.process_multiple_block()
    if _rpt_timing:
        profiler.disable()
        stats = pstats.Stats(profiler)
        stats.sort_stats("cumulative")
        stats.print_stats()
        stats.print_title()
        stats.print_callers()
        stats.print_callees()
        stats.dump_stats(filename="./docs/perf/timing_blk_%d.stats" % _blk_size)
    # print(sorted(dic))
    # print(len(dic))
