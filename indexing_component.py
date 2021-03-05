import math
# from memory_profiler import profile
from pathlib import Path
import multiprocessing as mp

from indexing_SPIMI import indexing_SPIMI
from indexing_get_tokens import indexing_get_tokens


class indexing_component:
    def __init__(self, f_sort_dir, f_out_dir, f_block_size=666, f_whether_multi_pro=False):
        self.sort_dir = f_sort_dir
        self.out_dir = f_out_dir
        self.debug = False
        self.doc_num = 100 if self.debug else len(list(Path(self.sort_dir).iterdir()))
        self.block_size = 20 if self.debug else f_block_size
        self.get_tokens = indexing_get_tokens(f_sort_dir=f_sort_dir)
        self.spimi = indexing_SPIMI(f_output_dir=self.out_dir)
        self.multi_pro = f_whether_multi_pro

    # @profile
    # @profile(precision=4, stream=open("./docs/perf/single_block_memory.log", 'w+'))
    def process_one_block(self, f_start_id, f_end_id):
        _tokens = self.get_tokens.reading_files(f_start_id=f_start_id, f_end_id=f_end_id)
        _sored_terms_dict = self.spimi.spimi_invert(f_token_stream=_tokens)
        return _sored_terms_dict

    # @profile
    # @profile(precision=4, stream=open("./docs/perf/multi_block_memory.log", 'w+'))
    def process_multiple_block(self, f_core_num):
        # print("[INFO] There are %d documents" % self.doc_num)
        _loop_num = int(self.doc_num / self.block_size)
        _whether_remains = self.doc_num % self.block_size != 0
        _sub_process_num = min(_loop_num + _whether_remains, f_core_num)
        _pool = mp.Pool(_sub_process_num)
        _merge_layers = math.ceil(math.log2(_loop_num))
        _dic_lists = []
        for _block_idx in range(_loop_num):
            # print("[INFO] Block Id %d" % _block_idx)
            if self.multi_pro:
                _dic_lists.append(_pool.apply_async(func=self.process_one_block,
                                                    args=(_block_idx * self.block_size,
                                                          (_block_idx + 1) * self.block_size - 1,)
                                                    ))
            else:
                _dic_lists.append(self.process_one_block(f_start_id=_block_idx * self.block_size,
                                                         f_end_id=(_block_idx + 1) * self.block_size - 1))
        if _whether_remains:
            # print("[INFO] processing remaining documents.")
            if self.multi_pro:
                _dic_lists.append(_pool.apply_async(func=self.process_one_block,
                                                    args=(_loop_num * self.block_size,
                                                          self.doc_num - 1,)
                                                    ))
            else:
                _dic_lists.append(self.process_one_block(f_start_id=_loop_num * self.block_size,
                                                         f_end_id=self.doc_num - 1))
        if self.multi_pro:
            _pool.close()
            _pool.join()
            _dic_lists = [_dic.get() for _dic in _dic_lists]
        while len(_dic_lists) != 1:
            _dic_lists.append(self.spimi.merge_two_blocks(_dic_1=_dic_lists.pop(0), _dic_2=_dic_lists.pop(0)))
        # _sorted_dic = dict(sorted(_dic_lists[0].items()))
        _sorted_dic = _dic_lists[0]
        return _sorted_dic

    def process_multiple_block_and_write_outputs(self, f_core_num):
        _dic = self.process_multiple_block(f_core_num)
        f = open(self.out_dir + "/term_doc.txt", "w")
        for k, v in _dic.items():
            f.write(str(k) + ' ' + str(v) + '\n')
        f.close()


if __name__ == '__main__':
    import cProfile
    import pstats

    profiler = cProfile.Profile()
    _rpt_timing = False
    _blk_size = 600
    _core_num = mp.cpu_count()
    _whether_multi_processor = True
    sort_dir = "./docs/HillaryEmails"
    indexing = indexing_component(f_sort_dir=sort_dir, f_out_dir="./docs/output",
                                  f_block_size=_blk_size, f_whether_multi_pro=_whether_multi_processor)
    if _rpt_timing:
        profiler.enable()
    indexing.process_multiple_block_and_write_outputs(f_core_num=_core_num)
    # dic = indexing.process_multiple_block()
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
