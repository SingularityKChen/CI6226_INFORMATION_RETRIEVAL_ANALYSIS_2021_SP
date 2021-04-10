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
        self.loop_num = int(self.doc_num / self.block_size)
        self.whether_remains = self.doc_num % self.block_size != 0

    # @profile
    # @profile(precision=4, stream=open("./docs/perf/single_block_memory.log", 'w+'))
    def process_one_block(self, f_start_id, f_end_id, f_block_id):
        _tokens = self.get_tokens.reading_files(f_start_id=f_start_id, f_end_id=f_end_id)
        self.spimi.spimi_invert(f_token_stream=_tokens, f_block_id=f_block_id)
        # _sored_terms_dict = self.spimi.spimi_invert(f_token_stream=_tokens, f_block_id=f_block_id)
        # return _sored_terms_dict

    # @profile
    # @profile(precision=4, stream=open("./docs/perf/multi_block_memory.log", 'w+'))
    def process_multiple_block(self, f_core_num):
        # print("[INFO] There are %d documents" % self.doc_num)
        _sub_process_num = min(self.loop_num + self.whether_remains, f_core_num)
        _pool = mp.Pool(_sub_process_num)
        for _block_idx in range(self.loop_num):
            # print("[INFO] Block Id %d" % _block_idx)
            if self.multi_pro:
                _pool.apply_async(func=self.process_one_block,
                                  args=(_block_idx * self.block_size,
                                        (_block_idx + 1) * self.block_size - 1,
                                        _block_idx,)
                                  )
            else:
                self.process_one_block(f_start_id=_block_idx * self.block_size,
                                       f_end_id=(_block_idx + 1) * self.block_size - 1,
                                       f_block_id=_block_idx)
        if self.whether_remains:
            # print("[INFO] processing remaining documents.")
            if self.multi_pro:
                _pool.apply_async(func=self.process_one_block,
                                  args=(self.loop_num * self.block_size,
                                        self.doc_num, self.loop_num,)
                                  )
            else:
                self.process_one_block(f_start_id=self.loop_num * self.block_size,
                                       f_end_id=self.doc_num,
                                       f_block_id=self.loop_num)
        if self.multi_pro:
            _pool.close()
            _pool.join()

    # @profile
    def merge_blocks(self):
        self.spimi.try_merge_blocks(f_block_number=self.loop_num + self.whether_remains)

    def process_multiple_block_and_write_outputs(self, f_core_num):
        self.process_multiple_block(f_core_num)
        self.merge_blocks()


if __name__ == '__main__':
    import cProfile
    import pstats

    profiler = cProfile.Profile()
    _rpt_timing = False
    _blk_size = 600
    _whether_multi_processor = False
    _core_num = mp.cpu_count() if _whether_multi_processor else 1
    sort_dir = "./docs/HillaryEmails"
    indexing = indexing_component(f_sort_dir=sort_dir, f_out_dir="./docs/output/",
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
