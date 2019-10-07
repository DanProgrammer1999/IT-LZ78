import os
import lz
import time
from multiprocessing import Process, Pool
import threading

global_lock = threading.Lock()


class Parameters:
    root_path = os.getcwd()

    source_dest = 'dataset'
    output_dest = 'DaniilDvoryanovOutputs'

    log_file = 'log.txt'
    log_fd = None

    verbose = False
    validate = False


class ProcessData:
    ratios = []
    failed_count = 0

    @staticmethod
    def set(*args):
        ratio, failed = args[0]
        ProcessData.ratios.append(ratio)
        ProcessData.failed_count += failed

    @staticmethod
    def reset():
        ProcessData.ratios = []
        ProcessData.failed_count = 0


def timed_function(f, *args, **kwargs):
    start = time.time()
    res = f(*args, **kwargs)
    end = time.time()

    return res, (end - start)


def log(*data, sep=' ', end='\n', stdout=False, close=False):
    global_lock.acquire()
    if not Parameters.log_fd:
        Parameters.log_fd = open(Parameters.log_file, 'w')

    Parameters.log_fd.write(sep.join(data) + end)

    if close:
        Parameters.log_fd.flush()
        Parameters.log_fd.close()
        Parameters.log_fd = None

    global_lock.release()

    if stdout:
        print(*data, sep=sep, end=end)


def get_compression_ratio(original, compressed):
    return os.path.getsize(original) / os.path.getsize(compressed)


def test_file(path, filename):
    dirname = path.split(os.sep)[-1]
    name, extensions = filename.split('.', 1)

    source_path = os.path.join(path, filename)
    compressed_path = os.path.join(Parameters.root_path, Parameters.output_dest, dirname,
                                   name + "Compressed." + extensions)
    decompressed_path = os.path.join(Parameters.root_path, Parameters.output_dest, dirname,
                                     name + "Decompressed." + extensions)

    lz.compress(source_path, compressed_path)
    ratio = get_compression_ratio(source_path, compressed_path)
    log("Compressed {} with compression ratio ".format(filename, ratio), stdout=Parameters.verbose)

    lz.decompress(compressed_path, decompressed_path)
    log("Decompressed {}".format(filename), stdout=Parameters.verbose)

    is_valid = None

    if Parameters.validate:

        is_valid, time = timed_function(lz.compare_files, source_path, decompressed_path)
        if is_valid:
            log("Validation of file {} passed in {}s".format(os.path.join(dirname, filename), time),
                stdout=Parameters.verbose)
        else:
            log("Validation of file {} FAILED in {}s".format(os.path.join(dirname, filename), time), stdout=True)

    # ProcessData.ratios.append(ratio)
    # ProcessData.time += total_time
    # ProcessData.failed_count += 1 if is_valid else 0

    return ratio, 1 if is_valid else 0


def test_folder(dirname, files):
    log("Testing folder {}\n".format(dirname), stdout=Parameters.verbose)

    source_path = os.path.join(Parameters.root_path, Parameters.source_dest, dirname)
    output_path = os.path.join(Parameters.root_path, Parameters.output_dest, dirname)

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    pool = Pool(os.cpu_count())
    processes = []

    for file in files:
        # time, ratio, failed = test_file(source_path, file)
        p = pool.apply_async(func=test_file, args=(source_path, file), callback=ProcessData.set)
        processes.append(p)

        log("", stdout=Parameters.verbose)

    pool.close()
    pool.join()

    if len(ProcessData.ratios) > 0:
        avg_ratio = sum(ProcessData.ratios) / len(ProcessData.ratios)
    else:
        avg_ratio = 0
    failed_count = ProcessData.failed_count

    ProcessData.reset()

    return avg_ratio, failed_count


def test():
    total_time = 0
    ratios = []
    failed_count = 0

    for dirpath, dirnames, filenames in os.walk(os.path.join(os.getcwd(), Parameters.source_dest)):
        dirname = dirpath.split(os.sep)[-1]
        if filenames:
            res, time = timed_function(test_folder, dirname, filenames)
            ratio, failed = res
            total_time += time
            ratios.append(ratio)
            failed_count += failed

            report = "{} finished testing. Average ratio: {}, time elapsed: {}s"
            if Parameters.validate:
                report += ", failed {} files"

            report += "\n" + "-" * 30
            log(report.format(dirname, ratio, time, failed_count), stdout=True)

    if len(ratios) > 0:
        avg_ratio = sum(ratios) / len(ratios)
    else:
        avg_ratio = 0

    report = "-" * 30 + "\nFinished testing. Average overall ratio: {}, total time elapsed: {}s (~{}min)"
    if Parameters.validate:
        report += ", failed {} files"

    log(report.format(avg_ratio, total_time, total_time / 60, failed_count), stdout=True, close=True)


if __name__ == '__main__':
    test()
