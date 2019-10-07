import os
import lz
import time


class Parameters:
    root_path = os.getcwd()

    source_dest = 'dataset'
    compressed_dest = 'generated{}compressed'.format(os.sep)
    decompressed_dest = 'generated{}decompressed'.format(os.sep)

    log_file = 'log.txt'
    log_fd = None

    verbose = False
    validate = False


def timed_function(f, *args, **kwargs):
    start = time.time()
    res = f(*args, **kwargs)
    end = time.time()

    return res, (end - start)


def log(*data, sep=' ', end='\n', stdout=False, close=False):
    if not Parameters.log_fd:
        Parameters.log_fd = open(Parameters.log_file, 'w')

    Parameters.log_fd.write(sep.join(data) + end)
    if stdout:
        print(*data, sep=sep, end=end)

    if close:
        Parameters.log_fd.flush()
        Parameters.log_fd.close()
        Parameters.log_fd = None


def get_compression_ratio(original, compressed):
    return os.path.getsize(original) / os.path.getsize(compressed)


def test_file(path, filename):
    dirname = path.split(os.sep)[-1]
    name, extensions = filename.split('.', 1)

    source_path = os.path.join(path, filename)
    compressed_path = os.path.join(Parameters.root_path, Parameters.compressed_dest, dirname, name + ".comp")
    decompressed_path = os.path.join(Parameters.root_path, Parameters.decompressed_dest, dirname,
                                     name + ".decomp." + extensions)

    total_time = 0

    log("Compressing file {}".format(os.path.join(dirname, filename)), stdout=Parameters.verbose)
    _, time = timed_function(lz.compress, source_path, compressed_path)
    total_time += time
    ratio = get_compression_ratio(source_path, compressed_path)
    log("Compressed in {}s with compression ratio ".format(time, ratio), stdout=Parameters.verbose)

    log("Decompressing file {}".format(os.path.join(dirname, name) + ".comp"), stdout=Parameters.verbose)
    _, time = timed_function(lz.decompress, compressed_path, decompressed_path)
    total_time += time
    log("Decompressed in {}s".format(time), stdout=Parameters.verbose)

    is_valid = None

    if Parameters.validate:

        is_valid, time = timed_function(lz.compare_files, source_path, decompressed_path)
        total_time += time
        if is_valid:
            log("Validation of file {} passed in {}s".format(os.path.join(dirname, filename), time),
                stdout=Parameters.verbose)
        else:
            log("Validation of file {} FAILED in {}s".format(os.path.join(dirname, filename), time), stdout=True)

    return total_time, ratio, 1 if is_valid else 0


def test_folder(dirname, files):
    log("Testing folder {}\n".format(dirname), stdout=Parameters.verbose)

    compressed_path = os.path.join(Parameters.root_path, Parameters.compressed_dest, dirname)
    decompressed_path = os.path.join(Parameters.root_path, Parameters.decompressed_dest, dirname)

    if not os.path.exists(compressed_path):
        os.makedirs(compressed_path)
    if not os.path.exists(decompressed_path):
        os.makedirs(decompressed_path)

    ratios = []
    failed_count = 0
    total_time = 0

    for file in files:
        time, ratio, failed = test_file(os.path.join(Parameters.root_path, Parameters.source_dest, dirname), file)
        total_time += time
        ratios.append(ratio)
        if failed:
            failed_count += failed

        log("", stdout=Parameters.verbose)

    if len(ratios) > 0:
        avg_ratio = sum(ratios) / len(ratios)
    else:
        avg_ratio = 0

    report = "{} finished testing. Average ratio: {}, time elapsed: {}s"
    if Parameters.validate:
        report += ", failed {} files"

    report += "\n" + "-" * 30
    log(report.format(dirname, avg_ratio, total_time, failed_count), stdout=True)

    return total_time, avg_ratio, failed_count


def test():
    total_time = 0
    ratios = []
    failed_count = 0

    for dirpath, dirnames, filenames in os.walk(os.path.join(os.getcwd(), Parameters.source_dest)):
        dirname = dirpath.split(os.sep)[-1]
        if filenames:
            time, ratio, failed = test_folder(dirname, filenames)
            total_time += time
            ratios.append(ratio)
            failed_count += failed

    if len(ratios) > 0:
        avg_ratio = sum(ratios) / len(ratios)
    else:
        avg_ratio = 0

    report = "-" * 30 + "\nFinished testing. Average overall ratio: {}, total time elapsed: {} (~{}min)"
    if Parameters.validate:
        report += ", failed {} files"

    log(report.format(avg_ratio, total_time, failed_count, total_time/60), stdout=True, close=True)


if __name__ == '__main__':
    test()
