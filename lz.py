import math


def build_dict(filename):
    buffer = b""
    prefix_dict = {}

    with open(filename, 'rb') as f:
        symbol = f.read(1)
        while symbol:
            buffer += symbol
            if buffer not in prefix_dict.keys():
                prefix_dict[buffer] = len(prefix_dict) + 1

                if len(buffer) == 1:
                    parent_number = 0
                else:
                    parent_number = prefix_dict[buffer[:-1]]

                # Generate next output tuple (i, x_n)
                yield parent_number, symbol
                buffer = b""

            symbol = f.read(1)

        if buffer:
            # Generate last output tuple (i, x_n)
            yield prefix_dict[buffer], b''


def compress(file_in, file_out):
    lz_dict = build_dict(file_in)
    with open(file_out, 'wb') as f:
        for index, datum in lz_dict:
            if datum:
                value_code = bin(ord(datum))[2:]
                value_code = '0' * (8 - len(value_code)) + value_code
            else:
                value_code = ''

            code = format_bytes(index) + value_code

            code = int(code, 2).to_bytes(len(code) // 8, 'big')
            f.write(code)


def decompress(file_in, file_out):
    buffer = ""
    decode_dict = {}

    with open(file_in, 'rb') as f_in, open(file_out, 'wb') as f_out:
        b = f_in.read(1)
        while b:
            b = bin(ord(b))[2:]
            if len(b) < 8:
                b = '0' * (8 - len(b)) + b

            buffer += b[1:]

            if b[0] == '1':
                index = int('0b' + buffer, 2)
                out = f_in.read(1)

                if index != 0:
                    out = decode_dict[index] + out

                decode_dict[len(decode_dict) + 1] = out
                f_out.write(out)

                buffer = ""

            b = f_in.read(1)


def format_bytes(number):
    # max is used in case the number is 0
    # Divide by 7 because the 8th bit is formatting one
    bytes_number = max(math.ceil(number.bit_length() / 7), 1)

    # Convert prefix to binary and remove leading '0b' symbols
    prefix = bin(number)[2:]

    # Add leading zeros to make the code to be multiple of 7
    if len(prefix) % 7 != 0:
        prefix = '0' * (7 - len(prefix) % 7) + prefix
    res = "0b"

    for i in range(bytes_number):
        # Last byte
        if len(prefix) == 7:
            first_bit = '1'
        else:
            first_bit = '0'

        res += first_bit + prefix[0:7]
        prefix = prefix[7:]

    return res


def compare_files(file1, file2):
    with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
        while True:
            s1, s2 = f1.read(1), f2.read(1)
            if s1 != s2:
                print(s1, ':', s2)
                return False
            if not s1:
                break
        return True
