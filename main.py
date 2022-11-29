import argparse
import json
import pathlib
import sys

from pathlib import Path

# 1. Считал аргументы
parser = argparse.ArgumentParser(description='Tool to merge sorted logs.')

parser.add_argument(
    'sources',
    metavar='<INPUT FILES>',
    nargs='+',
    type=pathlib.Path)
parser.add_argument(
    '-o', '--output',
    metavar='<OUTPUT FILE>',
    required=True,
    type=pathlib.Path,
    help='path to merged log')

args = parser.parse_args()


if not all(map(Path.exists, args.sources)):
    sys.exit('Неверно указано расположение файлов')

sources = [path.open('rt') for path in args.sources]


def read_record(file_object):
    try:
        return json.loads(file_object.readline())
    except:
        return None


records = {source: None for source in sources}
has_records = True

with args.output.open('wt') as dst:
    while has_records:
        records = {
            source: read_record(source) if value is None else value
            for source, value in records.items()
        }

        key, value = min(filter(lambda x: x[1], records.items()), key=lambda x: x[1].get('timestamp'))
        dst.write(f'{json.dumps(value)}\n')
        records[key] = None
        '''
        1. Найти минимальный таймстемп
        2. Записать его в дст
        3. Занулить
        '''

        has_records = any(records.values())

for file in sources:
    file.close()

# CURRENT_DIR = Path(__file__).resolve().parent
#
# src1_path = CURRENT_DIR / 'jsnl' / 'log_a.jsonl'
# src2_path = CURRENT_DIR / 'jsnl' / 'log_b.jsonl'
# dst_path = CURRENT_DIR / 'jsnl' / 'log_c.jsonl'
#
# src1_has_records, src2_has_records = True, True
# record_1, record_2 = None, None
#
# with (
#     src1_path.open('rt') as src1,
#     src2_path.open('rt') as src2,
#     dst_path.open('wt') as dst
# ):
#     while src1_has_records or src2_has_records:
#         if record_1 is None:
#             try:
#                 record_1 = json.loads(src1.readline())
#             except:
#                 record_1 = None
#
#         if record_2 is None:
#             try:
#                 record_2 = json.loads(src2.readline())
#             except:
#                 record_2 = None
#
#         record = None
#
#         if record_1 is None:
#             record = record_2
#             record_2 = None
#         elif record_2 is None:
#             record = record_1
#             record_1 = None
#         elif record_1['timestamp'] <= record_2['timestamp']:
#             record = record_1
#             record_1 = None
#         else:
#             record = record_2
#             record_2 = None
#
#         if record is not None:
#             dst.write(f'{json.dumps(record)}\n')
#
#         src1_has_records = record_1 is not None
#         src2_has_records = record_2 is not None
#
#
#
#
