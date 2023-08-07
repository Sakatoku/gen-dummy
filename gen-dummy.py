import random
import string
import pandas as pd
import pyarrow.parquet as pq

# ランダム文字列の配列を生成する関数
def gen_string_arr(rows):
    # 空の配列を生成する
    arr = []
    # 行数分繰り返す
    for i in range(rows):
        # ランダムな文字列を生成する
        s = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        # 生成した文字列を配列に追加する
        arr.append(s)
    return arr

# ランダム数値の配列を生成する関数
def gen_number_arr(rows):
    # 空の配列を生成する
    arr = []
    # 行数分繰り返す
    for i in range(rows):
        # ランダムな数値を生成する
        n = random.randint(0, 10000)
        # 生成した数値を配列に追加する
        arr.append(n)
    return arr

# 連番数値の配列を生成する関数
def gen_id_arr(rows):
    # 空の配列を生成する
    arr = []
    # 行数分繰り返す
    for i in range(rows):
        # 連番数値を配列に追加する
        arr.append(i)
    return arr

# ランダム日付の配列を生成する関数
def gen_date_arr(rows):
    # 空の配列を生成する
    arr = []
    # 各月の最大日数を定義する
    days = [31,28,31,30,31,30,31,31,30,31,30,31]
    # 行数分繰り返す
    for i in range(rows):
        # ランダムな日付を生成する
        y = random.randint(2000, 2020)
        m = random.randint(1, 12)
        d = random.randint(1, days[m-1])
        s = '{:04d}-{:02d}-{:02d}'.format(y, m, d)
        # 生成した日付を配列に追加する
        arr.append(s)
    return arr

# 各カラムを結合してCSVに出力する関数
def gen_csv(filename, columns, id, name, value1, value2, date):
    # ファイルを開く
    with open(filename, 'w') as f:
        # ヘッダーを出力する
        f.write(','.join(columns) + '\n')
        # 行数分繰り返す
        for i in range(len(id)):
            # 各カラムを結合して出力する
            f.write('{},{},{},{},{}\n'.format(id[i], name[i], value1[i], value2[i], date[i]))

# 各カラムを結合してTSVに出力する関数
def gen_tsv(filename, columns, id, name, value1, value2, date):
    # ファイルを開く
    with open(filename, 'w') as f:
        # ヘッダーを出力する
        f.write('\t'.join(columns) + '\n')
        # 行数分繰り返す
        for i in range(len(id)):
            # 各カラムを結合して出力する
            f.write('{}\t{}\t{}\t{}\t{}\n'.format(id[i], name[i], value1[i], value2[i], date[i]))

# 各カラムを結合してDataFrameに出力する関数
def gen_dataframe(columns, id, name, value1, value2, date):
    # DataFrameを生成する
    df = pd.DataFrame()
    # 各カラムをDataFrameに追加する
    df['id'] = id
    df['name'] = name
    df['value1'] = value1
    df['value2'] = value2
    df['date'] = date
    return df

# 各カラムを結合してParquetに出力する関数
def gen_parquet(filename, columns, id, name, value1, value2, date):
    # DataFrameを生成する
    df = gen_dataframe(columns, id, name, value1, value2, date)
    # Parquetに出力する
    df.to_parquet(filename)

# main関数
def main():
    # カラム名を定義する
    columns = ['id', 'name', 'value1', 'value2', 'date']
    # 行数を定義する
    rows = 1000000

    # 各カラムに対応する配列を生成する
    id = gen_id_arr(rows)
    name = gen_string_arr(rows)
    value1 = gen_number_arr(rows)
    value2 = gen_number_arr(rows)
    date = gen_date_arr(rows)

    # 各形式で出力する
    gen_csv('output/dummy.csv', columns, id, name, value1, value2, date)
    gen_tsv('output/dummy.tsv', columns, id, name, value1, value2, date)
    gen_parquet('output/dummy.parquet', columns, id, name, value1, value2, date)

# テスト用のmain関数
def test():
    # 出力したファイルを読み込んで先頭10行を表示する
    df = pd.read_csv('output/dummy.csv')
    print(df.head(10))
    df = pd.read_csv('output/dummy.tsv', sep='\t')
    print(df.head(10))
    df = pd.read_parquet('output/dummy.parquet')
    print(df.head(10))

# main関数を実行する
if __name__ == '__main__':
    main()
    test()
