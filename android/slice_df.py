import pandas as pd

FILE_NAMES = ['br.com.lojasrenner.pt-br']

if __name__ == '__main__':
    for file_name in FILE_NAMES:
        df = pd.read_csv(f'{file_name}.csv')

        file_count = 0
        print(len(df))
        for i in range(0, len(df), 500):
            file_count += 1
            if i + 500 > len(df):
                slice_df = df[i:]
            else: slice_df = df[i:i+500]
            slice_df.to_csv(f'{file_name}({file_count}).csv', index=False)