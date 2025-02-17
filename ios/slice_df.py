import pandas as pd

FILE_NAMES = ['567763947.br']

if __name__ == '__main__':
    df = pd.read_csv(f'{FILE_NAME}.csv')

    file_count = 0
    print(len(df))
    for i in range(0, len(df), 500):
        file_count += 1
        if i + 500 > len(df):
            slice_df = df[i:]
        else: slice_df = df[i:i+500]
        slice_df.to_csv(f'{FILE_NAME}({file_count}).csv', index=False)