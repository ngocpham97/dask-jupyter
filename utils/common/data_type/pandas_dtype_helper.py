import pandas as pd

def cast_dataframe_dtypes(df: pd.DataFrame, dtype_mapping: dict) -> pd.DataFrame:
    for col, dtype in dtype_mapping.items():
        if col in df.columns:
            try:
                if 'datetime' in dtype:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                print(f"[WARN] Cannot cast column '{col}' to '{dtype}': {e}")
    return df
