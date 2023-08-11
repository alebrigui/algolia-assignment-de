import pandas as pd


def transform_shopify_configurations(df: pd.DataFrame) -> pd.DataFrame:
    # Requested in the assignment
    df = df[df["application_id"].notna()].copy()
    df["has_specific_prefix"] = ~df["index_prefix"].str.startswith("shopify_")

    # Needed for proper loading into PostGres,
    # this transformation could be better done separately
    df["nbr_metafields"] = df["nbr_metafields"].astype("Int64").copy()
    return df
