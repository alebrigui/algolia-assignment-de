import pandas as pd
from algolia.data_transformers import (
    transform_shopify_configurations,
)


def test_transform_shopify_configurations():
    # GIVEN
    data = {
        "application_id": [1, None, 3, 4],
        "index_prefix": ["shopify_test", "test", "shopify_demo", "demo"],
        "nbr_metafields": [1, 2, None, 4],
    }
    df = pd.DataFrame(data)

    # WHEN
    df_result = transform_shopify_configurations(df)

    # THEN
    # 1. Ensure rows with NaN in 'application_id' are removed
    assert df_result.shape[0] == 3
    assert None not in df_result["application_id"].values

    # 2. Ensure the 'has_specific_prefix' column is correctly populated
    expected_values = [False, False, True]
    assert all(df_result["has_specific_prefix"].values == expected_values)

    # 3. Ensure integer field is properly cast
    expected_metafields = [1, 4]
    assert all(df_result["nbr_metafields"].dropna().values == expected_metafields)
    assert df_result["nbr_metafields"].dtype == "Int64"  # Ensure the data type is Int64
