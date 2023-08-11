import os
from typing import List

import pandas as pd
from airflow.models import BaseOperator


class LocalCSVTransformerOperator(BaseOperator):
    """
    Applies a processing function to a local file and writes the transformed data
    to a new location.
    """

    template_fields = (
        "input_filepath",
        "output_filepath",
    )

    def __init__(
        self,
        input_filepath: str,
        output_filepath: str,
        processing_function,
        na_values: List[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.input_filepath = input_filepath
        self.output_filepath = output_filepath
        self.processing_function = processing_function
        self.na_values = na_values

    def pre_execute(self, context: dict):
        if not os.path.exists(self.output_filepath):
            os.makedirs(os.path.dirname(self.output_filepath), exist_ok=True)

    def execute(self, context: dict):
        read_csv_kwargs = {}

        if self.na_values:
            read_csv_kwargs["na_values"] = self.na_values

        df = pd.read_csv(self.input_filepath, **read_csv_kwargs)

        df_transformed = self.processing_function(df)

        df_transformed.to_csv(self.output_filepath, index=False)

        self.log.info(f"Transformation complete: {self.output_filepath}")
