from airflow.models import BaseOperator
import json

import os

class SetConfigOperator(BaseOperator):
    """
    Operator to set the configuration of the database.
    """
    template_fields = ('config_file', )

    def __init__(
        self,
        config_file: str,
        task_id: str,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.config_file = config_file

    def execute(self):
        try:
            # Check if the file exists
            if not os.path.exists(self.config_file):
                self.log.error(f"Error 0 - O arquivo {self.config_file} não existe.")
                raise FileNotFoundError(f"Config file {self.config_file} does not exist.")
            
            # Read json file
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                self.log.info("Successfully read file")
                return config

            except Exception as e:
                self.log.error(f"Error 1 - Não foi possível ler o arquivo {self.config_file}: {str(e)}")
                raise
            
        except Exception as e:
            self.log.error(f"Unexpected error: {str(e)}")
            raise