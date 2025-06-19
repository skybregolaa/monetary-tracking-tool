from abc import abstractmethod


class AbstractClient:

    @abstractmethod
    def upload_dataframe(self, input_df, table_name, project_id, if_exists):
        pass
