import logging

from google.cloud import bigquery

from google.api_core.exceptions import NotFound, BadRequest

from .abstractClient import AbstractClient
import pandas as pd


class BigQueryClient(AbstractClient):

    def __init__(
        self,
        log_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        timeout=120,
    ):

        self.client = bigquery.Client()

        """
        client = google.cloud.logging.Client()
        handler = CloudLoggingHandler(client)
        handler.setFormatter(logging.Formatter(log_format))
        handler.labels = {"model": "test model template"} #config.get("gcloud", "DES_MODELLO") + "_" + config.get("gcloud", "DES_TIPO_MODELLO") + "_feat_select"
        google.cloud.logging.handlers.setup_logging(handler)
        """

        self._dataset_ref = None
        self._table_ref = None

        self.logger = logging.getLogger(BigQueryClient.__name__)

        self.timeout = timeout

        self.logger.info(
            "Big Query Client created using default project: {}".format(
                self.client.project
            )
        )

    @property
    def dataset_ref(self):
        return self._dataset_ref

    @dataset_ref.setter
    def dataset_ref(self, arr):
        self._dataset_ref = self.client.dataset(
            arr[0], project=arr[1]
        )  # set client reference to a particular dataset_ref

    @property
    def table_ref(self):
        return self._table_ref

    @table_ref.setter
    def table_ref(self, table_id):
        """

        table_id is string of following format "project.dataset.table"

        """
        self._table_ref = self.client.get_table(
            table_id
        )  # set client reference to a particular table

    def run_query(self, query, query_parameters=[], return_df=False):
        """
        execute a sql query as python string
        """
        try:
            job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

            self.logger.info("Running query:\n\n")
            self.logger.info(query)

            query_job = self.client.query(
                query,
                retry=bigquery.DEFAULT_RETRY.with_deadline(self.timeout),
                job_config=job_config,
            )
            dataframe = query_job.result()

            cost_euros = (query_job.total_bytes_billed / 1024**4) * 6
            self.logger.info(
                "The query processed {} bytes".format(query_job.total_bytes_processed)
            )
            self.logger.info("The query costed {} €".format(cost_euros))
            if return_df:
                return dataframe.to_dataframe()
            else:
                return
        except Exception as e:
            self.logger.error("BIG QUERY CLIENT ERROR: {}".format(e))
            raise Exception("BIG QUERY CLIENT ERROR: {}".format(e))

    def upload_dataframe(self, df, table_name, project_id, if_exists="replace"):
        """
        upload a dataframe to Big Query

        """

        try:
            # Load data to BQ
            df.to_gbq(table_name, project_id=project_id, if_exists=if_exists)
            self.logger.info("TABLE UPLOADED {}".format(self.client.project))

        except BadRequest as e:
            self.logger.error("BIG QUERY CLIENT ERROR: {}".format(e))
            raise Exception("BIG QUERY CLIENT ERROR")
