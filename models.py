import os
import json
from datetime import datetime, timedelta

from bingads.authorization import (
    AuthorizationData,
    OAuthDesktopMobileAuthCodeGrant,
)
from bingads.service_client import ServiceClient
from bingads.v13.reporting.reporting_download_parameters import (
    ReportingDownloadParameters,
)
from bingads.v13.reporting.reporting_service_manager import ReportingServiceManager
from google.cloud import bigquery


ACCOUNT_ID = "180518526"
CUSTOMER_ID = "251631063"

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

BQ_CLIENT = bigquery.Client()
DATASET = "BingAds"


def get_auth():
    """Get Authorzation data

    Returns:
        bingads.authorization.AuthorizationData: Authorization Data
    """

    authorization_data = AuthorizationData(
        account_id=None,
        customer_id=None,
        developer_token=os.getenv("DEVELOPER_TOKEN"),
        authentication=None,
    )
    authentication = OAuthDesktopMobileAuthCodeGrant(
        client_id=os.getenv("CLIENT_ID"),
        env="production",
    )

    authentication.state = "bld@bingads_amp"
    authentication.client_secret = os.getenv("CLIENT_SECRET")
    authorization_data.authentication = authentication

    authorization_data.authentication.request_oauth_tokens_by_refresh_token(
        os.getenv("REFRESH_TOKEN")
    )
    return authorization_data


class CampaignPerformanceReport:
    """Main model for the report

    Returns:
        CampaignPerformanceReport: Report
    """

    table = "CampaignPerformanceReport"

    @property
    def config(self):
        """Get config from JSON

        Returns:
            list: Schema
        """

        with open(f'configs/{self.table}.json') as f:
            config = json.load(f)
        return config['schema']

    def __init__(self, start, end):
        """Initiate the instance

        Args:
            start (str): Date in %Y-%m-%d
            end (str): Date in %Y-%m-%d
        """

        self.start, self.end = self._get_date_range(start, end)
        self.authorization_data = get_auth()
        self.schema = self.config

    def _get_date_range(self, _start, _end):
        """Generate date range for the run

        Args:
            _start (str): Date in %Y-%m-%d
            _end (str): Date in %Y-%m-%d

        Returns:
            tuple: (start, end)
        """

        if _start and _end:
            start, end = [datetime.strptime(i, DATE_FORMAT) for i in [_start, _end]]
        else:
            end = NOW.date()
            start = (NOW - timedelta(days=30)).date()
        return start, end

    def _get(self):
        """Get the report

        Returns:
            list: List of results
        """

        report_request = self._get_report_request()
        report_container = self._get_report(report_request)
        rows = [i for i in report_container.report_records]
        return rows

    def _get_custom_date_range(self, reporting_service, date):
        dt = reporting_service.factory.create("Date")
        dt.Day = date.day
        dt.Month = date.month
        dt.Year = date.year
        return dt

    def _get_time(self, reporting_service):
        time = reporting_service.factory.create("ReportTime")
        time.CustomDateRangeStart = self._get_custom_date_range(
            reporting_service,
            self.start,
        )
        time.CustomDateRangeEnd = self._get_custom_date_range(
            reporting_service,
            self.end,
        )
        time.ReportTimeZone = "PacificTimeUSCanadaTijuana"
        return time

    def _get_scope(self, reporting_service):
        scope = reporting_service.factory.create("AccountThroughCampaignReportScope")
        scope.AccountIds = {
            "long": [
                ACCOUNT_ID,
            ],
        }
        scope.Campaigns = None
        return scope

    def _get_columns(self, reporting_service):
        report_columns = reporting_service.factory.create(
            "ArrayOfCampaignPerformanceReportColumn"
        )
        report_columns.CampaignPerformanceReportColumn.append(
            [
                "AccountName",
                "AccountId",
                "TimePeriod",
                "CampaignId",
                "CampaignName",
                "Impressions",
                "Clicks",
                "Conversions",
                "Spend",
            ]
        )
        return report_columns

    def _get_report_request(self):
        reporting_service = ServiceClient(
            service="ReportingService",
            version=13,
            authorization_data=self.authorization_data,
            environment="production",
        )

        report_request = reporting_service.factory.create(
            "CampaignPerformanceReportRequest"
        )
        report_request.Aggregation = "Daily"
        report_request.ExcludeColumnHeaders = False
        report_request.ExcludeReportFooter = False
        report_request.ExcludeReportHeader = False
        report_request.Format = "Csv"
        report_request.ReturnOnlyCompleteData = False
        report_request.Time = self._get_time(reporting_service)
        report_request.ReportName = "CampaignPerformanceReport"
        report_request.Scope = self._get_scope(reporting_service)
        report_request.Columns = self._get_columns(reporting_service)

        return report_request

    def _get_report(self, report_request):
        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,
            timeout_in_milliseconds=3600000,
        )
        reporting_service_manager = ReportingServiceManager(
            authorization_data=self.authorization_data,
            poll_interval_in_milliseconds=5000,
            environment="production",
        )
        report_container = reporting_service_manager.download_report(
            reporting_download_parameters
        )
        return report_container

    def _transform(self, _rows):
        """Transform data

        Args:
            _rows (list): List of results

        Returns:
            list: List of results
        """

        rows = [
            {
                "AccountName": row.value("AccountName"),
                "AccountId": row.int_value("AccountId"),
                "TimePeriod": row.value("TimePeriod"),
                "CampaignId": row.int_value("CampaignId"),
                "CampaignName": row.value("CampaignName"),
                "Impressions": row.int_value("Impressions"),
                "Clicks": row.int_value("Clicks"),
                "Conversions": row.int_value("Conversions"),
                "Spend": row.float_value("Spend"),
                "_batched_at": NOW.isoformat(timespec="seconds"),
            }
            for row in _rows
        ]
        return rows

    def _load(self, rows):
        """Load to BigQuery

        Args:
            rows (list): List of results

        Returns:
            google.cloud.bigquery.LoadJob: Load Job
        """

        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}.{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=self.schema,
            ),
        ).result()

    def _update(self):
        """Update the table to the latest values"""

        query = f"""
        CREATE OR REPLACE TABLE {DATASET}.{self.table} AS
        SELECT * EXCEPT (row_num)
        FROM (
            SELECT *,
            ROW_NUMBER() OVER
            (PARTITION BY AccountName, AccountId, TimePeriod, CampaignId, CampaignName ORDER BY _batched_at DESC)
            AS row_num
            FROM {DATASET}.{self.table}
            )
        WHERE row_num = 1"""
        BQ_CLIENT.query(query).result()

    def run(self):
        """Run function

        Returns:
            dict: Job results
        """

        rows = self._get()
        response = {
            "table": "CampaignPerformanceReport",
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self._transform(rows)
            loads = self._load(rows)
            self._update()
            response["output_rows"] = loads.output_rows
        return response
