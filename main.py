import json
import base64

from models import CampaignPerformanceReport


def main(request):
    """API Gateway

    Args:
        request (flask.Request): HTTP request

    Returns:
        dict: Response
    """

    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    job = CampaignPerformanceReport(
        data.get("start"),
        data.get("end"),
    )
    results = job.run()
    response = {
        "pipelines": "BingAds",
        "results": results,
    }
    print(response)
    return response
